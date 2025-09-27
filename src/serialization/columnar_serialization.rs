/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


//! 序列化优化
//! 
//! 固定列块header + 列页偏移表，ZSTD自适应压缩

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::io::{Read, Write, Cursor};
use std::time::{Duration, Instant};
use bytes::{Bytes, BytesMut, Buf, BufMut};
use arrow::record_batch::RecordBatch;
use arrow::array::*;
use arrow::datatypes::*;
use serde::{Serialize, Deserialize};

/// 列块序列化器
pub struct ColumnBlockSerializer {
    /// 配置
    config: SerializationConfig,
    /// 压缩器
    compressor: Arc<Compressor>,
    /// 统计信息
    stats: Arc<SerializationStats>,
}

/// 序列化配置
#[derive(Debug, Clone)]
pub struct SerializationConfig {
    /// 是否启用压缩
    pub enable_compression: bool,
    /// 压缩级别
    pub compression_level: u32,
    /// 压缩算法
    pub compression_algorithm: CompressionAlgorithm,
    /// 是否启用自适应压缩
    pub enable_adaptive_compression: bool,
    /// 压缩阈值（字节）
    pub compression_threshold: usize,
    /// 是否启用零拷贝
    pub enable_zero_copy: bool,
    /// 列块大小
    pub column_block_size: usize,
    /// 页大小
    pub page_size: usize,
}

/// 压缩算法
#[derive(Debug, Clone, PartialEq)]
pub enum CompressionAlgorithm {
    /// ZSTD
    Zstd,
    /// LZ4
    Lz4,
    /// Gzip
    Gzip,
    /// Snappy
    Snappy,
    /// 自适应
    Adaptive,
}

/// 列块头
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnBlockHeader {
    /// 魔术数字
    magic: u32,
    /// 版本
    version: u32,
    /// 列块ID
    block_id: u64,
    /// 列数量
    column_count: u32,
    /// 行数
    row_count: u32,
    /// 压缩标志
    compressed: bool,
    /// 压缩算法
    compression_algorithm: u8,
    /// 压缩后大小
    compressed_size: u32,
    /// 原始大小
    original_size: u32,
    /// 校验和
    checksum: u32,
    /// 时间戳
    timestamp: u64,
}

/// 列页偏移表
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnPageOffsetTable {
    /// 列页偏移
    page_offsets: Vec<ColumnPageOffset>,
    /// 总页数
    total_pages: u32,
    /// 页大小
    page_size: u32,
}

/// 列页偏移
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnPageOffset {
    /// 列索引
    column_index: u32,
    /// 页索引
    page_index: u32,
    /// 偏移量
    offset: u64,
    /// 大小
    size: u32,
    /// 压缩大小
    compressed_size: u32,
    /// 是否压缩
    compressed: bool,
}

/// 列块数据
#[derive(Debug, Clone)]
pub struct ColumnBlockData {
    /// 头
    header: ColumnBlockHeader,
    /// 偏移表
    offset_table: ColumnPageOffsetTable,
    /// 数据
    data: Bytes,
}

/// 压缩器
pub struct Compressor {
    /// 配置
    config: SerializationConfig,
    /// 统计信息
    stats: Arc<SerializationStats>,
}

/// 序列化统计信息
#[derive(Debug)]
pub struct SerializationStats {
    /// 总序列化次数
    total_serializations: AtomicU64,
    /// 总反序列化次数
    total_deserializations: AtomicU64,
    /// 总压缩次数
    total_compressions: AtomicU64,
    /// 总解压缩次数
    total_decompressions: AtomicU64,
    /// 总原始大小
    total_original_size: AtomicU64,
    /// 总压缩后大小
    total_compressed_size: AtomicU64,
    /// 平均压缩比
    avg_compression_ratio: Arc<Mutex<f64>>,
    /// 平均序列化时间
    avg_serialization_time: AtomicU64,
    /// 平均反序列化时间
    avg_deserialization_time: AtomicU64,
}

/// 零拷贝序列化器
pub struct ZeroCopySerializer {
    /// 缓冲区
    buffer: BytesMut,
    /// 最大缓冲区大小
    max_buffer_size: usize,
    /// 压缩器
    compressor: Arc<Compressor>,
}

/// 自适应压缩器
pub struct AdaptiveCompressor {
    /// 压缩算法性能统计
    algorithm_stats: HashMap<CompressionAlgorithm, AlgorithmStats>,
    /// 当前最佳算法
    current_best_algorithm: CompressionAlgorithm,
    /// 更新间隔
    update_interval: Duration,
    /// 最后更新时间
    last_update: Instant,
}

/// 算法统计信息
#[derive(Debug, Clone)]
pub struct AlgorithmStats {
    /// 使用次数
    usage_count: u64,
    /// 平均压缩比
    avg_compression_ratio: f64,
    /// 平均压缩时间
    avg_compression_time: Duration,
    /// 平均解压缩时间
    avg_decompression_time: Duration,
}

impl ColumnBlockSerializer {
    /// 创建新的列块序列化器
    pub fn new(config: SerializationConfig) -> Self {
        let compressor = Arc::new(Compressor::new(config.clone()));
        let stats = Arc::new(SerializationStats::new());

        Self {
            config,
            compressor,
            stats,
        }
    }

    /// 序列化RecordBatch
    pub fn serialize_batch(&self, batch: &RecordBatch, block_id: u64) -> Result<ColumnBlockData, String> {
        let start_time = Instant::now();
        
        // 创建列块头
        let header = ColumnBlockHeader {
            magic: 0x4C42534C, // "LBSL" (Little Big Data Serialization Library)
            version: 1,
            block_id,
            column_count: batch.num_columns() as u32,
            row_count: batch.num_rows() as u32,
            compressed: false, // 将在压缩后更新
            compression_algorithm: 0, // 将在压缩后更新
            compressed_size: 0, // 将在压缩后更新
            original_size: 0, // 将在压缩后更新
            checksum: 0, // 将在计算后更新
            timestamp: Instant::now().elapsed().as_millis() as u64,
        };

        // 序列化列数据
        let mut column_data = Vec::new();
        let mut page_offsets = Vec::new();
        let mut current_offset = 0u64;

        for (column_index, column) in batch.columns().iter().enumerate() {
            let column_bytes = self.serialize_column(column)?;
            let column_size = column_bytes.len() as u32;
            
            // 分页处理
            let pages = self.split_into_pages(&column_bytes, self.config.page_size);
            
            for (page_index, page_data) in pages.into_iter().enumerate() {
                let page_offset = ColumnPageOffset {
                    column_index: column_index as u32,
                    page_index: page_index as u32,
                    offset: current_offset,
                    size: page_data.len() as u32,
                    compressed_size: 0, // 将在压缩后更新
                    compressed: false, // 将在压缩后更新
                };
                
                page_offsets.push(page_offset);
                column_data.extend_from_slice(&page_data);
                current_offset += page_data.len() as u64;
            }
        }

        // 创建偏移表
        let offset_table = ColumnPageOffsetTable {
            page_offsets,
            total_pages: page_offsets.len() as u32,
            page_size: self.config.page_size as u32,
        };

        // 压缩数据
        let (compressed_data, compression_info) = if self.config.enable_compression {
            self.compressor.compress(&column_data, &self.config)?
        } else {
            (column_data, CompressionInfo::new(false, CompressionAlgorithm::Zstd, 0.0))
        };

        // 更新头信息
        let mut updated_header = header;
        updated_header.compressed = compression_info.compressed;
        updated_header.compression_algorithm = compression_info.algorithm as u8;
        updated_header.compressed_size = compressed_data.len() as u32;
        updated_header.original_size = column_data.len() as u32;
        updated_header.checksum = self.calculate_checksum(&compressed_data);

        // 更新偏移表
        let mut updated_offset_table = offset_table;
        if compression_info.compressed {
            // 重新计算压缩后的偏移
            self.update_compressed_offsets(&mut updated_offset_table, &compression_info);
        }

        // 记录统计信息
        let serialization_time = start_time.elapsed();
        self.stats.record_serialization(column_data.len(), compressed_data.len(), serialization_time);

        Ok(ColumnBlockData {
            header: updated_header,
            offset_table: updated_offset_table,
            data: Bytes::from(compressed_data),
        })
    }

    /// 反序列化RecordBatch
    pub fn deserialize_batch(&self, block_data: &ColumnBlockData) -> Result<RecordBatch, String> {
        let start_time = Instant::now();
        
        // 验证头信息
        self.validate_header(&block_data.header)?;

        // 解压缩数据
        let decompressed_data = if block_data.header.compressed {
            self.compressor.decompress(&block_data.data, &block_data.header)?
        } else {
            block_data.data.to_vec()
        };

        // 重建列数据
        let mut columns = Vec::new();
        let mut current_column_data = Vec::new();
        let mut current_column_index = 0u32;

        for page_offset in &block_data.offset_table.page_offsets {
            if page_offset.column_index != current_column_index {
                // 完成当前列
                if !current_column_data.is_empty() {
                    let column = self.deserialize_column(&current_column_data, current_column_index as usize)?;
                    columns.push(column);
                    current_column_data.clear();
                }
                current_column_index = page_offset.column_index;
            }

            // 添加页数据
            let page_data = &decompressed_data[page_offset.offset as usize..(page_offset.offset + page_offset.size as u64) as usize];
            current_column_data.extend_from_slice(page_data);
        }

        // 处理最后一列
        if !current_column_data.is_empty() {
            let column = self.deserialize_column(&current_column_data, current_column_index as usize)?;
            columns.push(column);
        }

        // 创建RecordBatch
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(columns.iter().map(|c| c.data_type().clone()).collect())),
            columns
        ).map_err(|e| format!("Failed to create RecordBatch: {}", e))?;

        // 记录统计信息
        let deserialization_time = start_time.elapsed();
        self.stats.record_deserialization(deserialization_time);

        Ok(batch)
    }

    /// 序列化列
    fn serialize_column(&self, column: &dyn Array) -> Result<Vec<u8>, String> {
        // 使用Arrow的IPC格式序列化列
        let mut buffer = Vec::new();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buffer, &column.data_type())
            .map_err(|e| format!("Failed to create StreamWriter: {}", e))?;
        
        writer.write(column)
            .map_err(|e| format!("Failed to write column: {}", e))?;
        
        writer.finish()
            .map_err(|e| format!("Failed to finish writing: {}", e))?;

        Ok(buffer)
    }

    /// 反序列化列
    fn deserialize_column(&self, data: &[u8], column_index: usize) -> Result<ArrayRef, String> {
        let mut reader = Cursor::new(data);
        let mut stream_reader = arrow::ipc::reader::StreamReader::try_new(&mut reader, None)
            .map_err(|e| format!("Failed to create StreamReader: {}", e))?;
        
        let batch = stream_reader.next()
            .ok_or("No batch found")?
            .map_err(|e| format!("Failed to read batch: {}", e))?;
        
        if batch.num_columns() <= column_index {
            return Err(format!("Column index {} out of range", column_index));
        }
        
        Ok(batch.column(column_index).clone())
    }

    /// 分页
    fn split_into_pages(&self, data: &[u8], page_size: usize) -> Vec<Vec<u8>> {
        let mut pages = Vec::new();
        let mut offset = 0;
        
        while offset < data.len() {
            let end = (offset + page_size).min(data.len());
            pages.push(data[offset..end].to_vec());
            offset = end;
        }
        
        pages
    }

    /// 更新压缩后的偏移
    fn update_compressed_offsets(&self, offset_table: &mut ColumnPageOffsetTable, compression_info: &CompressionInfo) {
        // 根据压缩算法重新计算偏移
        for page_offset in &mut offset_table.page_offsets {
            page_offset.compressed = true;
            
            // 根据压缩算法计算实际压缩后的大小
            match compression_info.algorithm {
                CompressionAlgorithm::Zstd => {
                    // ZSTD压缩比通常在2:1到4:1之间
                    page_offset.compressed_size = (page_offset.size as f64 * compression_info.ratio) as usize;
                },
                CompressionAlgorithm::Lz4 => {
                    // LZ4压缩比通常在1.5:1到3:1之间
                    page_offset.compressed_size = (page_offset.size as f64 * compression_info.ratio) as usize;
                },
                CompressionAlgorithm::Gzip => {
                    // Gzip压缩比通常在2:1到5:1之间
                    page_offset.compressed_size = (page_offset.size as f64 * compression_info.ratio) as usize;
                },
                CompressionAlgorithm::Snappy => {
                    // Snappy压缩比通常在1.5:1到2.5:1之间
                    page_offset.compressed_size = (page_offset.size as f64 * compression_info.ratio) as usize;
                },
            }
            
            // 确保压缩后大小不超过原始大小
            page_offset.compressed_size = page_offset.compressed_size.min(page_offset.size);
        }
    }

    /// 计算校验和
    fn calculate_checksum(&self, data: &[u8]) -> u32 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        data.hash(&mut hasher);
        hasher.finish() as u32
    }

    /// 验证头信息
    fn validate_header(&self, header: &ColumnBlockHeader) -> Result<(), String> {
        if header.magic != 0x4C42534C {
            return Err("Invalid magic number".to_string());
        }
        
        if header.version != 1 {
            return Err("Unsupported version".to_string());
        }
        
        Ok(())
    }
}

impl Compressor {
    /// 创建新的压缩器
    pub fn new(config: SerializationConfig) -> Self {
        Self {
            config,
            stats: Arc::new(SerializationStats::new()),
        }
    }

    /// 压缩数据
    pub fn compress(&self, data: &[u8], config: &SerializationConfig) -> Result<(Vec<u8>, CompressionInfo), String> {
        if !config.enable_compression || data.len() < config.compression_threshold {
            return Ok((data.to_vec(), CompressionInfo::new(false, CompressionAlgorithm::Zstd, 0.0)));
        }

        let start_time = Instant::now();
        let algorithm = if config.enable_adaptive_compression {
            self.select_best_algorithm(data)?
        } else {
            config.compression_algorithm.clone()
        };

        let compressed_data = match algorithm {
            CompressionAlgorithm::Zstd => {
                zstd::encode_all(data, config.compression_level as i32)
                    .map_err(|e| format!("ZSTD compression failed: {}", e))?
            },
            CompressionAlgorithm::Lz4 => {
                lz4_flex::compress(data)
                    .map_err(|e| format!("LZ4 compression failed: {}", e))?
            },
            CompressionAlgorithm::Gzip => {
                use flate2::write::GzEncoder;
                use flate2::Compression;
                
                let mut encoder = GzEncoder::new(Vec::new(), Compression::new(config.compression_level));
                encoder.write_all(data)
                    .map_err(|e| format!("Gzip compression failed: {}", e))?;
                encoder.finish()
                    .map_err(|e| format!("Gzip compression failed: {}", e))?
            },
            CompressionAlgorithm::Adaptive => {
                // 自适应压缩，选择最佳算法
                self.adaptive_compress(data)?
            },
        };

        let compression_time = start_time.elapsed();
        let compression_ratio = compressed_data.len() as f64 / data.len() as f64;
        
        self.stats.record_compression(data.len(), compressed_data.len(), compression_time);

        Ok((compressed_data, CompressionInfo::new(true, algorithm, compression_ratio)))
    }

    /// 解压缩数据
    pub fn decompress(&self, data: &[u8], header: &ColumnBlockHeader) -> Result<Vec<u8>, String> {
        if !header.compressed {
            return Ok(data.to_vec());
        }

        let start_time = Instant::now();
        let algorithm = match header.compression_algorithm {
            0 => CompressionAlgorithm::Zstd,
            1 => CompressionAlgorithm::Lz4,
            2 => CompressionAlgorithm::Gzip,
            _ => return Err("Unknown compression algorithm".to_string()),
        };

        let decompressed_data = match algorithm {
            CompressionAlgorithm::Zstd => {
                zstd::decode_all(data)
                    .map_err(|e| format!("ZSTD decompression failed: {}", e))?
            },
            CompressionAlgorithm::Lz4 => {
                lz4_flex::decompress(data, data.len() * 4) // 假设压缩比不超过4:1
                    .map_err(|e| format!("LZ4 decompression failed: {}", e))?
            },
            CompressionAlgorithm::Gzip => {
                use flate2::read::GzDecoder;
                use std::io::Read;
                
                let mut decoder = GzDecoder::new(data);
                let mut result = Vec::new();
                decoder.read_to_end(&mut result)
                    .map_err(|e| format!("Gzip decompression failed: {}", e))?;
                result
            },
            CompressionAlgorithm::Adaptive => {
                return Err("Adaptive decompression not supported".to_string());
            },
        };

        let decompression_time = start_time.elapsed();
        self.stats.record_decompression(decompression_time);

        Ok(decompressed_data)
    }

    /// 选择最佳算法
    fn select_best_algorithm(&self, data: &[u8]) -> Result<CompressionAlgorithm, String> {
        // 根据数据特征选择最佳压缩算法
        let data_size = data.len();
        let entropy = self.calculate_entropy(data);
        let repetition_ratio = self.calculate_repetition_ratio(data);
        
        // 基于数据特征选择算法
        if data_size < 1024 {
            // 小数据使用LZ4，速度快
            Ok(CompressionAlgorithm::Lz4)
        } else if entropy < 0.5 {
            // 低熵数据（重复性高）使用ZSTD
            Ok(CompressionAlgorithm::Zstd)
        } else if repetition_ratio > 0.3 {
            // 高重复率数据使用ZSTD
            Ok(CompressionAlgorithm::Zstd)
        } else if data_size < 1024 * 1024 {
            // 中等大小数据使用ZSTD
            Ok(CompressionAlgorithm::Zstd)
        } else if entropy > 0.8 {
            // 高熵数据（随机性强）使用Snappy
            Ok(CompressionAlgorithm::Snappy)
        } else {
            // 大数据使用Gzip
            Ok(CompressionAlgorithm::Gzip)
        }
    }
    
    /// 计算数据熵
    fn calculate_entropy(&self, data: &[u8]) -> f64 {
        if data.is_empty() {
            return 0.0;
        }
        
        let mut counts = [0u32; 256];
        for &byte in data {
            counts[byte as usize] += 1;
        }
        
        let mut entropy = 0.0;
        let data_len = data.len() as f64;
        
        for &count in &counts {
            if count > 0 {
                let probability = count as f64 / data_len;
                entropy -= probability * probability.log2();
            }
        }
        
        entropy
    }
    
    /// 计算重复率
    fn calculate_repetition_ratio(&self, data: &[u8]) -> f64 {
        if data.len() < 2 {
            return 0.0;
        }
        
        let mut unique_bytes = std::collections::HashSet::new();
        for &byte in data {
            unique_bytes.insert(byte);
        }
        
        let unique_count = unique_bytes.len() as f64;
        let total_count = data.len() as f64;
        
        1.0 - (unique_count / total_count)
    }

    /// 自适应压缩
    fn adaptive_compress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        // 尝试多种算法，选择最佳结果
        let mut best_result = None;
        let mut best_ratio = 1.0;

        for algorithm in [CompressionAlgorithm::Zstd, CompressionAlgorithm::Lz4, CompressionAlgorithm::Gzip] {
            let compressed = match algorithm {
                CompressionAlgorithm::Zstd => {
                    zstd::encode_all(data, 3).map_err(|e| format!("ZSTD compression failed: {}", e))?
                },
                CompressionAlgorithm::Lz4 => {
                    lz4_flex::compress(data).map_err(|e| format!("LZ4 compression failed: {}", e))?
                },
                CompressionAlgorithm::Gzip => {
                    use flate2::write::GzEncoder;
                    use flate2::Compression;
                    
                    let mut encoder = GzEncoder::new(Vec::new(), Compression::new(3));
                    encoder.write_all(data).map_err(|e| format!("Gzip compression failed: {}", e))?;
                    encoder.finish().map_err(|e| format!("Gzip compression failed: {}", e))?
                },
                _ => continue,
            };

            let ratio = compressed.len() as f64 / data.len() as f64;
            if ratio < best_ratio {
                best_ratio = ratio;
                best_result = Some(compressed);
            }
        }

        best_result.ok_or("All compression algorithms failed".to_string())
    }
}

/// 压缩信息
#[derive(Debug, Clone)]
pub struct CompressionInfo {
    /// 是否压缩
    pub compressed: bool,
    /// 压缩算法
    pub algorithm: CompressionAlgorithm,
    /// 压缩比
    pub ratio: f64,
}

impl CompressionInfo {
    /// 创建新的压缩信息
    pub fn new(compressed: bool, algorithm: CompressionAlgorithm, ratio: f64) -> Self {
        Self { compressed, algorithm, ratio }
    }
}

impl SerializationStats {
    /// 创建新的序列化统计信息
    pub fn new() -> Self {
        Self {
            total_serializations: AtomicU64::new(0),
            total_deserializations: AtomicU64::new(0),
            total_compressions: AtomicU64::new(0),
            total_decompressions: AtomicU64::new(0),
            total_original_size: AtomicU64::new(0),
            total_compressed_size: AtomicU64::new(0),
            avg_compression_ratio: Arc::new(Mutex::new(0.0)),
            avg_serialization_time: AtomicU64::new(0),
            avg_deserialization_time: AtomicU64::new(0),
        }
    }

    /// 记录序列化
    pub fn record_serialization(&self, original_size: usize, compressed_size: usize, time: Duration) {
        self.total_serializations.fetch_add(1, Ordering::Relaxed);
        self.total_original_size.fetch_add(original_size as u64, Ordering::Relaxed);
        self.total_compressed_size.fetch_add(compressed_size as u64, Ordering::Relaxed);
        
        // 更新平均压缩比
        let total_original = self.total_original_size.load(Ordering::Relaxed);
        let total_compressed = self.total_compressed_size.load(Ordering::Relaxed);
        let ratio = if total_original > 0 {
            total_compressed as f64 / total_original as f64
        } else {
            1.0
        };
        let mut avg_ratio = self.avg_compression_ratio.lock().unwrap();
        *avg_ratio = ratio;
        
        // 更新平均序列化时间
        let total_serializations = self.total_serializations.load(Ordering::Relaxed);
        let current_avg = self.avg_serialization_time.load(Ordering::Relaxed);
        let new_avg = (current_avg * (total_serializations - 1) + time.as_micros() as u64) / total_serializations;
        self.avg_serialization_time.store(new_avg, Ordering::Relaxed);
    }

    /// 记录反序列化
    pub fn record_deserialization(&self, time: Duration) {
        self.total_deserializations.fetch_add(1, Ordering::Relaxed);
        
        // 更新平均反序列化时间
        let total_deserializations = self.total_deserializations.load(Ordering::Relaxed);
        let current_avg = self.avg_deserialization_time.load(Ordering::Relaxed);
        let new_avg = (current_avg * (total_deserializations - 1) + time.as_micros() as u64) / total_deserializations;
        self.avg_deserialization_time.store(new_avg, Ordering::Relaxed);
    }

    /// 记录压缩
    pub fn record_compression(&self, original_size: usize, compressed_size: usize, time: Duration) {
        self.total_compressions.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录解压缩
    pub fn record_decompression(&self, time: Duration) {
        self.total_decompressions.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for SerializationConfig {
    fn default() -> Self {
        Self {
            enable_compression: true,
            compression_level: 3,
            compression_algorithm: CompressionAlgorithm::Zstd,
            enable_adaptive_compression: true,
            compression_threshold: 1024,
            enable_zero_copy: true,
            column_block_size: 1024 * 1024, // 1MB
            page_size: 64 * 1024, // 64KB
        }
    }
}

impl Default for SerializationStats {
    fn default() -> Self {
        Self::new()
    }
}
