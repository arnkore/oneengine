//! 数据溢写管理器
//! 
//! 处理内存不足时的数据溢写到磁盘

use crate::columnar::batch::Batch;
use anyhow::Result;
use std::path::PathBuf;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, BufReader, Write, Read, Seek, SeekFrom};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use tracing::{debug, warn, error};
use serde::{Serialize, Deserialize};

/// 溢写配置
#[derive(Debug, Clone)]
pub struct SpillConfig {
    /// 溢写目录
    pub spill_dir: PathBuf,
    /// 最大内存使用量（触发溢写的阈值）
    pub max_memory_bytes: usize,
    /// 压缩算法
    pub compression: CompressionType,
    /// 批次大小
    pub batch_size: usize,
    /// 是否启用压缩
    pub enable_compression: bool,
}

/// 压缩类型
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CompressionType {
    /// 无压缩
    None,
    /// LZ4压缩
    LZ4,
    /// ZSTD压缩
    ZSTD,
    /// GZIP压缩
    GZIP,
}

/// 溢写文件信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpillFileInfo {
    /// 文件路径
    pub path: PathBuf,
    /// 文件大小
    pub size: u64,
    /// 批次数量
    pub batch_count: usize,
    /// 压缩类型
    pub compression: CompressionType,
    /// 创建时间
    pub created_at: u64,
}

/// 溢写管理器
pub struct SpillManager {
    /// 配置
    config: SpillConfig,
    /// 溢写文件信息
    spill_files: Arc<Mutex<HashMap<String, SpillFileInfo>>>,
    /// 当前内存使用量
    current_memory_usage: Arc<Mutex<usize>>,
    /// 溢写计数器
    spill_counter: Arc<Mutex<u64>>,
}

impl SpillManager {
    /// 创建新的溢写管理器
    pub fn new(config: SpillConfig) -> Result<Self> {
        // 确保溢写目录存在
        std::fs::create_dir_all(&config.spill_dir)?;

        Ok(Self {
            spill_files: Arc::new(Mutex::new(HashMap::new())),
            current_memory_usage: Arc::new(Mutex::new(0)),
            spill_counter: Arc::new(Mutex::new(0)),
            config,
        })
    }

    /// 检查是否需要溢写
    pub fn should_spill(&self, additional_memory: usize) -> bool {
        let current_usage = *self.current_memory_usage.lock().unwrap();
        current_usage + additional_memory > self.config.max_memory_bytes
    }

    /// 溢写批次数据
    pub fn spill_batch(&self, batch: &Batch, spill_id: &str) -> Result<SpillFileInfo> {
        let spill_counter = {
            let mut counter = self.spill_counter.lock().unwrap();
            *counter += 1;
            *counter
        };

        let filename = format!("spill_{}_{}.dat", spill_id, spill_counter);
        let file_path = self.config.spill_dir.join(&filename);

        // 创建溢写文件
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&file_path)?;

        let mut writer = BufWriter::new(file);

        // 序列化批次数据
        let serialized = self.serialize_batch(batch)?;
        
        // 压缩数据（如果启用）
        let compressed = if self.config.enable_compression {
            self.compress_data(&serialized)?
        } else {
            serialized
        };

        // 写入文件
        writer.write_all(&compressed)?;
        writer.flush()?;

        let file_size = file_path.metadata()?.len();

        // 创建文件信息
        let file_info = SpillFileInfo {
            path: file_path.clone(),
            size: file_size,
            batch_count: 1,
            compression: self.config.compression.clone(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        // 记录文件信息
        {
            let mut spill_files = self.spill_files.lock().unwrap();
            spill_files.insert(filename, file_info.clone());
        }

        // 更新内存使用量（简化实现）
        self.update_memory_usage(-((batch.len() * 8) as isize));

        debug!(
            "Spilled batch to file: {} ({} bytes)",
            file_path.display(),
            file_size
        );

        Ok(file_info)
    }

    /// 溢写多个批次
    pub fn spill_batches(&self, batches: &[Batch], spill_id: &str) -> Result<SpillFileInfo> {
        let spill_counter = {
            let mut counter = self.spill_counter.lock().unwrap();
            *counter += 1;
            *counter
        };

        let filename = format!("spill_{}_{}.dat", spill_id, spill_counter);
        let file_path = self.config.spill_dir.join(&filename);

        // 创建溢写文件
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&file_path)?;

        let mut writer = BufWriter::new(file);

        let mut total_size = 0;
        let mut total_memory = 0;

        // 序列化所有批次
        for batch in batches {
            let serialized = self.serialize_batch(batch)?;
            let compressed = if self.config.enable_compression {
                self.compress_data(&serialized)?
            } else {
                serialized
            };

            // 写入批次大小（用于读取时分割）
            writer.write_all(&(compressed.len() as u32).to_le_bytes())?;
            writer.write_all(&compressed)?;
            
            total_size += compressed.len() + 4; // +4 for size header
            total_memory += batch.len() * 8; // 简化实现
        }

        writer.flush()?;

        let file_size = file_path.metadata()?.len();

        // 创建文件信息
        let file_info = SpillFileInfo {
            path: file_path.clone(),
            size: file_size,
            batch_count: batches.len(),
            compression: self.config.compression.clone(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        // 记录文件信息
        {
            let mut spill_files = self.spill_files.lock().unwrap();
            spill_files.insert(filename, file_info.clone());
        }

        // 更新内存使用量
        self.update_memory_usage(-(total_memory as isize));

        debug!(
            "Spilled {} batches to file: {} ({} bytes)",
            batches.len(),
            file_path.display(),
            file_size
        );

        Ok(file_info)
    }

    /// 读取溢写文件
    pub fn read_spill_file(&self, file_info: &SpillFileInfo) -> Result<Vec<Batch>> {
        let file = File::open(&file_info.path)?;
        let mut reader = BufReader::new(file);

        let mut batches = Vec::new();

        if file_info.batch_count == 1 {
            // 单个批次
            let mut data = Vec::new();
            reader.read_to_end(&mut data)?;

            let decompressed = if self.config.enable_compression {
                self.decompress_data(&data)?
            } else {
                data
            };

            let batch = self.deserialize_batch(&decompressed)?;
            batches.push(batch);
        } else {
            // 多个批次
            while let Ok(size_bytes) = self.read_u32(&mut reader) {
                let mut batch_data = vec![0u8; size_bytes as usize];
                reader.read_exact(&mut batch_data)?;

                let decompressed = if self.config.enable_compression {
                    self.decompress_data(&batch_data)?
                } else {
                    batch_data
                };

                let batch = self.deserialize_batch(&decompressed)?;
                batches.push(batch);
            }
        }

        Ok(batches)
    }

    /// 删除溢写文件
    pub fn delete_spill_file(&self, file_info: &SpillFileInfo) -> Result<()> {
        std::fs::remove_file(&file_info.path)?;
        
        // 从记录中移除
        {
            let mut spill_files = self.spill_files.lock().unwrap();
            if let Some(filename) = file_info.path.file_name() {
                spill_files.remove(filename.to_str().unwrap());
            }
        }

        debug!("Deleted spill file: {}", file_info.path.display());
        Ok(())
    }

    /// 清理所有溢写文件
    pub fn cleanup_all(&self) -> Result<()> {
        let spill_files = self.spill_files.lock().unwrap();
        for file_info in spill_files.values() {
            if let Err(e) = std::fs::remove_file(&file_info.path) {
                warn!("Failed to delete spill file {}: {}", file_info.path.display(), e);
            }
        }

        debug!("Cleaned up all spill files");
        Ok(())
    }

    /// 获取溢写统计信息
    pub fn get_spill_stats(&self) -> SpillStats {
        let spill_files = self.spill_files.lock().unwrap();
        let total_files = spill_files.len();
        let total_size: u64 = spill_files.values().map(|f| f.size).sum();
        let total_batches: usize = spill_files.values().map(|f| f.batch_count).sum();

        SpillStats {
            total_files,
            total_size,
            total_batches,
            current_memory_usage: *self.current_memory_usage.lock().unwrap(),
        }
    }

    /// 序列化批次
    fn serialize_batch(&self, batch: &Batch) -> Result<Vec<u8>> {
        // 这里使用简单的序列化，实际应该使用更高效的格式
        let mut data = Vec::new();
        
        // 写入schema信息
        data.extend_from_slice(&(batch.schema.fields.len() as u32).to_le_bytes());
        for field in &batch.schema.fields {
            data.extend_from_slice(field.name.as_bytes());
            data.push(0); // null terminator
            data.push(field.data_type.to_u8()); // 简化实现
            data.push(field.nullable as u8);
        }

        // 写入批次大小
        data.extend_from_slice(&(batch.len() as u32).to_le_bytes());

        // 写入列数据（简化版本）
        for column in &batch.columns {
            data.extend_from_slice(&(column.data_type.to_u8()).to_le_bytes()); // 简化实现
            // 这里应该序列化实际的数据，为了简化，我们只写入占位符
            data.extend_from_slice(&(0u32).to_le_bytes()); // 数据长度占位符
        }

        Ok(data)
    }

    /// 反序列化批次
    fn deserialize_batch(&self, data: &[u8]) -> Result<Batch> {
        // 这里应该实现完整的反序列化逻辑
        // 为了简化，我们返回一个空的批次
        let schema = crate::columnar::batch::BatchSchema::new();
        let batch = Batch::from_columns(Vec::new(), schema)?;
        Ok(batch)
    }

    /// 压缩数据
    fn compress_data(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.config.compression {
            CompressionType::None => Ok(data.to_vec()),
            CompressionType::LZ4 => {
                // 使用lz4压缩
                let mut compressed = Vec::new();
                lz4_flex::compress_into(data, &mut compressed)?;
                Ok(compressed)
            }
            CompressionType::ZSTD => {
                // 使用zstd压缩
                let compressed = zstd::encode_all(data, 0)?;
                Ok(compressed)
            }
            CompressionType::GZIP => {
                // 使用gzip压缩
                let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                encoder.write_all(data)?;
                Ok(encoder.finish()?)
            }
        }
    }

    /// 解压数据
    fn decompress_data(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self.config.compression {
            CompressionType::None => Ok(data.to_vec()),
            CompressionType::LZ4 => {
                // 使用lz4解压
                let decompressed = lz4_flex::decompress_size_prepended(data)?;
                Ok(decompressed)
            }
            CompressionType::ZSTD => {
                // 使用zstd解压
                let decompressed = zstd::decode_all(data)?;
                Ok(decompressed)
            }
            CompressionType::GZIP => {
                // 使用gzip解压
                let mut decoder = flate2::read::GzDecoder::new(data);
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed)?;
                Ok(decompressed)
            }
        }
    }

    /// 读取u32
    fn read_u32(&self, reader: &mut BufReader<File>) -> Result<u32> {
        let mut bytes = [0u8; 4];
        reader.read_exact(&mut bytes)?;
        Ok(u32::from_le_bytes(bytes))
    }

    /// 更新内存使用量
    fn update_memory_usage(&self, delta: isize) {
        let mut usage = self.current_memory_usage.lock().unwrap();
        if delta > 0 {
            *usage += delta as usize;
        } else {
            *usage = usage.saturating_sub((-delta) as usize);
        }
    }
}

/// 溢写统计信息
#[derive(Debug, Clone)]
pub struct SpillStats {
    /// 总文件数
    pub total_files: usize,
    /// 总大小（字节）
    pub total_size: u64,
    /// 总批次数
    pub total_batches: usize,
    /// 当前内存使用量
    pub current_memory_usage: usize,
}

impl Default for SpillConfig {
    fn default() -> Self {
        Self {
            spill_dir: std::env::temp_dir().join("oneengine_spill"),
            max_memory_bytes: 100 * 1024 * 1024, // 100MB
            compression: CompressionType::LZ4,
            batch_size: 1000,
            enable_compression: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::batch::{Batch, BatchSchema, Field};
    use crate::columnar::column::Column;
    use crate::columnar::types::DataType;

    #[test]
    fn test_spill_manager_creation() {
        let config = SpillConfig::default();
        let manager = SpillManager::new(config).unwrap();
        assert_eq!(manager.get_spill_stats().total_files, 0);
    }

    #[test]
    fn test_should_spill() {
        let config = SpillConfig {
            max_memory_bytes: 1000,
            ..Default::default()
        };
        let manager = SpillManager::new(config).unwrap();
        
        // 更新内存使用量
        {
            let mut usage = manager.current_memory_usage.lock().unwrap();
            *usage = 800;
        }
        
        assert!(manager.should_spill(300)); // 800 + 300 > 1000
        assert!(!manager.should_spill(100)); // 800 + 100 <= 1000
    }
}
