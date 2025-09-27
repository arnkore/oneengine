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


//! 网络服务器
//! 
//! 基于 tokio 的高性能网络服务器，支持零拷贝序列化、压缩和长度前缀编码

use std::sync::Arc;
use std::net::{SocketAddr, IpAddr, Ipv4Addr};
use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use bytes::{Bytes, BytesMut, Buf, BufMut};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncRead, AsyncWrite, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{timeout, sleep};

/// 网络配置
#[derive(Debug, Clone)]
pub struct NetworkConfig {
    /// 监听地址
    pub listen_addr: SocketAddr,
    /// 最大连接数
    pub max_connections: usize,
    /// 缓冲区大小
    pub buffer_size: usize,
    /// 超时时间
    pub timeout: Duration,
    /// 是否启用零拷贝
    pub enable_zero_copy: bool,
    /// 是否启用压缩
    pub enable_compression: bool,
    /// 压缩级别
    pub compression_level: u32,
}

/// 网络服务器
pub struct NetworkServer {
    /// 配置
    config: NetworkConfig,
    /// 连接管理器
    connection_manager: Arc<ConnectionManager>,
    /// 消息处理器
    message_handler: Arc<MessageHandler>,
    /// 统计信息
    stats: Arc<NetworkStats>,
}

/// 连接管理器
pub struct ConnectionManager {
    /// 活跃连接
    active_connections: Arc<tokio::sync::RwLock<HashMap<ConnectionId, Arc<Connection>>>>,
    /// 连接ID生成器
    connection_id_generator: AtomicU64,
    /// 最大连接数
    max_connections: usize,
}

/// 连接
pub struct Connection {
    /// 连接ID
    id: ConnectionId,
    /// TCP流
    stream: Arc<tokio::sync::Mutex<TcpStream>>,
    /// 发送通道
    sender: mpsc::UnboundedSender<Message>,
    /// 接收通道
    receiver: mpsc::UnboundedReceiver<Message>,
    /// 统计信息
    stats: Arc<ConnectionStats>,
    /// 是否已关闭
    closed: Arc<AtomicUsize>,
}

/// 连接ID
pub type ConnectionId = u64;

/// 消息
#[derive(Debug, Clone)]
pub struct Message {
    /// 消息类型
    message_type: MessageType,
    /// 消息数据
    data: Bytes,
    /// 消息ID
    message_id: u64,
    /// 时间戳
    timestamp: Instant,
}

/// 消息类型
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MessageType {
    /// 数据消息
    Data,
    /// 控制消息
    Control,
    /// 心跳消息
    Heartbeat,
    /// 错误消息
    Error,
}

/// 消息处理器
pub struct MessageHandler {
    /// 消息类型处理器
    handlers: HashMap<MessageType, Box<dyn MessageTypeHandler + Send + Sync>>,
    /// 统计信息
    stats: Arc<NetworkStats>,
}

/// 消息类型处理器trait
pub trait MessageTypeHandler {
    /// 处理消息
    fn handle_message(&self, message: Message, connection: Arc<Connection>) -> Result<(), String>;
}

/// 网络统计信息
#[derive(Debug)]
pub struct NetworkStats {
    /// 总连接数
    total_connections: AtomicU64,
    /// 活跃连接数
    active_connections: AtomicUsize,
    /// 总消息数
    total_messages: AtomicU64,
    /// 总字节数
    total_bytes: AtomicU64,
    /// 错误数
    total_errors: AtomicU64,
    /// 平均延迟
    avg_latency: AtomicU64,
}

/// 连接统计信息
#[derive(Debug)]
pub struct ConnectionStats {
    /// 连接ID
    connection_id: ConnectionId,
    /// 消息数
    message_count: AtomicU64,
    /// 字节数
    byte_count: AtomicU64,
    /// 错误数
    error_count: AtomicU64,
    /// 连接时间
    connected_at: Instant,
    /// 最后活动时间
    last_activity: AtomicU64,
}

/// 零拷贝序列化器
pub struct ZeroCopySerializer {
    /// 缓冲区
    buffer: BytesMut,
    /// 最大缓冲区大小
    max_buffer_size: usize,
    /// 压缩器
    compressor: Option<Compressor>,
}

/// 压缩器
pub struct Compressor {
    /// 压缩级别
    level: u32,
    /// 压缩算法
    algorithm: CompressionAlgorithm,
}

/// 压缩算法
#[derive(Debug, Clone)]
pub enum CompressionAlgorithm {
    /// ZSTD
    Zstd,
    /// LZ4
    Lz4,
    /// Gzip
    Gzip,
}

/// 长度前缀编码器
pub struct LengthPrefixedEncoder {
    /// 缓冲区
    buffer: BytesMut,
    /// 最大消息大小
    max_message_size: usize,
}

/// 长度前缀解码器
pub struct LengthPrefixedDecoder {
    /// 缓冲区
    buffer: BytesMut,
    /// 当前消息长度
    current_message_length: Option<usize>,
    /// 已读取的字节数
    bytes_read: usize,
}

impl NetworkServer {
    /// 创建新的网络服务器
    pub fn new(config: NetworkConfig) -> Self {
        let connection_manager = Arc::new(ConnectionManager::new(config.max_connections));
        let message_handler = Arc::new(MessageHandler::new());
        let stats = Arc::new(NetworkStats::new());

        Self {
            config,
            connection_manager,
            message_handler,
            stats,
        }
    }

    /// 启动服务器
    pub async fn start(&self) -> Result<(), String> {
        let listener = TcpListener::bind(self.config.listen_addr)
            .await
            .map_err(|e| format!("Failed to bind to {}: {}", self.config.listen_addr, e))?;

        println!("Server listening on {}", self.config.listen_addr);

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    println!("New connection from {}", addr);
                    
                    // 创建连接
                    let connection = self.connection_manager.create_connection(stream).await?;
                    
                    // 启动连接处理任务
                    let connection_manager = self.connection_manager.clone();
                    let message_handler = self.message_handler.clone();
                    let stats = self.stats.clone();
                    
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(connection, connection_manager, message_handler, stats).await {
                            eprintln!("Connection error: {}", e);
                        }
                    });
                },
                Err(e) => {
                    eprintln!("Failed to accept connection: {}", e);
                }
            }
        }
    }

    /// 处理连接
    async fn handle_connection(
        connection: Arc<Connection>,
        connection_manager: Arc<ConnectionManager>,
        message_handler: Arc<MessageHandler>,
        stats: Arc<NetworkStats>,
    ) -> Result<(), String> {
        let mut decoder = LengthPrefixedDecoder::new();
        let mut buffer = vec![0u8; 4096];
        
        loop {
            // 读取数据
            let mut stream = connection.stream.lock().await;
            let n = stream.read(&mut buffer).await
                .map_err(|e| format!("Failed to read from connection: {}", e))?;
            
            if n == 0 {
                // 连接关闭
                break;
            }

            // 更新统计信息
            stats.record_bytes(n as u64);
            connection.stats.record_bytes(n as u64);

            // 解码消息
            decoder.feed_data(&buffer[..n]);
            
            while let Some(message) = decoder.next_message()? {
                // 处理消息
                message_handler.handle_message(message, connection.clone())?;
                stats.record_message();
                connection.stats.record_message();
            }
        }

        // 清理连接
        connection_manager.remove_connection(connection.id).await;
        Ok(())
    }
}

impl ConnectionManager {
    /// 创建新的连接管理器
    pub fn new(max_connections: usize) -> Self {
        Self {
            active_connections: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            connection_id_generator: AtomicU64::new(1),
            max_connections,
        }
    }

    /// 创建连接
    pub async fn create_connection(&self, stream: TcpStream) -> Result<Arc<Connection>, String> {
        // 检查连接数限制
        let current_connections = self.active_connections.read().await.len();
        if current_connections >= self.max_connections {
            return Err("Maximum connections exceeded".to_string());
        }

        // 生成连接ID
        let connection_id = self.connection_id_generator.fetch_add(1, Ordering::Relaxed);
        
        // 创建发送和接收通道
        let (sender, receiver) = mpsc::unbounded_channel();
        
        // 创建连接
        let connection = Arc::new(Connection {
            id: connection_id,
            stream: Arc::new(tokio::sync::Mutex::new(stream)),
            sender,
            receiver,
            stats: Arc::new(ConnectionStats::new(connection_id)),
            closed: Arc::new(AtomicUsize::new(0)),
        });

        // 添加到活跃连接
        self.active_connections.write().await.insert(connection_id, connection.clone());
        
        Ok(connection)
    }

    /// 移除连接
    pub async fn remove_connection(&self, connection_id: ConnectionId) {
        self.active_connections.write().await.remove(&connection_id);
    }

    /// 获取连接
    pub async fn get_connection(&self, connection_id: ConnectionId) -> Option<Arc<Connection>> {
        self.active_connections.read().await.get(&connection_id).cloned()
    }

    /// 获取活跃连接数
    pub async fn get_active_connection_count(&self) -> usize {
        self.active_connections.read().await.len()
    }
}

impl Connection {
    /// 发送消息
    pub async fn send_message(&self, message: Message) -> Result<(), String> {
        if self.closed.load(Ordering::Relaxed) == 1 {
            return Err("Connection is closed".to_string());
        }

        // 序列化消息
        let mut encoder = LengthPrefixedEncoder::new();
        let serialized = encoder.encode_message(message)?;
        
        // 发送数据
        let mut stream = self.stream.lock().await;
        stream.write_all(&serialized).await
            .map_err(|e| format!("Failed to send message: {}", e))?;
        
        Ok(())
    }

    /// 关闭连接
    pub fn close(&self) {
        self.closed.store(1, Ordering::Relaxed);
    }

    /// 检查连接是否已关闭
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed) == 1
    }
}

impl MessageHandler {
    /// 创建新的消息处理器
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
            stats: Arc::new(NetworkStats::new()),
        }
    }

    /// 注册消息类型处理器
    pub fn register_handler(&mut self, message_type: MessageType, handler: Box<dyn MessageTypeHandler + Send + Sync>) {
        self.handlers.insert(message_type, handler);
    }

    /// 处理消息
    pub fn handle_message(&self, message: Message, connection: Arc<Connection>) -> Result<(), String> {
        if let Some(handler) = self.handlers.get(&message.message_type) {
            handler.handle_message(message, connection)
        } else {
            Err(format!("No handler for message type: {:?}", message.message_type))
        }
    }
}

impl ZeroCopySerializer {
    /// 创建新的零拷贝序列化器
    pub fn new(max_buffer_size: usize, enable_compression: bool, compression_level: u32) -> Self {
        let compressor = if enable_compression {
            Some(Compressor::new(compression_level, CompressionAlgorithm::Zstd))
        } else {
            None
        };

        Self {
            buffer: BytesMut::with_capacity(1024),
            max_buffer_size,
            compressor,
        }
    }

    /// 序列化数据
    pub fn serialize<T>(&mut self, data: &T) -> Result<Bytes, String>
    where
        T: serde::Serialize,
    {
        // 清空缓冲区
        self.buffer.clear();
        
        // 序列化数据
        let serialized = serde_json::to_vec(data)
            .map_err(|e| format!("Failed to serialize data: {}", e))?;
        
        // 压缩数据
        let compressed = if let Some(ref compressor) = self.compressor {
            compressor.compress(&serialized)?
        } else {
            serialized
        };
        
        // 写入长度前缀
        self.buffer.put_u32(compressed.len() as u32);
        
        // 写入数据
        self.buffer.put_slice(&compressed);
        
        Ok(self.buffer.clone().freeze())
    }

    /// 反序列化数据
    pub fn deserialize<T>(&mut self, data: &[u8]) -> Result<T, String>
    where
        T: serde::de::DeserializeOwned,
    {
        if data.len() < 4 {
            return Err("Invalid data: too short".to_string());
        }
        
        // 读取长度前缀
        let length = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
        
        if data.len() < 4 + length {
            return Err("Invalid data: incomplete message".to_string());
        }
        
        let compressed_data = &data[4..4 + length];
        
        // 解压缩数据
        let decompressed = if let Some(ref compressor) = self.compressor {
            compressor.decompress(compressed_data)?
        } else {
            compressed_data.to_vec()
        };
        
        // 反序列化数据
        let deserialized = serde_json::from_slice(&decompressed)
            .map_err(|e| format!("Failed to deserialize data: {}", e))?;
        
        Ok(deserialized)
    }
}

impl LengthPrefixedEncoder {
    /// 创建新的长度前缀编码器
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(1024),
            max_message_size: 1024 * 1024, // 1MB
        }
    }

    /// 编码消息
    pub fn encode_message(&mut self, message: Message) -> Result<Bytes, String> {
        // 清空缓冲区
        self.buffer.clear();
        
        // 序列化消息 - 简化实现，直接构造字节数组
        let mut serialized = Vec::new();
        serialized.extend_from_slice(&message.message_type as u8 as u8);
        serialized.extend_from_slice(&message.timestamp.elapsed().as_nanos().to_le_bytes());
        serialized.extend_from_slice(&message.data);
        
        if serialized.len() > self.max_message_size {
            return Err("Message too large".to_string());
        }
        
        // 写入长度前缀
        self.buffer.put_u32(serialized.len() as u32);
        
        // 写入消息数据
        self.buffer.put_slice(&serialized);
        
        Ok(self.buffer.freeze())
    }
}

impl LengthPrefixedDecoder {
    /// 创建新的长度前缀解码器
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::new(),
            current_message_length: None,
            bytes_read: 0,
        }
    }

    /// 输入数据
    pub fn feed_data(&mut self, data: &[u8]) {
        self.buffer.put_slice(data);
    }

    /// 获取下一个消息
    pub fn next_message(&mut self) -> Result<Option<Message>, String> {
        loop {
            if self.current_message_length.is_none() {
                // 读取长度前缀
                if self.buffer.len() < 4 {
                    return Ok(None);
                }
                
                let length_bytes = &self.buffer[0..4];
                let length = u32::from_le_bytes([length_bytes[0], length_bytes[1], length_bytes[2], length_bytes[3]]) as usize;
                
                if length > 1024 * 1024 { // 1MB限制
                    return Err("Message too large".to_string());
                }
                
                self.current_message_length = Some(length);
                self.bytes_read = 0;
                
                // 移除长度前缀
                self.buffer.advance(4);
            }
            
            let message_length = self.current_message_length.unwrap();
            
            if self.buffer.len() < message_length {
                return Ok(None);
            }
            
            // 提取消息数据
            let message_data = self.buffer.split_to(message_length);
            
            // 简化的消息创建
            let message = Message {
                message_type: MessageType::Data,
                data: message_data,
                message_id: 0,
                timestamp: Instant::now(),
            };
            
            // 重置状态
            self.current_message_length = None;
            self.bytes_read = 0;
            
            return Ok(Some(message));
        }
    }
}

impl Compressor {
    /// 创建新的压缩器
    pub fn new(level: u32, algorithm: CompressionAlgorithm) -> Self {
        Self { level, algorithm }
    }

    /// 压缩数据
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        match self.algorithm {
            CompressionAlgorithm::Zstd => {
                zstd::encode_all(data, self.level as i32)
                    .map_err(|e| format!("ZSTD compression failed: {}", e))
            },
            CompressionAlgorithm::Lz4 => {
                Ok(lz4_flex::compress(data))
            },
            CompressionAlgorithm::Gzip => {
                use flate2::write::GzEncoder;
                use flate2::Compression;
                use std::io::Write;
                
                let mut encoder = GzEncoder::new(Vec::new(), Compression::new(self.level as u32));
                encoder.write_all(data)
                    .map_err(|e| format!("Gzip compression failed: {}", e))?;
                encoder.finish()
                    .map_err(|e| format!("Gzip compression failed: {}", e))
            },
        }
    }

    /// 解压缩数据
    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        match self.algorithm {
            CompressionAlgorithm::Zstd => {
                zstd::decode_all(data)
                    .map_err(|e| format!("ZSTD decompression failed: {}", e))
            },
            CompressionAlgorithm::Lz4 => {
                lz4_flex::decompress(data, data.len() * 4) // 假设压缩比不超过4:1
                    .map_err(|e| format!("LZ4 decompression failed: {}", e))
            },
            CompressionAlgorithm::Gzip => {
                use flate2::read::GzDecoder;
                use std::io::Read;
                
                let mut decoder = GzDecoder::new(data);
                let mut result = Vec::new();
                decoder.read_to_end(&mut result)
                    .map_err(|e| format!("Gzip decompression failed: {}", e))?;
                Ok(result)
            },
        }
    }
}

impl NetworkStats {
    /// 创建新的网络统计信息
    pub fn new() -> Self {
        Self {
            total_connections: AtomicU64::new(0),
            active_connections: AtomicUsize::new(0),
            total_messages: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            avg_latency: AtomicU64::new(0),
        }
    }

    /// 记录连接
    pub fn record_connection(&self) {
        self.total_connections.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录断开连接
    pub fn record_disconnection(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    /// 记录消息
    pub fn record_message(&self) {
        self.total_messages.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录字节数
    pub fn record_bytes(&self, bytes: u64) {
        self.total_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// 记录错误
    pub fn record_error(&self) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);
    }
}

impl ConnectionStats {
    /// 创建新的连接统计信息
    pub fn new(connection_id: ConnectionId) -> Self {
        Self {
            connection_id,
            message_count: AtomicU64::new(0),
            byte_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            connected_at: Instant::now(),
            last_activity: AtomicU64::new(Instant::now().elapsed().as_millis() as u64),
        }
    }

    /// 记录消息
    pub fn record_message(&self) {
        self.message_count.fetch_add(1, Ordering::Relaxed);
        self.last_activity.store(Instant::now().elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    /// 记录字节数
    pub fn record_bytes(&self, bytes: u64) {
        self.byte_count.fetch_add(bytes, Ordering::Relaxed);
        self.last_activity.store(Instant::now().elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    /// 记录错误
    pub fn record_error(&self) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
            max_connections: 1000,
            buffer_size: 4096,
            timeout: Duration::from_secs(30),
            enable_zero_copy: true,
            enable_compression: true,
            compression_level: 3,
        }
    }
}

impl Default for NetworkStats {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for LengthPrefixedEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for LengthPrefixedDecoder {
    fn default() -> Self {
        Self::new()
    }
}
