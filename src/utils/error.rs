use thiserror::Error;

/// OneEngine error types
#[derive(Error, Debug)]
pub enum OneEngineError {
    #[error("Configuration error: {0}")]
    ConfigError(String),
    
    #[error("Scheduler error: {0}")]
    SchedulerError(String),
    
    #[error("Executor error: {0}")]
    ExecutorError(String),
    
    #[error("Memory error: {0}")]
    MemoryError(String),
    
    #[error("Protocol error: {0}")]
    ProtocolError(String),
    
    #[error("Task error: {0}")]
    TaskError(String),
    
    #[error("Pipeline error: {0}")]
    PipelineError(String),
    
    #[error("Resource error: {0}")]
    ResourceError(String),
    
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
    
    #[error("YAML error: {0}")]
    YamlError(#[from] serde_yaml::Error),
    
    #[error("Anyhow error: {0}")]
    AnyhowError(#[from] anyhow::Error),
}

/// Result type for OneEngine operations
pub type OneEngineResult<T> = Result<T, OneEngineError>;
