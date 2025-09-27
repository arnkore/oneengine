use crate::core::task::Task;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Base trait for all operators
#[async_trait::async_trait]
pub trait Operator: Send + Sync {
    /// Execute the operator
    async fn execute(&self, input: OperatorInput) -> Result<OperatorOutput>;
    
    /// Get operator metadata
    fn get_metadata(&self) -> OperatorMetadata;
}

/// Input to an operator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorInput {
    pub data: Vec<u8>,
    pub schema: Option<String>,
    pub metadata: HashMap<String, String>,
}

/// Output from an operator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorOutput {
    pub data: Vec<u8>,
    pub schema: Option<String>,
    pub metadata: HashMap<String, String>,
}

/// Operator metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorMetadata {
    pub name: String,
    pub version: String,
    pub description: Option<String>,
    pub input_schema: Option<String>,
    pub output_schema: Option<String>,
}

/// Map operator for data transformation
pub struct MapOperator {
    metadata: OperatorMetadata,
    transform_fn: Box<dyn Fn(Vec<u8>) -> Result<Vec<u8>> + Send + Sync>,
}

impl MapOperator {
    pub fn new(
        name: String,
        transform_fn: Box<dyn Fn(Vec<u8>) -> Result<Vec<u8>> + Send + Sync>,
    ) -> Self {
        Self {
            metadata: OperatorMetadata {
                name,
                version: "1.0.0".to_string(),
                description: None,
                input_schema: None,
                output_schema: None,
            },
            transform_fn,
        }
    }
}

#[async_trait::async_trait]
impl Operator for MapOperator {
    async fn execute(&self, input: OperatorInput) -> Result<OperatorOutput> {
        let transformed_data = (self.transform_fn)(input.data)?;
        
        Ok(OperatorOutput {
            data: transformed_data,
            schema: input.schema,
            metadata: input.metadata,
        })
    }
    
    fn get_metadata(&self) -> OperatorMetadata {
        self.metadata.clone()
    }
}

/// Filter operator for data filtering
pub struct FilterOperator {
    metadata: OperatorMetadata,
    predicate_fn: Box<dyn Fn(&[u8]) -> bool + Send + Sync>,
}

impl FilterOperator {
    pub fn new(
        name: String,
        predicate_fn: Box<dyn Fn(&[u8]) -> bool + Send + Sync>,
    ) -> Self {
        Self {
            metadata: OperatorMetadata {
                name,
                version: "1.0.0".to_string(),
                description: None,
                input_schema: None,
                output_schema: None,
            },
            predicate_fn,
        }
    }
}

#[async_trait::async_trait]
impl Operator for FilterOperator {
    async fn execute(&self, input: OperatorInput) -> Result<OperatorOutput> {
        let should_keep = (self.predicate_fn)(&input.data);
        
        if should_keep {
            Ok(OperatorOutput {
                data: input.data,
                schema: input.schema,
                metadata: input.metadata,
            })
        } else {
            // Return empty data for filtered out records
            Ok(OperatorOutput {
                data: Vec::new(),
                schema: input.schema,
                metadata: input.metadata,
            })
        }
    }
    
    fn get_metadata(&self) -> OperatorMetadata {
        self.metadata.clone()
    }
}
