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


use crate::execution::task::Task;
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
