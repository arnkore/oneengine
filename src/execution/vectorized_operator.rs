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


use crate::columnar::batch::Batch;
use anyhow::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Trait for vectorized operators
pub trait Operator: Send {
    /// Prepare the operator for execution
    fn prepare(&mut self) -> Result<()>;
    
    /// Get the next batch of data (non-blocking)
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Batch>>>;
    
    /// Close the operator and clean up resources
    fn close(&mut self) -> Result<()>;
    
    /// Get the schema of the output
    fn output_schema(&self) -> &crate::columnar::batch::BatchSchema;
}

/// A boxed operator for dynamic dispatch
pub type BoxedOperator = Box<dyn Operator>;

/// Operator state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorState {
    Prepared,
    Running,
    Finished,
    Error,
}

/// Base operator implementation
pub struct BaseOperator {
    pub state: OperatorState,
    pub output_schema: crate::columnar::batch::BatchSchema,
}

impl BaseOperator {
    pub fn new(output_schema: crate::columnar::batch::BatchSchema) -> Self {
        Self {
            state: OperatorState::Prepared,
            output_schema,
        }
    }
}

impl Operator for BaseOperator {
    fn prepare(&mut self) -> Result<()> {
        self.state = OperatorState::Running;
        Ok(())
    }

    fn poll_next(&mut self, _cx: &mut Context<'_>) -> Poll<Result<Option<Batch>>> {
        Poll::Ready(Ok(None))
    }

    fn close(&mut self) -> Result<()> {
        self.state = OperatorState::Finished;
        Ok(())
    }

    fn output_schema(&self) -> &crate::columnar::batch::BatchSchema {
        &self.output_schema
    }
}
