use crate::columnar::batch::Batch;
use crate::execution::context::ExecContext;
use anyhow::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Trait for vectorized operators
pub trait Operator: Send {
    /// Prepare the operator for execution
    fn prepare(&mut self, ctx: &ExecContext) -> Result<()>;
    
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
    fn prepare(&mut self, _ctx: &ExecContext) -> Result<()> {
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
