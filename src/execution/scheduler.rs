use crate::execution::driver::Driver;
use anyhow::Result;
use std::sync::Arc;
use std::collections::VecDeque;

/// Scheduler for managing driver execution
pub struct ExecutionScheduler {
    pub drivers: VecDeque<Arc<Driver>>,
    pub running: bool,
}

impl ExecutionScheduler {
    pub fn new() -> Self {
        Self {
            drivers: VecDeque::new(),
            running: false,
        }
    }

    pub fn submit_driver(&mut self, driver: Arc<Driver>) {
        self.drivers.push_back(driver);
    }

    pub fn start(&mut self) -> Result<()> {
        self.running = true;
        Ok(())
    }

    pub fn stop(&mut self) -> Result<()> {
        self.running = false;
        Ok(())
    }
}
