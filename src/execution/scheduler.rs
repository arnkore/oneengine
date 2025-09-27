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
