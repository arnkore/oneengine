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


use crate::execution::vectorized_operator::BoxedOperator;
use crate::execution::context::ExecContext;
use crate::push_runtime::{Event, event_loop::EventLoop, OperatorContext, OpStatus, outbox::Outbox, PortId, OperatorId};
use crate::push_runtime::metrics::SimpleMetricsCollector;
use arrow::record_batch::RecordBatch;
use anyhow::Result;
use std::time::{Duration, Instant};
use std::sync::Arc;
use std::collections::VecDeque;
use tracing::{debug, info, warn, error};

/// A driver that executes a pipeline of operators
pub struct Driver {
    pub operators: Vec<BoxedOperator>,
    pub state: DriverState,
    event_loop: EventLoop,
    operator_contexts: Vec<OperatorContext>,
    outboxes: Vec<Outbox<'static>>,
    execution_stats: ExecutionStats,
    resource_limits: ResourceLimits,
    event_queue: VecDeque<Event>,
    port_mappings: std::collections::HashMap<PortId, OperatorId>,
    running_operators: std::collections::HashSet<OperatorId>,
    blocked_operators: std::collections::HashSet<OperatorId>,
    completed_operators: std::collections::HashSet<OperatorId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriverState {
    Created,
    Running,
    Paused,
    Finished,
    Error,
}

/// Execution statistics for the driver
#[derive(Debug, Clone)]
pub struct ExecutionStats {
    pub total_execution_time: Duration,
    pub operator_execution_times: std::collections::HashMap<OperatorId, Duration>,
    pub batches_processed: u64,
    pub rows_processed: u64,
    pub memory_used: usize,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub spill_count: u64,
    pub spill_size: usize,
}

impl Default for ExecutionStats {
    fn default() -> Self {
        Self {
            total_execution_time: Duration::ZERO,
            operator_execution_times: std::collections::HashMap::new(),
            batches_processed: 0,
            rows_processed: 0,
            memory_used: 0,
            cache_hits: 0,
            cache_misses: 0,
            spill_count: 0,
            spill_size: 0,
        }
    }
}

/// Resource limits for the driver
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    pub max_memory: usize,
    pub max_execution_time: Duration,
    pub max_batches_per_cycle: usize,
    pub max_events_per_cycle: usize,
    pub enable_backpressure: bool,
    pub enable_spilling: bool,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_memory: 1024 * 1024 * 1024, // 1GB
            max_execution_time: Duration::from_secs(300), // 5 minutes
            max_batches_per_cycle: 1000,
            max_events_per_cycle: 10000,
            enable_backpressure: true,
            enable_spilling: true,
        }
    }
}

impl Driver {
    pub fn new(operators: Vec<BoxedOperator>) -> Self {
        let metrics = Arc::new(SimpleMetricsCollector::default());
        let event_loop = EventLoop::new(metrics);
        
        // Create operator contexts and outboxes
        let mut operator_contexts = Vec::new();
        let mut outboxes = Vec::new();
        let mut port_mappings = std::collections::HashMap::new();
        
        for (i, _operator) in operators.iter().enumerate() {
            let operator_id = i as u32;
            let context = OperatorContext::new(operator_id);
            let outbox = Outbox::new(operator_id);
            
            operator_contexts.push(context);
            outboxes.push(outbox);
            
            // Map ports to operators (simplified mapping)
            port_mappings.insert(i as PortId, operator_id);
        }
        
        Self {
            operators,
            state: DriverState::Created,
            event_loop,
            operator_contexts,
            outboxes,
            execution_stats: ExecutionStats::default(),
            resource_limits: ResourceLimits::default(),
            event_queue: VecDeque::new(),
            port_mappings,
            running_operators: std::collections::HashSet::new(),
            blocked_operators: std::collections::HashSet::new(),
            completed_operators: std::collections::HashSet::new(),
        }
    }

    /// Initialize the driver
    pub fn initialize(&mut self) -> Result<()> {
        info!("Initializing driver with {} operators", self.operators.len());
        
        // Initialize all operators
        for (i, operator) in self.operators.iter_mut().enumerate() {
            let operator_id = i as u32;
            let context = &self.operator_contexts[i];
            
            debug!("Initializing operator {}: {:?}", operator_id, operator.name());
            
            // Initialize operator
            operator.initialize(context)?;
            
            // Add to running operators
            self.running_operators.insert(operator_id);
        }
        
        self.state = DriverState::Running;
        info!("Driver initialized successfully");
        Ok(())
    }

    /// Run the driver for a specified time budget
    pub fn run_for(&mut self, budget: Duration) -> Result<DriverYield> {
        let start_time = Instant::now();
        let mut batches_processed = 0;
        let mut events_processed = 0;
        
        debug!("Starting driver execution with budget: {:?}", budget);
        
        while start_time.elapsed() < budget {
            // Check resource limits
            if self.check_resource_limits()? {
                return Ok(DriverYield::Blocked);
            }
            
            // Process events
            let events_this_cycle = self.process_events()?;
            events_processed += events_this_cycle;
            
            if events_this_cycle == 0 {
                // No more events to process
                break;
            }
            
            // Process operators
            let batches_this_cycle = self.process_operators()?;
            batches_processed += batches_this_cycle;
            
            // Check if all operators are completed
            if self.all_operators_completed() {
                self.state = DriverState::Finished;
                break;
            }
            
            // Check cycle limits
            if batches_processed >= self.resource_limits.max_batches_per_cycle {
                debug!("Reached max batches per cycle: {}", batches_processed);
                break;
            }
            
            if events_processed >= self.resource_limits.max_events_per_cycle {
                debug!("Reached max events per cycle: {}", events_processed);
                break;
            }
        }
        
        // Update execution stats
        self.execution_stats.total_execution_time += start_time.elapsed();
        self.execution_stats.batches_processed += batches_processed as u64;
        
        // Determine yield reason
        if self.state == DriverState::Finished {
            Ok(DriverYield::Complete)
        } else if start_time.elapsed() >= budget {
            Ok(DriverYield::Timeout)
        } else if self.all_operators_blocked() {
            Ok(DriverYield::Blocked)
        } else {
            Ok(DriverYield::Complete)
        }
    }

    /// Process events in the event loop
    fn process_events(&mut self) -> Result<usize> {
        let mut events_processed = 0;
        
        while let Some(event) = self.event_loop.get_next_event() {
            debug!("Processing event: {:?}", event);
            
            match self.event_loop.process_event(event) {
                Ok(has_more) => {
                    events_processed += 1;
                    if !has_more {
                        break;
                    }
                },
                Err(e) => {
                    error!("Event processing failed: {}", e);
                    self.state = DriverState::Error;
                    return Err(e);
                }
            }
        }
        
        Ok(events_processed)
    }

    /// Process all operators
    fn process_operators(&mut self) -> Result<usize> {
        let mut batches_processed = 0;
        
        for (i, operator) in self.operators.iter_mut().enumerate() {
            let operator_id = i as u32;
            
            // Skip if operator is completed or blocked
            if self.completed_operators.contains(&operator_id) || 
               self.blocked_operators.contains(&operator_id) {
                continue;
            }
            
            let context = &self.operator_contexts[i];
            let outbox = &mut self.outboxes[i];
            
            // Execute operator
            let start_time = Instant::now();
            let status = operator.execute(context, outbox)?;
            let execution_time = start_time.elapsed();
            
            // Update execution stats
            self.execution_stats.operator_execution_times
                .entry(operator_id)
                .and_modify(|t| *t += execution_time)
                .or_insert(execution_time);
            
            // Handle operator status
            match status {
                OpStatus::Finished => {
                    debug!("Operator {} finished", operator_id);
                    self.completed_operators.insert(operator_id);
                    self.running_operators.remove(&operator_id);
                },
                OpStatus::Blocked => {
                    debug!("Operator {} blocked", operator_id);
                    self.blocked_operators.insert(operator_id);
                    self.running_operators.remove(&operator_id);
                },
                OpStatus::Error(e) => {
                    error!("Operator {} error: {}", operator_id, e);
                    self.state = DriverState::Error;
                    return Err(anyhow::anyhow!("Operator {} error: {}", operator_id, e));
                },
                OpStatus::HasMore => {
                    // Operator has more work to do
                    batches_processed += 1;
                }
            }
        }
        
        Ok(batches_processed)
    }

    /// Check resource limits
    fn check_resource_limits(&self) -> Result<bool> {
        // Check memory usage
        if self.execution_stats.memory_used > self.resource_limits.max_memory {
            warn!("Memory limit exceeded: {} > {}", 
                  self.execution_stats.memory_used, 
                  self.resource_limits.max_memory);
            return Ok(true);
        }
        
        // Check execution time
        if self.execution_stats.total_execution_time > self.resource_limits.max_execution_time {
            warn!("Execution time limit exceeded: {:?} > {:?}", 
                  self.execution_stats.total_execution_time, 
                  self.resource_limits.max_execution_time);
            return Ok(true);
        }
        
        Ok(false)
    }

    /// Check if all operators are completed
    fn all_operators_completed(&self) -> bool {
        self.completed_operators.len() == self.operators.len()
    }

    /// Check if all operators are blocked
    fn all_operators_blocked(&self) -> bool {
        self.running_operators.is_empty() && 
        !self.completed_operators.is_empty() &&
        self.blocked_operators.len() + self.completed_operators.len() == self.operators.len()
    }

    /// Add event to the event queue
    pub fn add_event(&mut self, event: Event) {
        self.event_loop.add_event(event);
    }

    /// Get execution statistics
    pub fn get_stats(&self) -> &ExecutionStats {
        &self.execution_stats
    }

    /// Get driver state
    pub fn get_state(&self) -> DriverState {
        self.state
    }

    /// Reset the driver
    pub fn reset(&mut self) {
        self.state = DriverState::Created;
        self.running_operators.clear();
        self.blocked_operators.clear();
        self.completed_operators.clear();
        self.execution_stats = ExecutionStats::default();
        self.event_queue.clear();
    }

    /// Shutdown the driver
    pub fn shutdown(&mut self) -> Result<()> {
        info!("Shutting down driver");
        
        // Shutdown all operators
        for (i, operator) in self.operators.iter_mut().enumerate() {
            let context = &self.operator_contexts[i];
            operator.shutdown(context)?;
        }
        
        self.state = DriverState::Finished;
        info!("Driver shutdown complete");
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriverYield {
    Complete,
    Timeout,
    Blocked,
    Error,
}
