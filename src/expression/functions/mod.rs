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

//! Expression function evaluation framework
//! 
//! Provides extensible function evaluation framework with individual function implementations

pub mod arithmetic;
pub mod comparison;
pub mod logical;
pub mod math;
pub mod string;
pub mod date;
pub mod aggregate;

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use anyhow::Result;
use std::sync::Arc;

/// Function evaluation context
#[derive(Debug, Clone)]
pub struct FunctionContext {
    /// Input batch
    pub batch: Arc<RecordBatch>,
    /// Function arguments
    pub args: Vec<ArrayRef>,
    /// Execution statistics
    pub stats: FunctionStats,
}

/// Function execution statistics
#[derive(Debug, Clone, Default)]
pub struct FunctionStats {
    pub execution_count: u64,
    pub execution_time: std::time::Duration,
    pub memory_used: usize,
}

/// Function evaluation result
#[derive(Debug, Clone)]
pub struct FunctionResult {
    /// Result array
    pub result: ArrayRef,
    /// Function execution statistics
    pub stats: FunctionStats,
}

/// Function arity
#[derive(Debug, Clone, PartialEq)]
pub enum FunctionArity {
    /// Exact number of arguments
    Exact(usize),
    /// Fixed number of arguments (alias for Exact)
    Fixed(usize),
    /// Variable number of arguments
    Variadic,
    /// Range of arguments (min, max)
    Range(usize, usize),
}

/// Function evaluator trait
pub trait FunctionEvaluator: Send + Sync {
    /// Function name
    fn name(&self) -> &str;
    /// Function description
    fn description(&self) -> &str;
    /// Function arity
    fn arity(&self) -> FunctionArity;
    /// Return type
    fn return_type(&self, arg_types: &[arrow::datatypes::DataType]) -> Result<arrow::datatypes::DataType>;
    /// Evaluate function
    fn evaluate(&self, context: &FunctionContext) -> Result<FunctionResult>;
    /// Whether supports vectorization (default: true)
    fn supports_vectorization(&self) -> bool {
        true
    }
    /// Whether supports SIMD (default: true)
    fn supports_simd(&self) -> bool {
        true
    }
}

/// Function registry
pub struct FunctionRegistry {
    functions: std::collections::HashMap<String, Box<dyn FunctionEvaluator>>,
}

impl FunctionRegistry {
    /// Create new function registry
    pub fn new() -> Self {
        Self {
            functions: std::collections::HashMap::new(),
        }
    }
    
    /// Register function
    pub fn register<F>(&mut self, function: F) 
    where 
        F: FunctionEvaluator + 'static,
    {
        self.functions.insert(function.name().to_string(), Box::new(function));
    }
    
    /// Get function
    pub fn get(&self, name: &str) -> Option<&dyn FunctionEvaluator> {
        self.functions.get(name).map(|f| f.as_ref())
    }
    
    /// List all functions
    pub fn list_functions(&self) -> Vec<&str> {
        self.functions.keys().map(|k| k.as_str()).collect()
    }
}

/// Function evaluation engine
pub struct FunctionEvaluatorEngine {
    registry: FunctionRegistry,
    stats: FunctionStats,
}

impl FunctionEvaluatorEngine {
    /// Create new function evaluator
    pub fn new() -> Self {
        let mut registry = FunctionRegistry::new();
        
        // Register arithmetic functions
        registry.register(arithmetic::AddFunction::new());
        registry.register(arithmetic::SubtractFunction::new());
        registry.register(arithmetic::MultiplyFunction::new());
        registry.register(arithmetic::DivideFunction::new());
        registry.register(arithmetic::ModuloFunction::new());
        registry.register(arithmetic::PowerFunction::new());
        
        // Register comparison functions
        registry.register(comparison::EqualFunction::new());
        registry.register(comparison::NotEqualFunction::new());
        registry.register(comparison::LessThanFunction::new());
        registry.register(comparison::LessThanOrEqualFunction::new());
        registry.register(comparison::GreaterThanFunction::new());
        registry.register(comparison::GreaterThanOrEqualFunction::new());
        
        // Register logical functions
        registry.register(logical::AndFunction::new());
        registry.register(logical::OrFunction::new());
        registry.register(logical::NotFunction::new());
        
        // Register math functions
        registry.register(math::AbsFunction::new());
        registry.register(math::SqrtFunction::new());
        registry.register(math::SinFunction::new());
        registry.register(math::CosFunction::new());
        registry.register(math::ExpFunction::new());
        registry.register(math::LnFunction::new());
        
        // Register date/time functions
        registry.register(date::CurrentDateFunction::new());
        registry.register(date::CurrentTimeFunction::new());
        registry.register(date::CurrentTimestampFunction::new());
        registry.register(date::DateAddFunction::new());
        registry.register(date::DateSubFunction::new());
        registry.register(date::DateDiffFunction::new());
        registry.register(date::ExtractFunction::new());
        registry.register(date::FormatDateFunction::new());
        
        // Register string functions
        registry.register(string::ConcatFunction::new());
        registry.register(string::SubstringFunction::new());
        registry.register(string::LengthFunction::new());
        registry.register(string::UpperFunction::new());
        registry.register(string::LowerFunction::new());
        registry.register(string::TrimFunction::new());
        registry.register(string::ReplaceFunction::new());
        registry.register(string::RegexMatchFunction::new());
        
        Self {
            registry,
            stats: FunctionStats::default(),
        }
    }
    
    /// Evaluate function
    pub fn evaluate(&mut self, name: &str, context: &FunctionContext) -> Result<FunctionResult> {
        let function = self.registry.get(name)
            .ok_or_else(|| anyhow::anyhow!("Function not found: {}", name))?;
        
        let start = std::time::Instant::now();
        let result = function.evaluate(context)?;
        let execution_time = start.elapsed();
        
        // Update statistics
        self.stats.execution_count += 1;
        self.stats.execution_time += execution_time;
        
        Ok(result)
    }
    
    /// Get statistics
    pub fn get_stats(&self) -> &FunctionStats {
        &self.stats
    }
    
    /// List all functions
    pub fn list_functions(&self) -> Vec<&str> {
        self.registry.list_functions()
    }
}

impl Default for FunctionEvaluatorEngine {
    fn default() -> Self {
        Self::new()
    }
}