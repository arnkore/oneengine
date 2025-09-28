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

//! 表达式缓存模块
//! 
//! 提供表达式缓存和复用机制

pub mod expression_cache;
pub mod result_cache;

use crate::expression::ast::Expression;
use crate::expression::jit::JitFunction;
use std::collections::HashMap;
use std::sync::Arc;

/// 表达式缓存
pub struct ExpressionCache {
    compiled_cache: HashMap<Expression, JitFunction>,
    hit_count: std::sync::atomic::AtomicU64,
    miss_count: std::sync::atomic::AtomicU64,
    size_limit: usize,
}

impl ExpressionCache {
    /// 创建新的表达式缓存
    pub fn new(size_limit: usize) -> Self {
        Self {
            compiled_cache: HashMap::new(),
            hit_count: std::sync::atomic::AtomicU64::new(0),
            miss_count: std::sync::atomic::AtomicU64::new(0),
            size_limit,
        }
    }

    /// 获取编译后的表达式
    pub fn get_compiled(&self, expression: &Expression) -> Option<JitFunction> {
        self.compiled_cache.get(expression).cloned()
    }

    /// 缓存编译后的表达式
    pub fn put_compiled(&mut self, expression: Expression, compiled: JitFunction) {
        if self.compiled_cache.len() < self.size_limit {
            self.compiled_cache.insert(expression, compiled);
        }
    }

    /// 获取缓存命中次数
    pub fn get_hit_count(&self) -> u64 {
        self.hit_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// 获取缓存未命中次数
    pub fn get_miss_count(&self) -> u64 {
        self.miss_count.load(std::sync::atomic::Ordering::Relaxed)
    }
}
