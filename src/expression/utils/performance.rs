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

//! 性能监控工具
//! 
//! 提供表达式执行性能监控功能

use std::time::Instant;

/// 性能监控器
pub struct PerformanceMonitor {
    start_time: Option<Instant>,
}

impl PerformanceMonitor {
    /// 创建新的性能监控器
    pub fn new() -> Self {
        Self {
            start_time: None,
        }
    }

    /// 开始监控
    pub fn start(&mut self) {
        self.start_time = Some(Instant::now());
    }

    /// 结束监控
    pub fn end(&mut self) -> Option<std::time::Duration> {
        self.start_time.take().map(|start| start.elapsed())
    }
}
