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


//! 算子上下文
//! 
//! 提供算子运行时的上下文信息

use crate::push_runtime::{PortId, OperatorId, MetricsCollector};
use std::sync::Arc;

/// 算子上下文
pub struct OperatorContext {
    /// 算子ID
    pub operator_id: OperatorId,
    /// 输入端口
    pub input_ports: Vec<PortId>,
    /// 输出端口
    pub output_ports: Vec<PortId>,
    /// 指标收集器
    pub metrics: Arc<dyn MetricsCollector>,
}

impl OperatorContext {
    /// 创建新的算子上下文
    pub fn new(
        operator_id: OperatorId,
        input_ports: Vec<PortId>,
        output_ports: Vec<PortId>,
        metrics: Arc<dyn MetricsCollector>,
    ) -> Self {
        Self {
            operator_id,
            input_ports,
            output_ports,
            metrics,
        }
    }
}
