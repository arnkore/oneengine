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


//! DataLake模块
//! 
//! 提供数据湖存储、格式适配器等功能

pub mod unified_lake_reader;
pub mod adapters;

// 重新导出常用类型
pub use unified_lake_reader::{
    UnifiedLakeReader, UnifiedLakeReaderConfig, LakeFormat, UnifiedPredicate,
    TimeTravelConfig, IncrementalReadConfig, PartitionPruningInfo, ColumnProjection,
    TableMetadata, TableStatistics, FormatAdapter
};
