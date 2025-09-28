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

//! Iceberg格式适配器
//! 
//! 实现Apache Iceberg表的真实数据读取和优化功能

use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Schema, Field, DataType, SchemaRef};
use arrow::array::{ArrayRef, StringArray, Int64Array, Float64Array, BooleanArray, TimestampMicrosecondArray};
use std::collections::HashMap;
use std::path::Path;
use std::fs;
use std::sync::Arc;
use anyhow::Result;
use tracing::{debug, info, warn, error};
use serde_json::Value;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use avro_rs::{Reader, from_value, Schema as AvroSchema};
use serde::{Deserialize, Serialize};

use crate::datalake::unified_lake_reader::{
    FormatAdapter, TableMetadata, TableStatistics, UnifiedPredicate,
    ColumnProjection, PartitionPruningInfo, TimeTravelConfig, IncrementalReadConfig,
    SnapshotInfo, PartitionSpec, PartitionField, SortOrder, SortField,
};

/// Iceberg表元数据
#[derive(Debug, Clone)]
struct IcebergTableMetadata {
    table_id: String,
    table_name: String,
    namespace: String,
    location: String,
    current_snapshot_id: Option<i64>,
    snapshots: Vec<IcebergSnapshot>,
    partition_specs: HashMap<i32, PartitionSpec>,
    default_spec_id: i32,
    sort_orders: HashMap<i32, SortOrder>,
    default_sort_order_id: i32,
    current_schema_id: i32,
    schemas: HashMap<i32, Schema>,
    properties: HashMap<String, String>,
    created_at: i64,
    updated_at: i64,
}

/// Iceberg快照
#[derive(Debug, Clone)]
struct IcebergSnapshot {
    snapshot_id: i64,
    timestamp_ms: i64,
    parent_snapshot_id: Option<i64>,
    manifest_list: String,
    summary: HashMap<String, String>,
    schema_id: i32,
}

/// Iceberg文件清单条目
#[derive(Debug, Clone)]
struct ManifestEntry {
    file_path: String,
    file_format: String,
    partition_values: HashMap<String, String>,
    file_size_bytes: u64,
    record_count: u64,
    column_sizes: HashMap<String, u64>,
    value_counts: HashMap<String, u64>,
    null_value_counts: HashMap<String, u64>,
    lower_bounds: HashMap<String, Vec<u8>>,
    upper_bounds: HashMap<String, Vec<u8>>,
    key_metadata: Option<Vec<u8>>,
    split_offsets: Vec<i64>,
    sort_order_id: Option<i32>,
}

/// Avro Manifest List条目
#[derive(Debug, Deserialize, Serialize)]
struct AvroManifestListEntry {
    manifest_path: String,
    manifest_length: i64,
    partition_spec_id: i32,
    content: i32,
    sequence_number: i64,
    min_sequence_number: i64,
    added_snapshot_id: i64,
    added_files_count: i32,
    existing_files_count: i32,
    deleted_files_count: i32,
    added_rows_count: i64,
    existing_rows_count: i64,
    deleted_rows_count: i64,
    partitions: Option<Vec<AvroPartitionFieldSummary>>,
    key_metadata: Option<Vec<u8>>,
}

/// Avro分区字段摘要
#[derive(Debug, Deserialize, Serialize)]
struct AvroPartitionFieldSummary {
    contains_null: bool,
    contains_nan: Option<bool>,
    lower_bound: Option<Vec<u8>>,
    upper_bound: Option<Vec<u8>>,
}

/// Avro Manifest条目
#[derive(Debug, Deserialize, Serialize)]
struct AvroManifestEntry {
    status: i32,
    snapshot_id: i64,
    sequence_number: Option<i64>,
    file_sequence_number: Option<i64>,
    data_file: AvroDataFile,
}

/// Avro数据文件
#[derive(Debug, Deserialize, Serialize)]
struct AvroDataFile {
    content: i32,
    file_path: String,
    file_format: String,
    partition: HashMap<String, String>,
    record_count: i64,
    file_size_in_bytes: i64,
    column_sizes: Option<HashMap<String, i64>>,
    value_counts: Option<HashMap<String, i64>>,
    null_value_counts: Option<HashMap<String, i64>>,
    nan_value_counts: Option<HashMap<String, i64>>,
    lower_bounds: Option<HashMap<String, Vec<u8>>>,
    upper_bounds: Option<HashMap<String, Vec<u8>>>,
    key_metadata: Option<Vec<u8>>,
    split_offsets: Option<Vec<i64>>,
    sort_order_id: Option<i32>,
}

/// Iceberg适配器
pub struct IcebergAdapter {
    /// 表路径
    table_path: String,
    /// 表元数据
    table_metadata: Option<TableMetadata>,
    /// Iceberg表元数据
    iceberg_metadata: Option<IcebergTableMetadata>,
    /// 当前快照ID
    current_snapshot_id: Option<i64>,
    /// 当前快照
    current_snapshot: Option<IcebergSnapshot>,
    /// 文件清单条目
    manifest_entries: Vec<ManifestEntry>,
    /// 谓词列表
    predicates: Vec<UnifiedPredicate>,
    /// 列投影
    column_projection: Option<ColumnProjection>,
    /// 分区剪枝信息
    partition_pruning: Option<PartitionPruningInfo>,
    /// 时间旅行配置
    time_travel: Option<TimeTravelConfig>,
    /// 增量读取配置
    incremental_read: Option<IncrementalReadConfig>,
    /// 表Schema
    table_schema: Option<Schema>,
}

impl IcebergAdapter {
    /// 创建新的Iceberg适配器
    pub fn new() -> Self {
        Self {
            table_path: String::new(),
            table_metadata: None,
            iceberg_metadata: None,
            current_snapshot_id: None,
            current_snapshot: None,
            manifest_entries: Vec::new(),
            predicates: Vec::new(),
            column_projection: None,
            partition_pruning: None,
            time_travel: None,
            incremental_read: None,
            table_schema: None,
        }
    }
    
    /// 初始化Iceberg表
    fn initialize_iceberg_table(&mut self, table_path: &str) -> Result<()> {
        info!("Initializing Iceberg table: {}", table_path);
        
        // 检查表路径是否存在
        let path = Path::new(table_path);
        if !path.exists() {
            return Err(anyhow::anyhow!("Table path does not exist: {}", table_path));
        }
        
        // 读取表元数据文件
        let metadata_file = path.join("metadata").join("metadata.json");
        if !metadata_file.exists() {
            return Err(anyhow::anyhow!("Metadata file not found: {:?}", metadata_file));
        }
        
        // 解析Iceberg表元数据
        self.iceberg_metadata = Some(self.parse_iceberg_metadata(&metadata_file)?);
        let iceberg_metadata = self.iceberg_metadata.as_ref().unwrap();
        
        // 设置当前快照
        self.current_snapshot_id = iceberg_metadata.current_snapshot_id;
        if let Some(snapshot_id) = self.current_snapshot_id {
            self.current_snapshot = iceberg_metadata.snapshots.iter()
                .find(|s| s.snapshot_id == snapshot_id)
                .cloned();
        }
        
        // 构建统一表元数据
        self.table_metadata = Some(self.build_unified_table_metadata(iceberg_metadata)?);
        
        // 加载文件清单
        if let Some(snapshot) = &self.current_snapshot {
            self.manifest_entries = self.load_manifest_entries(table_path, snapshot)?;
        }
        
        // 构建表Schema
        self.table_schema = Some(self.build_table_schema(iceberg_metadata)?);
        
        info!("Successfully initialized Iceberg table with {} manifest entries", 
              self.manifest_entries.len());
        
        Ok(())
    }
    
    /// 解析Iceberg表元数据
    fn parse_iceberg_metadata(&self, metadata_file: &Path) -> Result<IcebergTableMetadata> {
        let content = fs::read_to_string(metadata_file)?;
        let json: Value = serde_json::from_str(&content)?;
        
        let table_id = json["table-id"].as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing table-id"))?
            .to_string();
        
        let table_name = json["table-name"].as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing table-name"))?
            .to_string();
        
        let namespace = json["namespace"].as_array()
            .ok_or_else(|| anyhow::anyhow!("Missing namespace"))?
            .iter()
            .filter_map(|v| v.as_str())
            .collect::<Vec<_>>()
            .join(".");
        
        let location = json["location"].as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing location"))?
            .to_string();
        
        let current_snapshot_id = json["current-snapshot-id"].as_i64();
        
        // 解析快照
        let mut snapshots = Vec::new();
        if let Some(snapshots_array) = json["snapshots"].as_array() {
            for snapshot_json in snapshots_array {
                let snapshot = IcebergSnapshot {
                    snapshot_id: snapshot_json["snapshot-id"].as_i64()
                        .ok_or_else(|| anyhow::anyhow!("Missing snapshot-id"))?,
                    timestamp_ms: snapshot_json["timestamp-ms"].as_i64()
                        .ok_or_else(|| anyhow::anyhow!("Missing timestamp-ms"))?,
                    parent_snapshot_id: snapshot_json["parent-snapshot-id"].as_i64(),
                    manifest_list: snapshot_json["manifest-list"].as_str()
                        .ok_or_else(|| anyhow::anyhow!("Missing manifest-list"))?
                        .to_string(),
                    summary: snapshot_json["summary"]
                        .as_object()
                        .map(|obj| obj.iter()
                            .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                            .collect())
                        .unwrap_or_default(),
                    schema_id: snapshot_json["schema-id"].as_i64()
                        .unwrap_or(0) as i32,
                };
                snapshots.push(snapshot);
            }
        }
        
        // 解析分区规范
        let mut partition_specs = HashMap::new();
        if let Some(specs_array) = json["partition-specs"].as_array() {
            for spec_json in specs_array {
                let spec_id = spec_json["spec-id"].as_i64().unwrap_or(0) as i32;
                let mut partition_fields = Vec::new();
                
                if let Some(fields_array) = spec_json["fields"].as_array() {
                    for field_json in fields_array {
                        let field = PartitionField {
                            field_name: field_json["name"].as_str()
                                .unwrap_or("").to_string(),
                            field_id: field_json["field-id"].as_i64().unwrap_or(0) as i32,
                            transform: field_json["transform"].as_str()
                                .unwrap_or("identity").to_string(),
                        };
                        partition_fields.push(field);
                    }
                }
                
                let spec = PartitionSpec {
                    partition_fields,
                    spec_id,
                };
                partition_specs.insert(spec_id, spec);
            }
        }
        
        let default_spec_id = json["default-spec-id"].as_i64().unwrap_or(0) as i32;
        
        // 解析排序顺序
        let mut sort_orders = HashMap::new();
        if let Some(orders_array) = json["sort-orders"].as_array() {
            for order_json in orders_array {
                let order_id = order_json["order-id"].as_i64().unwrap_or(0) as i32;
                let mut sort_fields = Vec::new();
                
                if let Some(fields_array) = order_json["fields"].as_array() {
                    for field_json in fields_array {
                        let field = SortField {
                            field_name: field_json["name"].as_str()
                                .unwrap_or("").to_string(),
                            field_id: field_json["field-id"].as_i64().unwrap_or(0) as i32,
                            direction: field_json["direction"].as_str()
                                .unwrap_or("asc").to_string(),
                            null_order: field_json["null-order"].as_str()
                                .unwrap_or("first").to_string(),
                        };
                        sort_fields.push(field);
                    }
                }
                
                let order = SortOrder {
                    sort_fields,
                    order_id,
                };
                sort_orders.insert(order_id, order);
            }
        }
        
        let default_sort_order_id = json["default-sort-order-id"].as_i64().unwrap_or(0) as i32;
        let current_schema_id = json["current-schema-id"].as_i64().unwrap_or(0) as i32;
        
        // 解析Schema
        let mut schemas = HashMap::new();
        if let Some(schemas_array) = json["schemas"].as_array() {
            for schema_json in schemas_array {
                let schema_id = schema_json["schema-id"].as_i64().unwrap_or(0) as i32;
                let schema = self.parse_iceberg_schema(schema_json)?;
                schemas.insert(schema_id, schema);
            }
        }
        
        // 解析属性
        let properties = json["properties"]
            .as_object()
            .map(|obj| obj.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                .collect())
            .unwrap_or_default();
        
        let created_at = json["created-at"].as_i64().unwrap_or(0);
        let updated_at = json["updated-at"].as_i64().unwrap_or(0);
        
        Ok(IcebergTableMetadata {
            table_id,
            table_name,
            namespace,
            location,
            current_snapshot_id,
            snapshots,
            partition_specs,
            default_spec_id,
            sort_orders,
            default_sort_order_id,
            current_schema_id,
            schemas,
            properties,
            created_at,
            updated_at,
        })
    }
    
    /// 解析Iceberg Schema
    fn parse_iceberg_schema(&self, schema_json: &Value) -> Result<Schema> {
        let mut fields = Vec::new();
        
        if let Some(fields_array) = schema_json["fields"].as_array() {
            for field_json in fields_array {
                let field = self.parse_iceberg_field(field_json)?;
                fields.push(field);
            }
        }
        
        Ok(Schema::new(fields))
    }
    
    /// 解析Iceberg字段
    fn parse_iceberg_field(&self, field_json: &Value) -> Result<Field> {
        let name = field_json["name"].as_str()
            .ok_or_else(|| anyhow::anyhow!("Missing field name"))?
            .to_string();
        
        let field_id = field_json["id"].as_i64().unwrap_or(0) as i32;
        let required = field_json["required"].as_bool().unwrap_or(false);
        
        let data_type = self.parse_iceberg_type(field_json["type"].as_object()
            .ok_or_else(|| anyhow::anyhow!("Missing field type"))?)?;
        
        let mut metadata = HashMap::new();
        metadata.insert("field-id".to_string(), field_id.to_string());
        
        Ok(Field::new(name, data_type, !required).with_metadata(metadata))
    }
    
    /// 解析Iceberg数据类型
    fn parse_iceberg_type(&self, type_obj: &serde_json::Map<String, Value>) -> Result<DataType> {
        let type_name = type_obj.get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing type name"))?;
        
        match type_name {
            "boolean" => Ok(DataType::Boolean),
            "int" => {
                let width = type_obj.get("width")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(32) as i8;
                match width {
                    8 => Ok(DataType::Int8),
                    16 => Ok(DataType::Int16),
                    32 => Ok(DataType::Int32),
                    64 => Ok(DataType::Int64),
                    _ => Ok(DataType::Int32),
                }
            },
            "long" => Ok(DataType::Int64),
            "float" => Ok(DataType::Float32),
            "double" => Ok(DataType::Float64),
            "date" => Ok(DataType::Date32),
            "time" => Ok(DataType::Time64(arrow::datatypes::TimeUnit::Microsecond)),
            "timestamp" => {
                let unit = type_obj.get("unit")
                    .and_then(|v| v.as_str())
                    .unwrap_or("micros");
                match unit {
                    "micros" => Ok(DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)),
                    "millis" => Ok(DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)),
                    "nanos" => Ok(DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)),
                    _ => Ok(DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)),
                }
            },
            "timestamptz" => {
                let unit = type_obj.get("unit")
                    .and_then(|v| v.as_str())
                    .unwrap_or("micros");
                match unit {
                    "micros" => Ok(DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some(Arc::from("UTC")))),
                    "millis" => Ok(DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, Some(Arc::from("UTC")))),
                    "nanos" => Ok(DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, Some(Arc::from("UTC")))),
                    _ => Ok(DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some(Arc::from("UTC")))),
                }
            },
            "string" => Ok(DataType::Utf8),
            "uuid" => Ok(DataType::FixedSizeBinary(16)),
            "binary" => Ok(DataType::Binary),
            "decimal" => {
                let precision = type_obj.get("precision")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(10) as i8;
                let scale = type_obj.get("scale")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0) as i8;
                Ok(DataType::Decimal128(precision as u8, scale))
            },
            "struct" => {
                let mut struct_fields = Vec::new();
                if let Some(fields_array) = type_obj.get("fields").and_then(|v| v.as_array()) {
                    for field_json in fields_array {
                        let field = self.parse_iceberg_field(field_json)?;
                        struct_fields.push(field);
                    }
                }
                Ok(DataType::Struct(struct_fields.into()))
            },
            "list" => {
                let element_type = type_obj.get("element")
                    .and_then(|v| v.as_object())
                    .map(|obj| self.parse_iceberg_type(obj))
                    .transpose()?
                    .unwrap_or(DataType::Utf8);
                Ok(DataType::List(Arc::new(Field::new("item", element_type, true))))
            },
            "map" => {
                let key_type = type_obj.get("key")
                    .and_then(|v| v.as_object())
                    .map(|obj| self.parse_iceberg_type(obj))
                    .transpose()?
                    .unwrap_or(DataType::Utf8);
                let value_type = type_obj.get("value")
                    .and_then(|v| v.as_object())
                    .map(|obj| self.parse_iceberg_type(obj))
                    .transpose()?
                    .unwrap_or(DataType::Utf8);
                
                let key_field = Field::new("key", key_type, false);
                let value_field = Field::new("value", value_type, true);
                let struct_field = Field::new("entries", DataType::Struct(vec![key_field, value_field].into()), false);
                Ok(DataType::List(Arc::new(struct_field)))
                },
                _ => {
                warn!("Unknown Iceberg type: {}, defaulting to Utf8", type_name);
                Ok(DataType::Utf8)
            }
        }
    }
    
    /// 构建统一表元数据
    fn build_unified_table_metadata(&self, iceberg_metadata: &IcebergTableMetadata) -> Result<TableMetadata> {
        let snapshots = iceberg_metadata.snapshots.iter()
            .map(|s| SnapshotInfo {
                snapshot_id: s.snapshot_id,
                timestamp_ms: s.timestamp_ms,
                parent_snapshot_id: s.parent_snapshot_id,
                operation: s.summary.get("operation").cloned().unwrap_or_default(),
                summary: s.summary.clone(),
            })
            .collect();
        
        let partition_spec = iceberg_metadata.partition_specs.get(&iceberg_metadata.default_spec_id).cloned();
        let sort_order = iceberg_metadata.sort_orders.get(&iceberg_metadata.default_sort_order_id).cloned();
        
        Ok(TableMetadata {
            table_id: iceberg_metadata.table_id.clone(),
            table_name: iceberg_metadata.table_name.clone(),
            namespace: iceberg_metadata.namespace.clone(),
            current_snapshot_id: iceberg_metadata.current_snapshot_id,
            snapshots,
            partition_spec,
            sort_order,
            properties: iceberg_metadata.properties.clone(),
            created_at: iceberg_metadata.created_at,
            updated_at: iceberg_metadata.updated_at,
        })
    }
    
    /// 加载文件清单条目
    fn load_manifest_entries(&self, table_path: &str, snapshot: &IcebergSnapshot) -> Result<Vec<ManifestEntry>> {
        let mut entries = Vec::new();
        
        // 读取manifest list文件
        let manifest_list_path = Path::new(table_path).join(&snapshot.manifest_list);
        if !manifest_list_path.exists() {
            return Err(anyhow::anyhow!("Manifest list not found: {:?}", manifest_list_path));
        }
        
        // 解析manifest list文件
        let manifest_list_entries = self.parse_manifest_list_file(&manifest_list_path)?;
        
        // 遍历每个manifest文件
        for manifest_list_entry in manifest_list_entries {
            let manifest_path = Path::new(table_path).join(&manifest_list_entry.manifest_path);
            if manifest_path.exists() {
                match self.parse_manifest_file(&manifest_path) {
                    Ok(manifest_entries) => {
                        entries.extend(manifest_entries);
                    },
                    Err(e) => {
                        warn!("Failed to parse manifest file {:?}: {}", manifest_path, e);
                    }
                }
            } else {
                warn!("Manifest file not found: {:?}", manifest_path);
            }
        }
        
        Ok(entries)
    }
    
    /// 解析manifest list文件
    fn parse_manifest_list_file(&self, manifest_list_path: &Path) -> Result<Vec<AvroManifestListEntry>> {
        let file = File::open(manifest_list_path)?;
        let reader = Reader::new(file)?;
        let mut entries = Vec::new();
        
        for value in reader {
            let value = value?;
            let entry: AvroManifestListEntry = from_value(&value)?;
            entries.push(entry);
        }
        
        Ok(entries)
    }
    
    /// 解析manifest文件
    fn parse_manifest_file(&self, manifest_path: &Path) -> Result<Vec<ManifestEntry>> {
        let file = File::open(manifest_path)?;
        let reader = Reader::new(file)?;
        let mut entries = Vec::new();
        
        for value in reader {
            let value = value?;
            let avro_entry: AvroManifestEntry = from_value(&value)?;
            
            // 只处理ADDED状态的文件
            if avro_entry.status == 1 { // 1 = ADDED
                let data_file = avro_entry.data_file;
                
                // 转换列大小
                let mut column_sizes = HashMap::new();
                if let Some(sizes) = data_file.column_sizes {
                    for (col, size) in sizes {
                        column_sizes.insert(col, size as u64);
                    }
                }
                
                // 转换值计数
                let mut value_counts = HashMap::new();
                if let Some(counts) = data_file.value_counts {
                    for (col, count) in counts {
                        value_counts.insert(col, count as u64);
                    }
                }
                
                // 转换null值计数
                let mut null_value_counts = HashMap::new();
                if let Some(null_counts) = data_file.null_value_counts {
                    for (col, count) in null_counts {
                        null_value_counts.insert(col, count as u64);
                    }
                }
                
                // 转换下界
                let mut lower_bounds = HashMap::new();
                if let Some(bounds) = data_file.lower_bounds {
                    for (col, bound) in bounds {
                        lower_bounds.insert(col, bound);
                    }
                }
                
                // 转换上界
                let mut upper_bounds = HashMap::new();
                if let Some(bounds) = data_file.upper_bounds {
                    for (col, bound) in bounds {
                        upper_bounds.insert(col, bound);
                    }
                }
                
                // 转换split offsets
                let split_offsets = data_file.split_offsets.unwrap_or_default();
                
                let entry = ManifestEntry {
                    file_path: data_file.file_path,
                    file_format: data_file.file_format,
                    partition_values: data_file.partition,
                    file_size_bytes: data_file.file_size_in_bytes as u64,
                    record_count: data_file.record_count as u64,
                    column_sizes,
                    value_counts,
                    null_value_counts,
                    lower_bounds,
                    upper_bounds,
                    key_metadata: data_file.key_metadata,
                    split_offsets,
                    sort_order_id: data_file.sort_order_id,
                };
                
                entries.push(entry);
            }
        }
        
        Ok(entries)
    }
    
    /// 构建表Schema
    fn build_table_schema(&self, iceberg_metadata: &IcebergTableMetadata) -> Result<Schema> {
        if let Some(schema) = iceberg_metadata.schemas.get(&iceberg_metadata.current_schema_id) {
            Ok(schema.clone())
        } else {
            Err(anyhow::anyhow!("Schema not found for ID: {}", iceberg_metadata.current_schema_id))
        }
    }
    
    /// 读取Parquet文件
    fn read_parquet_file(&self, entry: &ManifestEntry) -> Result<Vec<RecordBatch>> {
        let file_path = Path::new(&entry.file_path);
        if !file_path.exists() {
            return Err(anyhow::anyhow!("Parquet file not found: {:?}", file_path));
        }
        
        let file = File::open(file_path)?;
        let mut arrow_reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .build()?;
        
        let mut batches = Vec::new();
        while let Some(batch) = arrow_reader.next() {
            let batch = batch?;
            batches.push(batch);
        }
        
        Ok(batches)
    }
}

impl FormatAdapter for IcebergAdapter {
    fn open_table(&mut self, table_path: &str) -> Result<()> {
        self.table_path = table_path.to_string();
        self.initialize_iceberg_table(table_path)
    }
    
    fn get_metadata(&self) -> Result<TableMetadata> {
        self.table_metadata.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Table not opened"))
            .map(|m| m.clone())
    }
    
    fn get_statistics(&self) -> Result<TableStatistics> {
        let total_files = self.manifest_entries.len() as u64;
        let total_records = self.manifest_entries.iter()
            .map(|e| e.record_count)
            .sum();
        let total_size = self.manifest_entries.iter()
            .map(|e| e.file_size_bytes)
            .sum();
        let avg_file_size = if total_files > 0 { total_size / total_files } else { 0 };
        
        Ok(TableStatistics {
            total_files,
            total_records,
            total_size_bytes: total_size,
            avg_file_size_bytes: avg_file_size,
            avg_records_per_file: if total_files > 0 { total_records / total_files } else { 0 },
            partition_count: self.manifest_entries.iter()
                .map(|e| e.partition_values.len())
                .max()
                .unwrap_or(0) as u64,
            snapshot_count: self.iceberg_metadata.as_ref()
                .map(|m| m.snapshots.len() as u64)
                .unwrap_or(0),
        })
    }
    
    fn apply_predicate_pushdown(&mut self, predicates: &[UnifiedPredicate]) -> Result<u64> {
        self.predicates = predicates.to_vec();
        
        let mut filtered_count = 0u64;
        for entry in &self.manifest_entries {
            let mut matches = true;
            for predicate in predicates {
                if !self.evaluate_predicate_on_entry(predicate, entry)? {
                    matches = false;
                    break;
                }
            }
            if matches {
                filtered_count += 1;
            }
        }
        
        info!("Predicate pushdown filtered {} files from {} total", 
              filtered_count, self.manifest_entries.len());
        
        Ok(filtered_count)
    }
    
    fn apply_column_projection(&mut self, projection: &ColumnProjection) -> Result<()> {
        self.column_projection = Some(projection.clone());
        info!("Applied column projection: {:?}", projection.columns);
        Ok(())
    }
    
    fn apply_partition_pruning(&mut self, pruning: &PartitionPruningInfo) -> Result<u64> {
        self.partition_pruning = Some(pruning.clone());
        
        let mut pruned_count = 0u64;
        for entry in &self.manifest_entries {
            if self.matches_partition_pruning(entry, pruning)? {
                pruned_count += 1;
            }
        }
        
        info!("Partition pruning kept {} files from {} total", 
              pruned_count, self.manifest_entries.len());
        
        Ok(pruned_count)
    }
    
    fn apply_time_travel(&mut self, time_travel: &TimeTravelConfig) -> Result<()> {
        self.time_travel = Some(time_travel.clone());
        
        if let Some(snapshot_id) = time_travel.snapshot_id {
            if let Some(iceberg_metadata) = &self.iceberg_metadata {
                if let Some(snapshot) = iceberg_metadata.snapshots.iter()
                    .find(|s| s.snapshot_id == snapshot_id) {
                    self.current_snapshot = Some(snapshot.clone());
                    self.current_snapshot_id = Some(snapshot_id);
                    info!("Applied time travel to snapshot: {}", snapshot_id);
                } else {
                    return Err(anyhow::anyhow!("Snapshot not found: {}", snapshot_id));
                }
            }
        }
        
        Ok(())
    }
    
    fn apply_incremental_read(&mut self, incremental: &IncrementalReadConfig) -> Result<()> {
        self.incremental_read = Some(incremental.clone());
        info!("Applied incremental read configuration");
        Ok(())
    }
    
    fn read_data(&mut self) -> Result<Vec<RecordBatch>> {
        let mut all_batches = Vec::new();
        
        for entry in &self.manifest_entries {
            match self.read_parquet_file(entry) {
                Ok(batches) => {
                    all_batches.extend(batches);
                },
                Err(e) => {
                    warn!("Failed to read file {}: {}", entry.file_path, e);
                }
            }
        }
        
        // 应用列投影
        if let Some(projection) = &self.column_projection {
            all_batches = self.apply_column_projection_to_batches(all_batches, projection)?;
        }
        
        info!("Read {} batches from {} files", all_batches.len(), self.manifest_entries.len());
        Ok(all_batches)
    }
    
    fn get_schema(&self) -> Result<Schema> {
        self.table_schema.as_ref()
            .ok_or_else(|| anyhow::anyhow!("Table not opened"))
            .map(|s| s.clone())
    }
}

// 辅助方法实现
impl IcebergAdapter {
    /// 评估谓词
    fn evaluate_predicate_on_entry(&self, predicate: &UnifiedPredicate, entry: &ManifestEntry) -> Result<bool> {
        match predicate {
            UnifiedPredicate::Equal { column, value } => {
                self.evaluate_equality_predicate(column, value, entry)
            },
            UnifiedPredicate::GreaterThan { column, value } => {
                self.evaluate_range_predicate(column, value, ">", entry)
            },
            UnifiedPredicate::LessThan { column, value } => {
                self.evaluate_range_predicate(column, value, "<", entry)
            },
            UnifiedPredicate::Between { column, min, max } => {
                self.evaluate_between_predicate(column, min, max, entry)
            },
            UnifiedPredicate::In { column, values } => {
                self.evaluate_in_predicate(column, values, entry)
            },
            UnifiedPredicate::IsNull { column } => {
                self.evaluate_null_predicate(column, true, entry)
            },
            UnifiedPredicate::IsNotNull { column } => {
                self.evaluate_null_predicate(column, false, entry)
            },
            UnifiedPredicate::Like { column, pattern } => {
                self.evaluate_like_predicate(column, pattern, entry)
            },
        }
    }
    
    /// 评估等值谓词
    fn evaluate_equality_predicate(&self, column: &str, value: &str, entry: &ManifestEntry) -> Result<bool> {
        // 检查分区值
        if let Some(partition_value) = entry.partition_values.get(column) {
            return Ok(partition_value == value);
        }
        
        // 检查边界值
        if let (Some(lower), Some(upper)) = (entry.lower_bounds.get(column), entry.upper_bounds.get(column)) {
            let value_bytes = value.as_bytes();
            return Ok(value_bytes >= lower.as_slice() && value_bytes <= upper.as_slice());
        }
        
        Ok(true) // 如果无法确定，返回true
    }
    
    /// 评估范围谓词
    fn evaluate_range_predicate(&self, column: &str, value: &str, op: &str, entry: &ManifestEntry) -> Result<bool> {
        // 检查分区值
        if let Some(partition_value) = entry.partition_values.get(column) {
            return Ok(self.compare_partition_values(partition_value, value, op)?);
        }
        
        // 检查边界值
        if let (Some(lower), Some(upper)) = (entry.lower_bounds.get(column), entry.upper_bounds.get(column)) {
            let value_bytes = value.as_bytes();
            return Ok(match op {
                ">" => value_bytes > lower.as_slice(),
                "<" => value_bytes < upper.as_slice(),
                ">=" => value_bytes >= lower.as_slice(),
                "<=" => value_bytes <= upper.as_slice(),
                _ => true,
            });
        }
        
        Ok(true)
    }
    
    /// 评估Between谓词
    fn evaluate_between_predicate(&self, column: &str, min: &str, max: &str, entry: &ManifestEntry) -> Result<bool> {
        // 检查分区值
        if let Some(partition_value) = entry.partition_values.get(column) {
            return Ok(partition_value.as_str() >= min && partition_value.as_str() <= max);
        }
        
        // 检查边界值
        if let (Some(lower), Some(upper)) = (entry.lower_bounds.get(column), entry.upper_bounds.get(column)) {
            let min_bytes = min.as_bytes();
            let max_bytes = max.as_bytes();
            return Ok(min_bytes <= upper.as_slice() && max_bytes >= lower.as_slice());
        }
        
        Ok(true)
    }
    
    /// 评估In谓词
    fn evaluate_in_predicate(&self, column: &str, values: &[String], entry: &ManifestEntry) -> Result<bool> {
        // 检查分区值
        if let Some(partition_value) = entry.partition_values.get(column) {
            return Ok(values.contains(partition_value));
        }
        
        // 对于边界值，如果任何值在范围内就返回true
        if let (Some(lower), Some(upper)) = (entry.lower_bounds.get(column), entry.upper_bounds.get(column)) {
            for value in values {
                let value_bytes = value.as_bytes();
                if value_bytes >= lower.as_slice() && value_bytes <= upper.as_slice() {
                    return Ok(true);
                }
            }
        }
        
        Ok(true)
    }
    
    /// 评估Null谓词
    fn evaluate_null_predicate(&self, column: &str, is_null: bool, entry: &ManifestEntry) -> Result<bool> {
        if let Some(null_count) = entry.null_value_counts.get(column) {
            if is_null {
                return Ok(*null_count > 0);
            } else {
                return Ok(*null_count < entry.record_count);
            }
        }
        
        Ok(true)
    }
    
    /// 评估Like谓词
    fn evaluate_like_predicate(&self, column: &str, pattern: &str, entry: &ManifestEntry) -> Result<bool> {
        // 检查分区值
        if let Some(partition_value) = entry.partition_values.get(column) {
            return Ok(self.matches_like_pattern(partition_value, pattern));
        }
        
        // 对于边界值，如果模式可能匹配就返回true
        Ok(true)
    }
    
    /// 比较分区值
    fn compare_partition_values(&self, partition_value: &str, value: &str, op: &str) -> Result<bool> {
        Ok(match op {
            ">" => partition_value > value,
            "<" => partition_value < value,
            ">=" => partition_value >= value,
            "<=" => partition_value <= value,
            _ => false,
        })
    }
    
    /// 匹配Like模式
    fn matches_like_pattern(&self, text: &str, pattern: &str) -> bool {
        let mut text_chars = text.chars().peekable();
        let mut pattern_chars = pattern.chars().peekable();
        
        while let (Some(text_char), Some(pattern_char)) = (text_chars.peek(), pattern_chars.peek()) {
            match pattern_char {
                '%' => {
                    pattern_chars.next();
                    if pattern_chars.peek().is_none() {
                        return true; // % 在末尾，匹配剩余所有字符
                    }
                    // 递归匹配剩余模式
                    let remaining_pattern: String = pattern_chars.collect();
                    while text_chars.peek().is_some() {
                        let remaining_text: String = text_chars.clone().collect();
                        if self.matches_like_pattern(&remaining_text, &remaining_pattern) {
                            return true;
                        }
                        text_chars.next();
                    }
                    return false;
                },
                '_' => {
                    text_chars.next();
                    pattern_chars.next();
                },
                _ => {
                    if text_char == pattern_char {
                        text_chars.next();
                        pattern_chars.next();
                    } else {
                        return false;
                    }
                }
            }
        }
        
        text_chars.peek().is_none() && pattern_chars.peek().is_none()
    }
    
    /// 匹配分区剪枝
    fn matches_partition_pruning(&self, entry: &ManifestEntry, pruning: &PartitionPruningInfo) -> Result<bool> {
        for (column, value) in &pruning.partition_values {
            if let Some(partition_value) = entry.partition_values.get(column) {
                if partition_value != value {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }
    
    /// 应用列投影到批次
    fn apply_column_projection_to_batches(&self, batches: Vec<RecordBatch>, projection: &ColumnProjection) -> Result<Vec<RecordBatch>> {
        let mut projected_batches = Vec::new();
        
        for batch in batches {
            let projected_batch = self.apply_column_projection_to_batch(&batch, projection)?;
            projected_batches.push(projected_batch);
        }
        
        Ok(projected_batches)
    }
    
    /// 应用列投影到单个批次
    fn apply_column_projection_to_batch(&self, batch: &RecordBatch, projection: &ColumnProjection) -> Result<RecordBatch> {
        let mut projected_columns = Vec::new();
        let mut projected_fields = Vec::new();
        
        for column_name in &projection.columns {
            if let Some(column_index) = self.find_column_index(&batch.schema(), column_name) {
                projected_columns.push(batch.column(column_index).clone());
                projected_fields.push(batch.schema().field(column_index).clone());
            }
        }
        
        let projected_schema = Arc::new(Schema::new(projected_fields));
        RecordBatch::try_new(projected_schema, projected_columns)
            .map_err(|e| anyhow::anyhow!("Failed to create projected batch: {}", e))
    }
    
    /// 查找列索引
    fn find_column_index(&self, schema: &Schema, column_name: &str) -> Option<usize> {
        schema.fields().iter().position(|field| field.name() == column_name)
    }
}
