use crate::columnar::column::Column;
use crate::columnar::types::DataType;
use anyhow::Result;
use std::fmt;
use smallvec::SmallVec;

/// A batch of columnar data optimized for vectorized execution
/// Typically contains 4k-16k rows for optimal cache performance
#[derive(Debug, Clone)]
pub struct Batch {
    pub columns: SmallVec<[Column; 8]>,
    pub len: usize,
    pub schema: BatchSchema,
}

/// Schema information for a batch
#[derive(Debug, Clone)]
pub struct BatchSchema {
    pub fields: SmallVec<[Field; 8]>,
}

/// A field in the batch schema
#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

impl Batch {
    /// Create a new empty batch with the specified schema
    pub fn new(schema: BatchSchema) -> Self {
        Self {
            columns: SmallVec::new(),
            len: 0,
            schema,
        }
    }

    /// Create a batch from columns
    pub fn from_columns(columns: Vec<Column>, schema: BatchSchema) -> Result<Self> {
        if columns.is_empty() {
            return Ok(Self {
                columns: SmallVec::new(),
                len: 0,
                schema,
            });
        }

        let len = columns[0].len();
        for (i, col) in columns.iter().enumerate() {
            if col.len() != len {
                return Err(anyhow::anyhow!(
                    "Column {} has length {} but expected {}",
                    i, col.len(), len
                ));
            }
        }

        Ok(Self {
            columns: SmallVec::from_vec(columns),
            len,
            schema,
        })
    }

    /// Get the number of rows in the batch
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if the batch is empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get the number of columns in the batch
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Get a column by index
    pub fn column(&self, index: usize) -> Option<&Column> {
        self.columns.get(index)
    }

    /// Get a column by name
    pub fn column_by_name(&self, name: &str) -> Option<&Column> {
        self.schema.fields.iter()
            .position(|field| field.name == name)
            .and_then(|index| self.columns.get(index))
    }

    /// Get a mutable reference to a column by index
    pub fn column_mut(&mut self, index: usize) -> Option<&mut Column> {
        self.columns.get_mut(index)
    }

    /// Add a column to the batch
    pub fn add_column(&mut self, column: Column) -> Result<()> {
        if !self.columns.is_empty() && column.len() != self.len {
            return Err(anyhow::anyhow!(
                "Column length {} doesn't match batch length {}",
                column.len(), self.len
            ));
        }
        self.len = column.len();
        self.columns.push(column);
        Ok(())
    }

    /// Slice the batch to get a subset of rows
    pub fn slice(&self, offset: usize, length: usize) -> Result<Self> {
        if offset + length > self.len {
            return Err(anyhow::anyhow!("Slice out of bounds"));
        }

        let mut sliced_columns = SmallVec::new();
        for column in &self.columns {
            sliced_columns.push(column.slice(offset, length)?);
        }

        Ok(Self {
            columns: sliced_columns,
            len: length,
            schema: self.schema.clone(),
        })
    }

    /// Get the memory usage of the batch in bytes
    pub fn memory_usage(&self) -> usize {
        self.columns.iter().map(|col| col.memory_usage()).sum()
    }

    /// Get statistics about the batch
    pub fn stats(&self) -> BatchStats {
        let total_memory = self.memory_usage();
        let null_count = self.columns.iter().map(|col| col.null_count()).sum();
        let total_values = self.len * self.num_columns();

        BatchStats {
            num_rows: self.len,
            num_columns: self.num_columns(),
            memory_usage: total_memory,
            null_count,
            null_ratio: if total_values > 0 { null_count as f64 / total_values as f64 } else { 0.0 },
        }
    }
}

impl BatchSchema {
    /// Create a new schema
    pub fn new() -> Self {
        Self {
            fields: SmallVec::new(),
        }
    }

    /// Add a field to the schema
    pub fn add_field(&mut self, name: String, data_type: DataType, nullable: bool) {
        self.fields.push(Field {
            name,
            data_type,
            nullable,
        });
    }

    /// Get the number of fields
    pub fn num_fields(&self) -> usize {
        self.fields.len()
    }

    /// Get a field by index
    pub fn field(&self, index: usize) -> Option<&Field> {
        self.fields.get(index)
    }

    /// Get a field by name
    pub fn field_by_name(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|field| field.name == name)
    }

    /// Find the index of a field by name
    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|field| field.name == name)
    }
}

impl Default for BatchSchema {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about a batch
#[derive(Debug, Clone)]
pub struct BatchStats {
    pub num_rows: usize,
    pub num_columns: usize,
    pub memory_usage: usize,
    pub null_count: usize,
    pub null_ratio: f64,
}

impl fmt::Display for Batch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Batch(rows={}, cols={}, memory={}B)", 
               self.len, self.num_columns(), self.memory_usage())
    }
}

impl fmt::Display for BatchSchema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Schema(")?;
        for (i, field) in self.fields.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}: {}", field.name, field.data_type)?;
            if field.nullable {
                write!(f, "?")?;
            }
        }
        write!(f, ")")
    }
}
