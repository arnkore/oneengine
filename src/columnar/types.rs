use serde::{Deserialize, Serialize};
use std::fmt;

/// Supported data types for columnar execution
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    Binary,
    Date32,
    Date64,
    Timestamp,
    Decimal(u8, u8), // precision, scale
}

impl DataType {
    /// Get the size in bytes for fixed-width types
    pub fn size(&self) -> Option<usize> {
        match self {
            DataType::Boolean => Some(1),
            DataType::Int8 | DataType::UInt8 => Some(1),
            DataType::Int16 | DataType::UInt16 => Some(2),
            DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date32 => Some(4),
            DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::Date64 | DataType::Timestamp => Some(8),
            DataType::Utf8 | DataType::Binary | DataType::Decimal(_, _) => None, // Variable width
        }
    }

    /// Check if this is a numeric type
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
            DataType::Float32 | DataType::Float64 | DataType::Decimal(_, _)
        )
    }

    /// Check if this is an integer type
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
        )
    }

    /// Check if this is a floating point type
    pub fn is_float(&self) -> bool {
        matches!(self, DataType::Float32 | DataType::Float64)
    }

    /// Check if this is a string type
    pub fn is_string(&self) -> bool {
        matches!(self, DataType::Utf8)
    }

    /// Check if this is a date/time type
    pub fn is_temporal(&self) -> bool {
        matches!(self, DataType::Date32 | DataType::Date64 | DataType::Timestamp)
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Boolean => write!(f, "Boolean"),
            DataType::Int8 => write!(f, "Int8"),
            DataType::Int16 => write!(f, "Int16"),
            DataType::Int32 => write!(f, "Int32"),
            DataType::Int64 => write!(f, "Int64"),
            DataType::UInt8 => write!(f, "UInt8"),
            DataType::UInt16 => write!(f, "UInt16"),
            DataType::UInt32 => write!(f, "UInt32"),
            DataType::UInt64 => write!(f, "UInt64"),
            DataType::Float32 => write!(f, "Float32"),
            DataType::Float64 => write!(f, "Float64"),
            DataType::Utf8 => write!(f, "Utf8"),
            DataType::Binary => write!(f, "Binary"),
            DataType::Date32 => write!(f, "Date32"),
            DataType::Date64 => write!(f, "Date64"),
            DataType::Timestamp => write!(f, "Timestamp"),
            DataType::Decimal(precision, scale) => write!(f, "Decimal({}, {})", precision, scale),
        }
    }
}

/// Null bitmap for tracking null values
#[derive(Debug, Clone)]
pub struct Bitmap {
    bits: Vec<u64>,
    len: usize,
}

impl Bitmap {
    /// Create a new bitmap with the specified length
    pub fn new(len: usize) -> Self {
        let chunks = (len + 63) / 64;
        Self {
            bits: vec![0; chunks],
            len,
        }
    }

    /// Create a bitmap from existing bits
    pub fn from_bits(bits: Vec<u64>, len: usize) -> Self {
        Self { bits, len }
    }

    /// Get the length of the bitmap
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if the bitmap is empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get the value at the specified index
    pub fn get(&self, index: usize) -> bool {
        if index >= self.len {
            return false;
        }
        let chunk = index / 64;
        let bit = index % 64;
        (self.bits[chunk] >> bit) & 1 != 0
    }

    /// Set the value at the specified index
    pub fn set(&mut self, index: usize, value: bool) {
        if index >= self.len {
            return;
        }
        let chunk = index / 64;
        let bit = index % 64;
        if value {
            self.bits[chunk] |= 1 << bit;
        } else {
            self.bits[chunk] &= !(1 << bit);
        }
    }

    /// Count the number of set bits
    pub fn count_ones(&self) -> usize {
        self.bits.iter().map(|chunk| chunk.count_ones() as usize).sum()
    }

    /// Count the number of unset bits
    pub fn count_zeros(&self) -> usize {
        self.len - self.count_ones()
    }

    /// Get the underlying bits
    pub fn as_slice(&self) -> &[u64] {
        &self.bits
    }
}

/// Encoding types for column data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Encoding {
    Plain,
    Dictionary,
    RunLength,
    Delta,
    BitPacked,
}

impl fmt::Display for Encoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Encoding::Plain => write!(f, "Plain"),
            Encoding::Dictionary => write!(f, "Dictionary"),
            Encoding::RunLength => write!(f, "RunLength"),
            Encoding::Delta => write!(f, "Delta"),
            Encoding::BitPacked => write!(f, "BitPacked"),
        }
    }
}
