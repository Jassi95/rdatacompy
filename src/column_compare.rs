use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::*;
use crate::error::Result;
use crate::tolerance::ToleranceChecker;
use crate::types::{arrow_type_to_string, is_numeric_type};

/// Sample of a difference between two values
#[derive(Debug, Clone)]
pub struct SampleDiff {
    pub row_index: usize,
    pub value1: String,
    pub value2: String,
}

/// Result of comparing a single column
#[derive(Debug)]
pub struct ColumnComparisonResult {
    pub column_name: String,
    pub all_equal: bool,
    pub num_unequal: usize,
    pub num_null_diff: usize,
    pub max_diff: Option<f64>,
    pub type1: String,
    pub type2: String,
    pub sample_diffs: Vec<SampleDiff>,
}

/// Handles column-by-column comparison
pub struct ColumnComparator {
    tolerance: ToleranceChecker,
    max_samples: usize,
}

impl ColumnComparator {
    pub fn new(abs_tol: f64, rel_tol: f64, max_samples: usize) -> Self {
        Self {
            tolerance: ToleranceChecker::new(abs_tol, rel_tol),
            max_samples,
        }
    }
    
    /// Compare two columns for the given matched row indices
    pub fn compare_column(
        &self,
        column_name: &str,
        col1: &ArrayRef,
        col2: &ArrayRef,
        matched_indices: &[(usize, usize)],
    ) -> Result<ColumnComparisonResult> {
        let type1 = arrow_type_to_string(col1.data_type());
        let type2 = arrow_type_to_string(col2.data_type());
        
        let mut num_unequal = 0;
        let mut num_null_diff = 0;
        let mut max_diff: Option<f64> = None;
        let mut sample_diffs = Vec::new();
        
        let is_numeric = is_numeric_type(col1.data_type()) && is_numeric_type(col2.data_type());
        
        for &(idx1, idx2) in matched_indices {
            let null1 = col1.is_null(idx1);
            let null2 = col2.is_null(idx2);
            
            // Check for null differences
            if null1 != null2 {
                num_null_diff += 1;
                num_unequal += 1;
                
                if sample_diffs.len() < self.max_samples {
                    sample_diffs.push(SampleDiff {
                        row_index: idx1,
                        value1: if null1 { "NULL".to_string() } else { self.value_to_string(col1, idx1) },
                        value2: if null2 { "NULL".to_string() } else { self.value_to_string(col2, idx2) },
                    });
                }
                continue;
            }
            
            // Both null - consider equal
            if null1 && null2 {
                continue;
            }
            
            // Compare non-null values
            let equal = if is_numeric {
                self.compare_numeric_values(col1, idx1, col2, idx2, &mut max_diff)?
            } else {
                self.compare_non_numeric_values(col1, idx1, col2, idx2)?
            };
            
            if !equal {
                num_unequal += 1;
                
                if sample_diffs.len() < self.max_samples {
                    sample_diffs.push(SampleDiff {
                        row_index: idx1,
                        value1: self.value_to_string(col1, idx1),
                        value2: self.value_to_string(col2, idx2),
                    });
                }
            }
        }
        
        Ok(ColumnComparisonResult {
            column_name: column_name.to_string(),
            all_equal: num_unequal == 0,
            num_unequal,
            num_null_diff,
            max_diff,
            type1,
            type2,
            sample_diffs,
        })
    }
    
    /// Compare numeric values with tolerance
    fn compare_numeric_values(
        &self,
        col1: &ArrayRef,
        idx1: usize,
        col2: &ArrayRef,
        idx2: usize,
        max_diff: &mut Option<f64>,
    ) -> Result<bool> {
        let val1 = self.extract_numeric_value(col1, idx1)?;
        let val2 = self.extract_numeric_value(col2, idx2)?;
        
        let diff = self.tolerance.difference(val1, val2);
        
        // Update max difference
        *max_diff = Some(max_diff.map_or(diff, |current| current.max(diff)));
        
        Ok(self.tolerance.within_tolerance(val1, val2))
    }
    
    /// Compare non-numeric values (exact match)
    fn compare_non_numeric_values(
        &self,
        col1: &ArrayRef,
        idx1: usize,
        col2: &ArrayRef,
        idx2: usize,
    ) -> Result<bool> {
        let val1 = self.value_to_string(col1, idx1);
        let val2 = self.value_to_string(col2, idx2);
        Ok(val1 == val2)
    }
    
    /// Extract a numeric value from an array
    fn extract_numeric_value(&self, array: &ArrayRef, idx: usize) -> Result<f64> {
        let value = match array.data_type() {
            DataType::Int8 => array.as_primitive::<Int8Type>().value(idx) as f64,
            DataType::Int16 => array.as_primitive::<Int16Type>().value(idx) as f64,
            DataType::Int32 => array.as_primitive::<Int32Type>().value(idx) as f64,
            DataType::Int64 => array.as_primitive::<Int64Type>().value(idx) as f64,
            DataType::UInt8 => array.as_primitive::<UInt8Type>().value(idx) as f64,
            DataType::UInt16 => array.as_primitive::<UInt16Type>().value(idx) as f64,
            DataType::UInt32 => array.as_primitive::<UInt32Type>().value(idx) as f64,
            DataType::UInt64 => array.as_primitive::<UInt64Type>().value(idx) as f64,
            DataType::Float32 => array.as_primitive::<Float32Type>().value(idx) as f64,
            DataType::Float64 => array.as_primitive::<Float64Type>().value(idx),
            DataType::Decimal128(_, scale) => {
                let value = array.as_primitive::<Decimal128Type>().value(idx);
                // Convert decimal to f64: value / 10^scale
                value as f64 / 10_f64.powi(*scale as i32)
            },
            DataType::Decimal256(_, scale) => {
                let value = array.as_primitive::<Decimal256Type>().value(idx);
                // Convert i256 to f64 by converting to string then parsing
                // This is safer than direct conversion for very large decimals
                let decimal_str = value.to_string();
                let scale_divisor = 10_f64.powi(*scale as i32);
                decimal_str.parse::<f64>().unwrap_or(0.0) / scale_divisor
            },
            _ => 0.0,
        };
        Ok(value)
    }
    
    /// Convert an array value to string for display
    fn value_to_string(&self, array: &ArrayRef, idx: usize) -> String {
        if array.is_null(idx) {
            return "NULL".to_string();
        }
        
        match array.data_type() {
            DataType::Int8 => array.as_primitive::<Int8Type>().value(idx).to_string(),
            DataType::Int16 => array.as_primitive::<Int16Type>().value(idx).to_string(),
            DataType::Int32 => array.as_primitive::<Int32Type>().value(idx).to_string(),
            DataType::Int64 => array.as_primitive::<Int64Type>().value(idx).to_string(),
            DataType::UInt8 => array.as_primitive::<UInt8Type>().value(idx).to_string(),
            DataType::UInt16 => array.as_primitive::<UInt16Type>().value(idx).to_string(),
            DataType::UInt32 => array.as_primitive::<UInt32Type>().value(idx).to_string(),
            DataType::UInt64 => array.as_primitive::<UInt64Type>().value(idx).to_string(),
            DataType::Float32 => format!("{:.6}", array.as_primitive::<Float32Type>().value(idx)),
            DataType::Float64 => format!("{:.6}", array.as_primitive::<Float64Type>().value(idx)),
            DataType::Decimal128(_, scale) => {
                let value = array.as_primitive::<Decimal128Type>().value(idx);
                let decimal_value = value as f64 / 10_f64.powi(*scale as i32);
                format!("{:.prec$}", decimal_value, prec = *scale as usize)
            },
            DataType::Decimal256(_, scale) => {
                let value = array.as_primitive::<Decimal256Type>().value(idx);
                let decimal_str = value.to_string();
                let decimal_value = decimal_str.parse::<f64>().unwrap_or(0.0) / 10_f64.powi(*scale as i32);
                format!("{:.prec$}", decimal_value, prec = *scale as usize)
            },
            DataType::Utf8 => array.as_string::<i32>().value(idx).to_string(),
            DataType::LargeUtf8 => array.as_string::<i64>().value(idx).to_string(),
            DataType::Boolean => array.as_boolean().value(idx).to_string(),
            _ => format!("{:?}", array.slice(idx, 1)),
        }
    }
}
