use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use ahash::AHashMap;
use crate::error::{CompareError, Result};

/// Result of matching rows between two dataframes
#[derive(Debug)]
pub struct RowMatchResult {
    /// Pairs of (df1_index, df2_index) for matching rows
    pub common_rows: Vec<(usize, usize)>,
    /// Indices of rows only in df1
    pub df1_unique_indices: Vec<usize>,
    /// Indices of rows only in df2
    pub df2_unique_indices: Vec<usize>,
    /// Whether there were duplicate join keys
    pub has_duplicates: bool,
}

/// Handles matching rows between dataframes based on join columns
pub struct RowMatcher {
    join_columns: Vec<String>,
}

impl RowMatcher {
    pub fn new(join_columns: Vec<String>) -> Self {
        Self { join_columns }
    }
    
    /// Match rows between two dataframes based on join columns
    pub fn match_rows(
        &self,
        df1: &RecordBatch,
        df2: &RecordBatch,
    ) -> Result<RowMatchResult> {
        if self.join_columns.is_empty() {
            return Err(CompareError::EmptyJoinColumns);
        }
        
        // Verify join columns exist in both dataframes
        for col in &self.join_columns {
            if df1.schema().column_with_name(col).is_none() {
                return Err(CompareError::JoinColumnNotFound(col.clone(), "df1".to_string()));
            }
            if df2.schema().column_with_name(col).is_none() {
                return Err(CompareError::JoinColumnNotFound(col.clone(), "df2".to_string()));
            }
        }
        
        // Build hash map for df2 (key -> list of row indices)
        let df2_map = self.build_index_map(df2)?;
        
        // Check for duplicates
        let has_duplicates = df2_map.values().any(|indices| indices.len() > 1);
        
        let mut common_rows = Vec::new();
        let mut df1_unique_indices = Vec::new();
        let mut df2_matched = vec![false; df2.num_rows()];
        
        // For each row in df1, look for matches in df2
        for i in 0..df1.num_rows() {
            let key = self.extract_key(df1, i)?;
            
            if let Some(df2_indices) = df2_map.get(&key) {
                // Find the first unmatched row in df2 with this key
                for &j in df2_indices {
                    if !df2_matched[j] {
                        common_rows.push((i, j));
                        df2_matched[j] = true;
                        break;
                    }
                }
                
                // If all df2 rows with this key were already matched, this df1 row is unique
                if common_rows.is_empty() || common_rows.last().unwrap().0 != i {
                    df1_unique_indices.push(i);
                }
            } else {
                df1_unique_indices.push(i);
            }
        }
        
        // Collect unmatched df2 rows
        let df2_unique_indices: Vec<usize> = (0..df2.num_rows())
            .filter(|&i| !df2_matched[i])
            .collect();
        
        Ok(RowMatchResult {
            common_rows,
            df1_unique_indices,
            df2_unique_indices,
            has_duplicates,
        })
    }
    
    /// Build a hash map from join key to list of row indices
    fn build_index_map(&self, df: &RecordBatch) -> Result<AHashMap<Vec<String>, Vec<usize>>> {
        let mut map: AHashMap<Vec<String>, Vec<usize>> = AHashMap::new();
        
        for i in 0..df.num_rows() {
            let key = self.extract_key(df, i)?;
            map.entry(key).or_insert_with(Vec::new).push(i);
        }
        
        Ok(map)
    }
    
    /// Extract join key values for a given row
    fn extract_key(&self, df: &RecordBatch, row_idx: usize) -> Result<Vec<String>> {
        let mut key = Vec::with_capacity(self.join_columns.len());
        
        for col_name in &self.join_columns {
            let column = df
                .column_by_name(col_name)
                .ok_or_else(|| CompareError::JoinColumnNotFound(col_name.clone(), "unknown".to_string()))?;
            
            let value_str = self.array_value_to_string(column, row_idx)?;
            key.push(value_str);
        }
        
        Ok(key)
    }
    
    /// Convert an array value at a given index to a string representation
    fn array_value_to_string(&self, array: &ArrayRef, idx: usize) -> Result<String> {
        if array.is_null(idx) {
            return Ok("DATACOMPY_NULL".to_string());
        }
        
        let value = match array.data_type() {
            DataType::Int8 => array.as_primitive::<Int8Type>().value(idx).to_string(),
            DataType::Int16 => array.as_primitive::<Int16Type>().value(idx).to_string(),
            DataType::Int32 => array.as_primitive::<Int32Type>().value(idx).to_string(),
            DataType::Int64 => array.as_primitive::<Int64Type>().value(idx).to_string(),
            DataType::UInt8 => array.as_primitive::<UInt8Type>().value(idx).to_string(),
            DataType::UInt16 => array.as_primitive::<UInt16Type>().value(idx).to_string(),
            DataType::UInt32 => array.as_primitive::<UInt32Type>().value(idx).to_string(),
            DataType::UInt64 => array.as_primitive::<UInt64Type>().value(idx).to_string(),
            DataType::Float32 => array.as_primitive::<Float32Type>().value(idx).to_string(),
            DataType::Float64 => array.as_primitive::<Float64Type>().value(idx).to_string(),
            DataType::Utf8 => array.as_string::<i32>().value(idx).to_string(),
            DataType::LargeUtf8 => array.as_string::<i64>().value(idx).to_string(),
            DataType::Boolean => array.as_boolean().value(idx).to_string(),
            _ => format!("{:?}", array.slice(idx, 1)),
        };
        
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array};
    use std::sync::Arc;
    
    #[test]
    fn test_simple_match() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", DataType::Int32, false),
            arrow::datatypes::Field::new("value", DataType::Int32, false),
        ]));
        
        let df1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        ).unwrap();
        
        let df2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![2, 3, 4])),
                Arc::new(Int32Array::from(vec![20, 30, 40])),
            ],
        ).unwrap();
        
        let matcher = RowMatcher::new(vec!["id".to_string()]);
        let result = matcher.match_rows(&df1, &df2).unwrap();
        
        assert_eq!(result.common_rows.len(), 2);
        assert_eq!(result.df1_unique_indices, vec![0]);
        assert_eq!(result.df2_unique_indices, vec![2]);
    }
}
