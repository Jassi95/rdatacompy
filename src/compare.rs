use arrow::array::RecordBatch;
use std::collections::HashSet;

use crate::column_compare::{ColumnComparator, ColumnComparisonResult};
use crate::error::{CompareError, Result};
use crate::report::ReportGenerator;
use crate::row_matcher::{RowMatcher, RowMatchResult};
use crate::types::types_compatible;

/// Main comparison engine for dataframes
pub struct DataFrameCompare {
    df1: RecordBatch,
    df2: RecordBatch,
    join_columns: Vec<String>,
    abs_tol: f64,
    rel_tol: f64,
    df1_name: String,
    df2_name: String,
    
    // Results (computed lazily)
    match_result: Option<RowMatchResult>,
    column_results: Option<Vec<ColumnComparisonResult>>,
    common_columns: Option<HashSet<String>>,
    df1_unique_columns: Option<HashSet<String>>,
    df2_unique_columns: Option<HashSet<String>>,
}

impl DataFrameCompare {
    pub fn new(
        df1: RecordBatch,
        df2: RecordBatch,
        join_columns: Vec<String>,
        abs_tol: f64,
        rel_tol: f64,
        df1_name: String,
        df2_name: String,
    ) -> Result<Self> {
        // Validate inputs
        if join_columns.is_empty() {
            return Err(CompareError::EmptyJoinColumns);
        }
        
        if df1.num_columns() == 0 {
            return Err(CompareError::EmptyDataFrame);
        }
        
        if df2.num_columns() == 0 {
            return Err(CompareError::EmptyDataFrame);
        }
        
        Ok(Self {
            df1,
            df2,
            join_columns,
            abs_tol,
            rel_tol,
            df1_name,
            df2_name,
            match_result: None,
            column_results: None,
            common_columns: None,
            df1_unique_columns: None,
            df2_unique_columns: None,
        })
    }
    
    /// Run the comparison
    pub fn compare(&mut self) -> Result<()> {
        // Match rows
        let matcher = RowMatcher::new(self.join_columns.clone());
        self.match_result = Some(matcher.match_rows(&self.df1, &self.df2)?);
        
        // Identify common and unique columns
        self.compute_column_sets();
        
        // Compare common columns
        self.compare_columns()?;
        
        Ok(())
    }
    
    /// Compute sets of common and unique columns
    fn compute_column_sets(&mut self) {
        let df1_cols: HashSet<String> = self.df1.schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        
        let df2_cols: HashSet<String> = self.df2.schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        
        let common: HashSet<String> = df1_cols.intersection(&df2_cols)
            .cloned()
            .collect();
        
        let df1_unique: HashSet<String> = df1_cols.difference(&df2_cols)
            .cloned()
            .collect();
        
        let df2_unique: HashSet<String> = df2_cols.difference(&df1_cols)
            .cloned()
            .collect();
        
        self.common_columns = Some(common);
        self.df1_unique_columns = Some(df1_unique);
        self.df2_unique_columns = Some(df2_unique);
    }
    
    /// Compare all common columns
    fn compare_columns(&mut self) -> Result<()> {
        let common_cols = self.common_columns.as_ref().unwrap();
        let match_result = self.match_result.as_ref().unwrap();
        
        let comparator = ColumnComparator::new(self.abs_tol, self.rel_tol, 100);
        let mut results = Vec::new();
        
        for col_name in common_cols {
            // Skip join columns in comparison (they're equal by definition)
            if self.join_columns.contains(col_name) {
                continue;
            }
            
            let col1 = self.df1.column_by_name(col_name).unwrap();
            let col2 = self.df2.column_by_name(col_name).unwrap();
            
            // Check type compatibility
            if !types_compatible(col1.data_type(), col2.data_type()) {
                return Err(CompareError::IncompatibleTypes(
                    col_name.clone(),
                    format!("{:?}", col1.data_type()),
                    format!("{:?}", col2.data_type()),
                ));
            }
            
            let result = comparator.compare_column(
                col_name,
                col1,
                col2,
                &match_result.common_rows,
            )?;
            
            results.push(result);
        }
        
        self.column_results = Some(results);
        Ok(())
    }
    
    /// Generate a comprehensive report
    pub fn report(&self) -> Result<String> {
        if self.match_result.is_none() {
            return Err(CompareError::PythonError(
                "Comparison not run yet. This should not happen.".to_string()
            ));
        }
        
        let match_result = self.match_result.as_ref().unwrap();
        let column_results = self.column_results.as_ref().unwrap();
        let common_cols = self.common_columns.as_ref().unwrap();
        let df1_unq_cols = self.df1_unique_columns.as_ref().unwrap();
        let df2_unq_cols = self.df2_unique_columns.as_ref().unwrap();
        
        let generator = ReportGenerator::new(
            &self.df1_name,
            &self.df2_name,
            self.df1.num_rows(),
            self.df2.num_rows(),
            self.df1.num_columns(),
            self.df2.num_columns(),
            common_cols,
            df1_unq_cols,
            df2_unq_cols,
            &self.join_columns,
            self.abs_tol,
            self.rel_tol,
            match_result.common_rows.len(),
            match_result.df1_unique_indices.len(),
            match_result.df2_unique_indices.len(),
            match_result.has_duplicates,
            column_results,
            &self.df1, // Pass df1 for join key lookup
        );
        
        Ok(generator.generate())
    }
    
    /// Check if dataframes match exactly
    pub fn matches(&self) -> bool {
        if let (Some(match_result), Some(column_results)) = 
            (&self.match_result, &self.column_results) {
            
            // All rows must be matched
            if !match_result.df1_unique_indices.is_empty() || 
               !match_result.df2_unique_indices.is_empty() {
                return false;
            }
            
            // All columns must be equal
            column_results.iter().all(|r| r.all_equal)
        } else {
            false
        }
    }
    
    /// Get common column names
    pub fn intersect_columns(&self) -> Vec<String> {
        self.common_columns.as_ref()
            .map(|s| {
                let mut cols: Vec<_> = s.iter().cloned().collect();
                cols.sort();
                cols
            })
            .unwrap_or_default()
    }
    
    /// Get df1 unique column names
    pub fn df1_unq_columns(&self) -> Vec<String> {
        self.df1_unique_columns.as_ref()
            .map(|s| {
                let mut cols: Vec<_> = s.iter().cloned().collect();
                cols.sort();
                cols
            })
            .unwrap_or_default()
    }
    
    /// Get df2 unique column names
    pub fn df2_unq_columns(&self) -> Vec<String> {
        self.df2_unique_columns.as_ref()
            .map(|s| {
                let mut cols: Vec<_> = s.iter().cloned().collect();
                cols.sort();
                cols
            })
            .unwrap_or_default()
    }
    
    /// Get unique rows from df1
    pub fn df1_unq_rows(&self) -> Option<RecordBatch> {
        self.match_result.as_ref().and_then(|mr| {
            if mr.df1_unique_indices.is_empty() {
                return None;
            }
            
            // Create a new RecordBatch with only unique rows
            let indices = arrow::array::UInt32Array::from(
                mr.df1_unique_indices.iter().map(|&i| i as u32).collect::<Vec<_>>()
            );
            let result = arrow::compute::take_record_batch(&self.df1, &indices);
            result.ok()
        })
    }
    
    /// Get unique rows from df2
    pub fn df2_unq_rows(&self) -> Option<RecordBatch> {
        self.match_result.as_ref().and_then(|mr| {
            if mr.df2_unique_indices.is_empty() {
                return None;
            }
            
            let indices = arrow::array::UInt32Array::from(
                mr.df2_unique_indices.iter().map(|&i| i as u32).collect::<Vec<_>>()
            );
            let result = arrow::compute::take_record_batch(&self.df2, &indices);
            result.ok()
        })
    }
}
