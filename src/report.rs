use crate::column_compare::ColumnComparisonResult;
use arrow::array::RecordBatch;
use std::collections::HashSet;

/// Generates human-readable comparison reports
pub struct ReportGenerator<'a> {
    df1_name: &'a str,
    df2_name: &'a str,
    df1_rows: usize,
    df2_rows: usize,
    df1_cols: usize,
    df2_cols: usize,
    common_cols: &'a HashSet<String>,
    df1_unq_cols: &'a HashSet<String>,
    df2_unq_cols: &'a HashSet<String>,
    join_columns: &'a [String],
    abs_tol: f64,
    rel_tol: f64,
    num_common_rows: usize,
    num_df1_unique: usize,
    num_df2_unique: usize,
    has_duplicates: bool,
    column_results: &'a [ColumnComparisonResult],
    df1: &'a RecordBatch, // Reference to df1 for join key lookup
}

impl<'a> ReportGenerator<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        df1_name: &'a str,
        df2_name: &'a str,
        df1_rows: usize,
        df2_rows: usize,
        df1_cols: usize,
        df2_cols: usize,
        common_cols: &'a HashSet<String>,
        df1_unq_cols: &'a HashSet<String>,
        df2_unq_cols: &'a HashSet<String>,
        join_columns: &'a [String],
        abs_tol: f64,
        rel_tol: f64,
        num_common_rows: usize,
        num_df1_unique: usize,
        num_df2_unique: usize,
        has_duplicates: bool,
        column_results: &'a [ColumnComparisonResult],
        df1: &'a RecordBatch,
    ) -> Self {
        Self {
            df1_name,
            df2_name,
            df1_rows,
            df2_rows,
            df1_cols,
            df2_cols,
            common_cols,
            df1_unq_cols,
            df2_unq_cols,
            join_columns,
            abs_tol,
            rel_tol,
            num_common_rows,
            num_df1_unique,
            num_df2_unique,
            has_duplicates,
            column_results,
            df1,
        }
    }
    
    pub fn generate(&self) -> String {
        let mut report = String::new();
        
        report.push_str("DataComPy Comparison\n");
        report.push_str("--------------------\n\n");
        
        report.push_str(&self.generate_dataframe_summary());
        report.push_str(&self.generate_column_summary());
        report.push_str(&self.generate_row_summary());
        report.push_str(&self.generate_column_comparison());
        report.push_str(&self.generate_unequal_columns_detail());
        report.push_str(&self.generate_sample_diffs());
        
        report
    }
    
    fn generate_dataframe_summary(&self) -> String {
        format!(
            "DataFrame Summary\n\
             -----------------\n\n\
             {:<15} {:>10} {:>10}\n\
             {:<15} {:>10} {:>10}\n\
             {:<15} {:>10} {:>10}\n\n",
            "DataFrame", "Columns", "Rows",
            self.df1_name, self.df1_cols, self.df1_rows,
            self.df2_name, self.df2_cols, self.df2_rows
        )
    }
    
    fn generate_column_summary(&self) -> String {
        let mut summary = String::from("Column Summary\n--------------\n\n");
        
        summary.push_str(&format!("Number of columns in common: {}\n", self.common_cols.len()));
        summary.push_str(&format!(
            "Number of columns in {} but not in {}: {}\n",
            self.df1_name, self.df2_name, self.df1_unq_cols.len()
        ));
        summary.push_str(&format!(
            "Number of columns in {} but not in {}: {}\n",
            self.df2_name, self.df1_name, self.df2_unq_cols.len()
        ));
        
        if !self.df1_unq_cols.is_empty() {
            let mut cols: Vec<_> = self.df1_unq_cols.iter().cloned().collect();
            cols.sort();
            summary.push_str(&format!("\nColumns in {} only: {}\n", self.df1_name, cols.join(", ")));
        }
        
        if !self.df2_unq_cols.is_empty() {
            let mut cols: Vec<_> = self.df2_unq_cols.iter().cloned().collect();
            cols.sort();
            summary.push_str(&format!("Columns in {} only: {}\n", self.df2_name, cols.join(", ")));
        }
        
        summary.push('\n');
        summary
    }
    
    fn generate_row_summary(&self) -> String {
        let mut summary = String::from("Row Summary\n-----------\n\n");
        
        summary.push_str(&format!("Matched on: {}\n", self.join_columns.join(", ")));
        summary.push_str(&format!("Any duplicates on match values: {}\n", 
            if self.has_duplicates { "Yes" } else { "No" }));
        summary.push_str(&format!("Absolute Tolerance: {}\n", self.abs_tol));
        summary.push_str(&format!("Relative Tolerance: {}\n", self.rel_tol));
        summary.push_str(&format!("Number of rows in common: {}\n", self.num_common_rows));
        summary.push_str(&format!(
            "Number of rows in {} but not in {}: {}\n",
            self.df1_name, self.df2_name, self.num_df1_unique
        ));
        summary.push_str(&format!(
            "Number of rows in {} but not in {}: {}\n",
            self.df2_name, self.df1_name, self.num_df2_unique
        ));
        
        // Calculate rows with differences
        // A row has differences if ANY column has unequal values for that row
        // We need to track unique row indices that have at least one difference
        let mut rows_with_diffs = std::collections::HashSet::new();
        for col_result in self.column_results {
            if !col_result.all_equal {
                // This column has differences, so add all rows from sample_diffs
                for diff in &col_result.sample_diffs {
                    rows_with_diffs.insert(diff.row_index);
                }
            }
        }
        
        let num_rows_with_diffs = rows_with_diffs.len();
        let num_rows_all_equal = self.num_common_rows.saturating_sub(num_rows_with_diffs);
        
        summary.push_str(&format!("\nNumber of rows with some compared columns unequal: {}\n", num_rows_with_diffs));
        summary.push_str(&format!("Number of rows with all compared columns equal: {}\n\n", num_rows_all_equal));
        
        summary
    }
    
    fn generate_column_comparison(&self) -> String {
        let num_equal = self.column_results.iter().filter(|r| r.all_equal).count();
        let num_unequal = self.column_results.len() - num_equal;
        
        let total_unequal_values: usize = self.column_results.iter()
            .map(|r| r.num_unequal)
            .sum();
        
        format!(
            "Column Comparison\n\
             -----------------\n\n\
             Number of columns compared with some values unequal: {}\n\
             Number of columns compared with all values equal: {}\n\
             Total number of values which compare unequal: {}\n\n",
            num_unequal, num_equal, total_unequal_values
        )
    }
    
    fn generate_unequal_columns_detail(&self) -> String {
        let unequal_cols: Vec<_> = self.column_results.iter()
            .filter(|r| !r.all_equal)
            .collect();
        
        if unequal_cols.is_empty() {
            return String::new();
        }
        
        // Calculate dynamic column widths
        let max_col_name_len = unequal_cols.iter()
            .map(|c| c.column_name.len())
            .max()
            .unwrap_or(20)
            .max(20); // At least 20 chars
        
        let max_type_len = unequal_cols.iter()
            .flat_map(|c| [c.type1.len(), c.type2.len()])
            .max()
            .unwrap_or(15)
            .max(15); // At least 15 chars
        
        let mut detail = String::from("Columns with Unequal Values or Types\n");
        detail.push_str("------------------------------------\n\n");
        detail.push_str(&format!(
            "{:<width_col$} {:<width_type$} {:<width_type$} {:>12} {:>12} {:>12}\n",
            "Column", 
            &format!("{} dtype", self.df1_name),
            &format!("{} dtype", self.df2_name),
            "# Unequal", 
            "Max Diff", 
            "# Null Diff",
            width_col = max_col_name_len,
            width_type = max_type_len
        ));
        
        for col in unequal_cols {
            detail.push_str(&format!(
                "{:<width_col$} {:<width_type$} {:<width_type$} {:>12} {:>12} {:>12}\n",
                col.column_name,
                col.type1,
                col.type2,
                col.num_unequal,
                col.max_diff.map_or("N/A".to_string(), |d| format!("{:.4}", d)),
                col.num_null_diff,
                width_col = max_col_name_len,
                width_type = max_type_len
            ));
        }
        
        detail.push('\n');
        detail
    }
    
    fn generate_sample_diffs(&self) -> String {
        let mut samples = String::new();
        
        for col in self.column_results.iter().filter(|r| !r.sample_diffs.is_empty()) {
            samples.push_str(&format!(
                "Sample Rows with Unequal Values for '{}'\n\
                 {}\n\n",
                col.column_name,
                "-".repeat(50)
            ));
            
            // Create header with join columns first
            let mut header = String::new();
            for join_col in self.join_columns {
                header.push_str(&format!("{:<20} ", join_col));
            }
            header.push_str(&format!(
                "{:<25} {:<25}\n",
                &format!("{} ({})", col.column_name, self.df1_name),
                &format!("{} ({})", col.column_name, self.df2_name)
            ));
            samples.push_str(&header);
            
            for (i, diff) in col.sample_diffs.iter().enumerate() {
                if i >= 10 {
                    samples.push_str(&format!("... ({} more differences)\n", col.sample_diffs.len() - 10));
                    break;
                }
                
                // Extract and print join key values for this row
                for join_col_name in self.join_columns {
                    if let Some(join_col_array) = self.df1.column_by_name(join_col_name) {
                        let join_val = if join_col_array.is_null(diff.row_index) {
                            "NULL".to_string()
                        } else {
                            self.format_value(join_col_array, diff.row_index)
                        };
                        samples.push_str(&format!("{:<20} ", join_val));
                    }
                }
                
                // Then print the differing values
                samples.push_str(&format!(
                    "{:<25} {:<25}\n",
                    diff.value1,
                    diff.value2
                ));
            }
            
            samples.push('\n');
        }
        
        samples
    }
    
    /// Format a value from an array at a specific index
    fn format_value(&self, array: &arrow::array::ArrayRef, idx: usize) -> String {
        use arrow::array::*;
        use arrow::datatypes::DataType;
        
        match array.data_type() {
            DataType::Int8 => array.as_any().downcast_ref::<Int8Array>().unwrap().value(idx).to_string(),
            DataType::Int16 => array.as_any().downcast_ref::<Int16Array>().unwrap().value(idx).to_string(),
            DataType::Int32 => array.as_any().downcast_ref::<Int32Array>().unwrap().value(idx).to_string(),
            DataType::Int64 => array.as_any().downcast_ref::<Int64Array>().unwrap().value(idx).to_string(),
            DataType::UInt8 => array.as_any().downcast_ref::<UInt8Array>().unwrap().value(idx).to_string(),
            DataType::UInt16 => array.as_any().downcast_ref::<UInt16Array>().unwrap().value(idx).to_string(),
            DataType::UInt32 => array.as_any().downcast_ref::<UInt32Array>().unwrap().value(idx).to_string(),
            DataType::UInt64 => array.as_any().downcast_ref::<UInt64Array>().unwrap().value(idx).to_string(),
            DataType::Float32 => array.as_any().downcast_ref::<Float32Array>().unwrap().value(idx).to_string(),
            DataType::Float64 => array.as_any().downcast_ref::<Float64Array>().unwrap().value(idx).to_string(),
            DataType::Utf8 => array.as_any().downcast_ref::<StringArray>().unwrap().value(idx).to_string(),
            DataType::LargeUtf8 => array.as_any().downcast_ref::<LargeStringArray>().unwrap().value(idx).to_string(),
            DataType::Boolean => array.as_any().downcast_ref::<BooleanArray>().unwrap().value(idx).to_string(),
            _ => format!("{:?}", array),
        }
    }
}
