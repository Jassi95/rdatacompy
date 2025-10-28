use pyo3::prelude::*;
use pyo3::types::PyList;
use arrow::pyarrow::PyArrowType;
use arrow::record_batch::RecordBatch;

mod error;
mod types;
mod tolerance;
mod row_matcher;
mod column_compare;
mod report;
mod compare;
mod date_utils;

use compare::DataFrameCompare;

/// Convert PyArrow Table or RecordBatch to RecordBatch
fn pyarrow_to_record_batch(py: Python, obj: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    // Try to extract as RecordBatch first
    if let Ok(batch) = obj.extract::<PyArrowType<RecordBatch>>() {
        return Ok(batch.0);
    }
    
    // If it's a Table, convert to RecordBatch
    // Tables can have multiple batches, so we'll combine them
    let pyarrow = py.import_bound("pyarrow")?;
    
    // Check if it's a Table
    let table_class = pyarrow.getattr("Table")?;
    if obj.is_instance(&table_class)? {
        // Convert table to a single RecordBatch by combining all batches
        let batches = obj.call_method0("to_batches")?;
        let batches_list = batches.downcast::<PyList>()?;
        
        if batches_list.is_empty() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Table has no data"
            ));
        }
        
        // If single batch, just use it
        if batches_list.len() == 1 {
            let batch = batches_list.get_item(0)?;
            let record_batch = batch.extract::<PyArrowType<RecordBatch>>()?;
            return Ok(record_batch.0);
        }
        
        // Multiple batches - need to concatenate
        // For now, let's convert to a single batch using PyArrow's concat_tables
        let concat = pyarrow.getattr("concat_tables")?;
        let combined = concat.call1(([obj],))?;
        let combined_batches = combined.call_method0("to_batches")?;
        let combined_list = combined_batches.downcast::<PyList>()?;
        let first_batch = combined_list.get_item(0)?;
        let record_batch = first_batch.extract::<PyArrowType<RecordBatch>>()?;
        return Ok(record_batch.0);
    }
    
    Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
        "Expected PyArrow Table or RecordBatch"
    ))
}

/// Python wrapper for DataFrameCompare
#[pyclass]
struct Compare {
    inner: DataFrameCompare,
}

#[pymethods]
impl Compare {
    #[new]
    #[pyo3(signature = (df1, df2, join_columns, abs_tol=0.0, rel_tol=0.0, df1_name="df1".to_string(), df2_name="df2".to_string()))]
    fn new(
        py: Python,
        df1: &Bound<'_, PyAny>,
        df2: &Bound<'_, PyAny>,
        join_columns: &Bound<'_, PyAny>,
        abs_tol: f64,
        rel_tol: f64,
        df1_name: String,
        df2_name: String,
    ) -> PyResult<Self> {
        // Convert PyArrow objects to RecordBatch
        let batch1 = pyarrow_to_record_batch(py, df1)?;
        let batch2 = pyarrow_to_record_batch(py, df2)?;
        
        // Convert join_columns to Vec<String>
        let join_cols = if let Ok(s) = join_columns.extract::<String>() {
            vec![s]
        } else if let Ok(list) = join_columns.downcast::<PyList>() {
            list.iter()
                .map(|item| item.extract::<String>())
                .collect::<PyResult<Vec<_>>>()?
        } else {
            return Err(PyErr::new::<pyo3::exceptions::PyTypeError, _>(
                "join_columns must be a string or list of strings"
            ));
        };
        
        let mut comparer = DataFrameCompare::new(
            batch1,
            batch2,
            join_cols,
            abs_tol,
            rel_tol,
            df1_name,
            df2_name,
        )?;
        
        // Run comparison immediately
        comparer.compare()?;
        
        Ok(Self { inner: comparer })
    }
    
    /// Generate a comprehensive comparison report
    fn report(&self) -> PyResult<String> {
        Ok(self.inner.report()?)
    }
    
    /// Check if dataframes match exactly
    fn matches(&self) -> bool {
        self.inner.matches()
    }
    
    /// Get common column names
    #[getter]
    fn intersect_columns(&self, py: Python) -> PyResult<PyObject> {
        let cols = self.inner.intersect_columns();
        let set = pyo3::types::PySet::new_bound(py, &cols)?;
        Ok(set.into())
    }
    
    /// Get df1 unique column names
    #[getter]
    fn df1_unq_columns(&self, py: Python) -> PyResult<PyObject> {
        let cols = self.inner.df1_unq_columns();
        let set = pyo3::types::PySet::new_bound(py, &cols)?;
        Ok(set.into())
    }
    
    /// Get df2 unique column names
    #[getter]
    fn df2_unq_columns(&self, py: Python) -> PyResult<PyObject> {
        let cols = self.inner.df2_unq_columns();
        let set = pyo3::types::PySet::new_bound(py, &cols)?;
        Ok(set.into())
    }
    
    /// Get unique rows from df1 as PyArrow Table
    #[getter]
    fn df1_unq_rows(&self, py: Python) -> PyResult<PyObject> {
        if let Some(batch) = self.inner.df1_unq_rows() {
            let pyarrow = py.import_bound("pyarrow")?;
            let table_class = pyarrow.getattr("Table")?;
            
            // Convert RecordBatch to PyArrow RecordBatch
            let py_batch = PyArrowType(batch).into_py(py);
            
            // Convert to Table
            let table = table_class.call_method1("from_batches", ([py_batch],))?;
            Ok(table.into())
        } else {
            // Return empty table with same schema
            Ok(py.None())
        }
    }
    
    /// Get unique rows from df2 as PyArrow Table
    #[getter]
    fn df2_unq_rows(&self, py: Python) -> PyResult<PyObject> {
        if let Some(batch) = self.inner.df2_unq_rows() {
            let pyarrow = py.import_bound("pyarrow")?;
            let table_class = pyarrow.getattr("Table")?;
            
            let py_batch = PyArrowType(batch).into_py(py);
            let table = table_class.call_method1("from_batches", ([py_batch],))?;
            Ok(table.into())
        } else {
            Ok(py.None())
        }
    }
}

/// RDataCompy - Lightning-fast dataframe comparison for PyArrow
#[pymodule]
fn _rdatacompy(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Compare>()?;
    Ok(())
}
