use thiserror::Error;

#[derive(Debug, Error)]
pub enum CompareError {
    #[error("Join column '{0}' not found in dataframe {1}")]
    JoinColumnNotFound(String, String),
    
    #[error("Join columns cannot be empty")]
    EmptyJoinColumns,
    
    #[error("Incompatible types for column '{0}': {1} vs {2}")]
    IncompatibleTypes(String, String, String),
    
    #[error("Dataframe has no columns")]
    EmptyDataFrame,
    
    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),
    
    #[error("Python error: {0}")]
    PythonError(String),
}

impl From<CompareError> for pyo3::PyErr {
    fn from(err: CompareError) -> pyo3::PyErr {
        pyo3::exceptions::PyValueError::new_err(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, CompareError>;
