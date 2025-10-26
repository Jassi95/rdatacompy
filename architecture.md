# RDataCompy Architecture

## Overview

RDataCompy is a high-performance Rust library for comparing PyArrow dataframes, inspired by [Capital One's datacompy](https://github.com/capitalone/datacompy). The library leverages Rust's performance and PyO3 bindings to provide lightning-fast dataframe comparison capabilities callable from Python.

## Design Goals

1. **Performance**: Utilize Rust's zero-cost abstractions and efficient memory management for fast comparisons
2. **Python Integration**: Seamless integration with Python via PyO3 and PyArrow
3. **Memory Efficiency**: Process large dataframes efficiently using Arrow's columnar format
4. **Accuracy**: Support configurable absolute and relative tolerance for numeric comparisons
5. **Comprehensive Reporting**: Provide detailed, human-readable comparison reports

## Core Features (Phase 1)

### 1. DataFrame Comparison
- Compare two PyArrow dataframes/tables
- Support join-based comparison on one or more key columns
- Identify rows unique to each dataframe
- Identify rows common to both dataframes
- Handle duplicate rows in join columns

### 2. Column-wise Comparison
- Detect columns present in only one dataframe
- Compare values column-by-column for matching rows
- Support different data types:
  - Numeric types (int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64)
  - String types
  - Boolean types
  - Temporal types (date, timestamp)

### 3. Numeric Comparison with Tolerance
- Absolute tolerance: `|a - b| <= abs_tol`
- Relative tolerance: `|a - b| / |b| <= rel_tol`
- Configurable per comparison

### 4. Comprehensive Reporting
- DataFrame summary (rows, columns)
- Column summary (common, unique to each)
- Row summary (common, unique, matched, unmatched)
- Detailed mismatch reporting per column
- Sample rows with differences
- Statistics: max difference, null differences, etc.

## Technical Architecture

### Component Structure

```
rdatacompy/
├── src/
│   ├── lib.rs                 # Main library entry point, PyO3 module definition
│   ├── compare.rs             # Core comparison engine
│   ├── column_compare.rs      # Column-wise comparison logic
│   ├── row_matcher.rs         # Row matching and join logic
│   ├── tolerance.rs           # Numeric tolerance handling
│   ├── report.rs              # Report generation
│   ├── types.rs               # Type definitions and conversions
│   └── utils.rs               # Utility functions
├── python/
│   └── rdatacompy/
│       ├── __init__.py        # Python package entry point
│       └── _rdatacompy.pyi    # Type stubs for IDE support
├── tests/
│   ├── unit/                  # Rust unit tests
│   └── integration/           # Python integration tests
├── benches/                   # Benchmarks
├── Cargo.toml
├── pyproject.toml
└── README.md
```

### Key Components

#### 1. **Compare Engine** (`compare.rs`)
Main orchestrator for dataframe comparison.

```rust
pub struct DataFrameCompare {
    df1: RecordBatch,           // First dataframe (as Arrow RecordBatch)
    df2: RecordBatch,           // Second dataframe
    join_columns: Vec<String>,   // Columns to join on
    abs_tol: f64,               // Absolute tolerance for numeric comparison
    rel_tol: f64,               // Relative tolerance for numeric comparison
    df1_name: String,           // Name for df1 in reports
    df2_name: String,           // Name for df2 in reports
}

impl DataFrameCompare {
    pub fn new(...) -> Self;
    pub fn compare(&mut self) -> ComparisonResult;
    pub fn report(&self) -> String;
}
```

**Responsibilities:**
- Initialize comparison with two dataframes and configuration
- Coordinate row matching and column comparison
- Generate comparison results
- Produce human-readable reports

#### 2. **Row Matcher** (`row_matcher.rs`)
Handles joining dataframes on key columns and identifying matching/unique rows.

```rust
pub struct RowMatcher {
    join_columns: Vec<String>,
}

pub struct RowMatchResult {
    common_rows: Vec<(usize, usize)>,    // (df1_idx, df2_idx) pairs
    df1_unique_indices: Vec<usize>,       // Indices only in df1
    df2_unique_indices: Vec<usize>,       // Indices only in df2
    has_duplicates: bool,                 // Flag for duplicate join keys
}

impl RowMatcher {
    pub fn match_rows(&self, df1: &RecordBatch, df2: &RecordBatch) 
        -> Result<RowMatchResult>;
}
```

**Responsibilities:**
- Create composite keys from join columns
- Build hash maps for efficient lookups
- Handle duplicate keys by creating temporary unique IDs
- Identify matching and unique rows

#### 3. **Column Comparator** (`column_compare.rs`)
Performs column-by-column value comparison.

```rust
pub struct ColumnComparator {
    abs_tol: f64,
    rel_tol: f64,
}

pub struct ColumnComparisonResult {
    column_name: String,
    all_equal: bool,
    num_unequal: usize,
    num_null_diff: usize,
    max_diff: Option<f64>,      // For numeric columns
    sample_diffs: Vec<SampleDiff>,
}

pub struct SampleDiff {
    row_index: usize,
    value1: String,
    value2: String,
}

impl ColumnComparator {
    pub fn compare_column(
        &self,
        col1: &ArrayRef,
        col2: &ArrayRef,
        matched_indices: &[(usize, usize)]
    ) -> Result<ColumnComparisonResult>;
}
```

**Responsibilities:**
- Dispatch to type-specific comparison functions
- Handle numeric tolerance comparison
- Collect statistics about differences
- Sample rows with mismatches

#### 4. **Tolerance Checker** (`tolerance.rs`)
Implements numeric comparison with tolerance.

```rust
pub struct ToleranceChecker {
    abs_tol: f64,
    rel_tol: f64,
}

impl ToleranceChecker {
    pub fn within_tolerance(&self, a: f64, b: f64) -> bool;
    pub fn difference(&self, a: f64, b: f64) -> f64;
}
```

**Responsibilities:**
- Check if two numeric values are within tolerance
- Calculate differences for reporting

#### 5. **Report Generator** (`report.rs`)
Creates human-readable comparison reports.

```rust
pub struct ReportGenerator {
    comparison_result: ComparisonResult,
    df1_name: String,
    df2_name: String,
}

impl ReportGenerator {
    pub fn generate(&self) -> String;
    fn generate_summary(&self) -> String;
    fn generate_column_summary(&self) -> String;
    fn generate_row_summary(&self) -> String;
    fn generate_column_comparison(&self) -> String;
    fn generate_sample_diffs(&self) -> String;
}
```

**Responsibilities:**
- Format comparison results into readable text
- Generate summary statistics
- Format sample differences in tabular format

#### 6. **Type Handlers** (`types.rs`)
Manages Arrow type conversions and PyO3 integration.

```rust
pub fn pyarrow_to_recordbatch(py_table: PyObject) -> Result<RecordBatch>;
pub fn arrow_type_to_string(data_type: &DataType) -> String;
```

**Responsibilities:**
- Convert between PyArrow and Rust Arrow types
- Handle type compatibility checks
- Provide type name formatting for reports

### Data Flow

```
Python Code
    │
    ├─> Create PyArrow DataFrames/Tables
    │
    ├─> Call rdatacompy.Compare(df1, df2, join_columns=...)
    │
    ▼
PyO3 Bindings (lib.rs)
    │
    ├─> Convert PyArrow Tables to Arrow RecordBatch
    │
    ├─> Create DataFrameCompare instance
    │
    ▼
Compare Engine (compare.rs)
    │
    ├─> Validate inputs (columns exist, types compatible)
    │
    ├─> Call RowMatcher to identify matching rows
    │   │
    │   ├─> Build hash maps on join columns
    │   ├─> Handle duplicates with temp IDs
    │   └─> Return RowMatchResult
    │
    ├─> For each common column:
    │   │
    │   ├─> Call ColumnComparator
    │   │   │
    │   │   ├─> Dispatch to type-specific comparison
    │   │   ├─> Use ToleranceChecker for numerics
    │   │   └─> Collect statistics and samples
    │   │
    │   └─> Store ColumnComparisonResult
    │
    ├─> Build ComparisonResult
    │
    ▼
Report Generation (report.rs)
    │
    ├─> Format statistics
    ├─> Create tabular outputs
    └─> Return formatted string
    │
    ▼
Back to Python
    │
    └─> User accesses results and reports
```

## Dependencies

### Rust Dependencies
```toml
[dependencies]
arrow = "53.0"                # Apache Arrow implementation
pyo3 = { version = "0.22", features = ["extension-module"] }
pyo3-arrow = "0.4"            # PyArrow integration for PyO3
ahash = "0.8"                 # Fast hashing for join operations
indexmap = "2.0"              # Ordered hash maps
thiserror = "2.0"             # Error handling
```

### Python Dependencies
```toml
[build-system]
requires = ["maturin>=1.0,<2.0"]
build-backend = "maturin"

[project]
dependencies = [
    "pyarrow>=14.0.0",
]
```

## PyO3 Python Interface

```python
class Compare:
    """
    Compare two PyArrow tables/dataframes.
    
    Parameters
    ----------
    df1 : pyarrow.Table
        First dataframe to compare
    df2 : pyarrow.Table
        Second dataframe to compare
    join_columns : str or list of str
        Column(s) to join on
    abs_tol : float, default 0.0
        Absolute tolerance for numeric comparison
    rel_tol : float, default 0.0
        Relative tolerance for numeric comparison
    df1_name : str, default "df1"
        Name for first dataframe in reports
    df2_name : str, default "df2"
        Name for second dataframe in reports
    """
    
    def __init__(
        self,
        df1: pa.Table,
        df2: pa.Table,
        join_columns: Union[str, List[str]],
        abs_tol: float = 0.0,
        rel_tol: float = 0.0,
        df1_name: str = "df1",
        df2_name: str = "df2",
    ) -> None: ...
    
    def report(self) -> str:
        """Generate a comprehensive comparison report."""
        ...
    
    def matches(self) -> bool:
        """Return True if dataframes match exactly."""
        ...
    
    @property
    def intersect_columns(self) -> set[str]:
        """Columns present in both dataframes."""
        ...
    
    @property
    def df1_unq_columns(self) -> set[str]:
        """Columns only in df1."""
        ...
    
    @property
    def df2_unq_columns(self) -> set[str]:
        """Columns only in df2."""
        ...
    
    @property
    def df1_unq_rows(self) -> pa.Table:
        """Rows only in df1."""
        ...
    
    @property
    def df2_unq_rows(self) -> pa.Table:
        """Rows only in df2."""
        ...
```

## Performance Optimizations

### 1. **Efficient Row Matching**
- Use `AHashMap` (fast non-cryptographic hash) for join operations
- For duplicate handling, sort within groups before generating temp IDs
- Preallocate vectors based on dataframe sizes

### 2. **Columnar Processing**
- Leverage Arrow's columnar format for cache-friendly memory access
- Process columns in parallel using `rayon` when beneficial
- Avoid unnecessary data copies; work with views when possible

### 3. **Memory Management**
- Use Arrow's zero-copy slicing for subsetting data
- Stream comparisons for very large dataframes (future enhancement)
- Reuse allocations where possible

### 4. **SIMD Optimizations**
- Leverage Arrow's SIMD-optimized comparison kernels
- Use Arrow compute kernels for operations like null checking

## Error Handling

Use `thiserror` for structured error types:

```rust
#[derive(Debug, thiserror::Error)]
pub enum CompareError {
    #[error("Join column '{0}' not found in dataframe")]
    JoinColumnNotFound(String),
    
    #[error("Incompatible types for column '{0}': {1} vs {2}")]
    IncompatibleTypes(String, String, String),
    
    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),
    
    #[error("PyO3 error: {0}")]
    PyO3Error(#[from] pyo3::PyErr),
}
```

## Testing Strategy

### 1. **Unit Tests** (Rust)
- Test each component in isolation
- Test edge cases: empty dataframes, all nulls, duplicates
- Test tolerance logic with various numeric scenarios
- Test type conversions

### 2. **Integration Tests** (Python)
- Compare against datacompy for correctness
- Test with various PyArrow table configurations
- Test large dataframes for performance
- Test error cases and error messages

### 3. **Benchmarks**
- Compare performance vs datacompy (Pandas-based)
- Benchmark with various dataframe sizes
- Profile memory usage

## Development Phases

### Phase 1: Core Functionality (MVP)
- [x] Project architecture design
- [ ] Basic row matching on single join column
- [ ] Numeric column comparison with tolerance
- [ ] String column comparison
- [ ] Basic report generation
- [ ] PyO3 bindings for core API

### Phase 2: Enhanced Features
- [ ] Multi-column joins
- [ ] Duplicate row handling
- [ ] All data types support (temporal, boolean, etc.)
- [ ] Comprehensive report formatting
- [ ] Property accessors (intersect_columns, unique_rows, etc.)

### Phase 3: Performance & Polish
- [ ] Parallel column processing
- [ ] Memory optimization
- [ ] Comprehensive error messages
- [ ] Full test coverage
- [ ] Documentation and examples

### Phase 4: Advanced Features (Future)
- [ ] Streaming comparison for very large datasets
- [ ] Custom comparison functions
- [ ] Export comparison results to various formats (JSON, Parquet)
- [ ] Integration with Polars dataframes

## Build System

Using Maturin for building Python wheels:

```toml
[build-system]
requires = ["maturin>=1.0,<2.0"]
build-backend = "maturin"

[project]
name = "rdatacompy"
version = "0.1.0"
description = "Lightning-fast dataframe comparison library"
requires-python = ">=3.8"
```

Build commands:
```bash
# Development build
maturin develop

# Release build
maturin build --release

# Build and install wheel
maturin build --release && pip install target/wheels/rdatacompy-*.whl
```

## Example Usage

```python
import pyarrow as pa
import rdatacompy

# Create sample tables
df1 = pa.table({
    'id': [1, 2, 3, 4],
    'value': [10.0, 20.0, 30.0, 40.0],
    'name': ['Alice', 'Bob', 'Charlie', 'David']
})

df2 = pa.table({
    'id': [1, 2, 3, 5],
    'value': [10.001, 20.0, 30.5, 50.0],
    'name': ['Alice', 'Bob', 'Chuck', 'Eve']
})

# Compare
compare = rdatacompy.Compare(
    df1, 
    df2,
    join_columns='id',
    abs_tol=0.01,
    df1_name='original',
    df2_name='updated'
)

# Print report
print(compare.report())

# Access specific results
print(f"Matches: {compare.matches()}")
print(f"Common columns: {compare.intersect_columns}")
print(f"Unique to df1: {compare.df1_unq_rows}")
```

## Conclusion

This architecture provides a solid foundation for building a high-performance dataframe comparison library in Rust with Python bindings. The modular design allows for incremental development while maintaining clean separation of concerns. The use of Apache Arrow ensures efficient memory usage and compatibility with the broader data ecosystem.
