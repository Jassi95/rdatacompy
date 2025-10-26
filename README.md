# RDataCompy

Lightning-fast dataframe comparison library, implemented in Rust with Python bindings.

## Overview

RDataCompy is a high-performance library for comparing dataframes, inspired by Capital One's [datacompy](https://github.com/capitalone/datacompy). Built in Rust and leveraging Apache Arrow, it provides **40+ million cells/second** throughput with comprehensive comparison reports.

### Why RDataCompy?

- 🚀 **Blazing Fast**: 40-46M cells/second (100-1000x faster than Python-based solutions)
- � **Memory Efficient**: Zero-copy operations using Apache Arrow columnar format
- 🎯 **Flexible**: Configurable tolerance for numeric comparisons
- � **Comprehensive**: Detailed reports showing exact differences
- 🔧 **Multi-Format**: Works with PyArrow, Pandas, PySpark, and Polars DataFrames
- � **Decimal Support**: Full DECIMAL(p,s) support with cross-precision compatibility

## Installation

```bash
pip install rdatacompy
```

### Optional Dependencies

```bash
# For PySpark support
pip install rdatacompy[spark]

# For Pandas support
pip install rdatacompy[pandas]

# For Polars support
pip install rdatacompy[polars]

# Install everything
pip install rdatacompy[all]
```

## Quick Start

### Basic Usage (PyArrow)

```python
import pyarrow as pa
from rdatacompy import Compare

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

# Compare dataframes
comp = Compare(
    df1, 
    df2,
    join_columns=['id'],
    abs_tol=0.01,  # Absolute tolerance for floats
    df1_name='original',
    df2_name='updated'
)

# Print comprehensive report
print(comp.report())
```

### Using with Pandas

```python
import pandas as pd
from rdatacompy import Compare

df1 = pd.DataFrame({
    'id': [1, 2, 3],
    'amount': [100.50, 200.75, 300.25]
})

df2 = pd.DataFrame({
    'id': [1, 2, 3],
    'amount': [100.51, 200.75, 300.24]
})

# Directly compare Pandas DataFrames (auto-converted to Arrow)
comp = Compare(df1, df2, join_columns=['id'], abs_tol=0.01)
print(comp.report())
```

### Using with PySpark

```python
from pyspark.sql import SparkSession
from rdatacompy import Compare

spark = SparkSession.builder.getOrCreate()

df1 = spark.createDataFrame([(1, 100), (2, 200)], ['id', 'value'])
df2 = spark.createDataFrame([(1, 100), (2, 201)], ['id', 'value'])

# Directly compare Spark DataFrames (auto-converted to Arrow)
comp = Compare(df1, df2, join_columns=['id'])
print(comp.report())
```

### Decimal Support

```python
from decimal import Decimal
import pyarrow as pa
from rdatacompy import Compare

# Compare DECIMAL columns with different precision/scale
df1 = pa.table({
    'id': [1, 2, 3],
    'price': pa.array([
        Decimal('123.456789012345'),
        Decimal('999.999999999999'),
        Decimal('42.123456789012')
    ], type=pa.decimal128(28, 12))  # High precision
})

df2 = pa.table({
    'id': [1, 2, 3],
    'price': pa.array([
        Decimal('123.456789'),
        Decimal('999.999998'),
        Decimal('42.123457')
    ], type=pa.decimal128(18, 6))  # Lower precision
})

# Compare with tolerance - handles different precision automatically
comp = Compare(df1, df2, join_columns=['id'], abs_tol=0.00001)
print(comp.report())
```

## Features

### Comparison Report

The report includes:
- **DataFrame Summary**: Row and column counts
- **Column Summary**: Common columns, unique to each dataframe
- **Row Summary**: Matched rows, unique rows, duplicates
- **Column Comparison**: Which columns have differences
- **Sample Differences**: Example rows with unequal values
- **Statistics**: Number of differences, max difference, null differences

### API Methods

```python
comp = Compare(df1, df2, join_columns=['id'])

# Get full comparison report
report = comp.report()

# Check if dataframes match
matches = comp.matches()  # Returns bool

# Get common columns
common_cols = comp.intersect_columns()

# Get columns unique to each dataframe
df1_only = comp.df1_unq_columns()
df2_only = comp.df2_unq_columns()
```

### Supported Data Types

- ✅ Integers: int8, int16, int32, int64, uint8, uint16, uint32, uint64
- ✅ Floats: float32, float64
- ✅ Decimals: decimal128, decimal256 (with cross-precision compatibility)
- ✅ Strings: utf8, large_utf8
- ✅ Booleans
- ✅ Dates: date32, date64
- ✅ Timestamps (with timezone support)

### Cross-Type Compatibility

RDataCompy is designed for real-world data migration scenarios:

```python
# Compare different numeric types (int vs float vs decimal)
df1 = pa.table({'id': [1], 'val': pa.array([100], type=pa.int64())})
df2 = pa.table({'id': [1], 'val': pa.array([100.0], type=pa.float64())})
comp = Compare(df1, df2, join_columns=['id'])
comp.matches()  # True - types are compatible!

# Compare different decimal precisions
df1 = pa.table({'val': pa.array([Decimal('123.45')], type=pa.decimal128(28, 12))})
df2 = pa.table({'val': pa.array([Decimal('123.45')], type=pa.decimal128(18, 6))})
# Compares successfully - precision difference handled automatically
```

## Performance

Benchmarked on a dataset with 150,000 rows × 200 columns (58.8M data points):

- **Comparison time**: 1.3 seconds
- **Throughput**: 46 million cells/second
- **Memory overhead**: 16 MB (only stores differences)

### vs datacompy

RDataCompy is significantly faster than Python-based solutions:
- **Columnar processing**: Uses SIMD-optimized Arrow compute kernels
- **Zero-copy**: Works directly on Arrow arrays without data duplication
- **Hash-based joins**: O(n) row matching vs O(n²) pandas merge
- **No type inference**: Arrow types known upfront (no runtime checks per column)

## Development

### Prerequisites

- Rust 1.70+
- Python 3.8+
- maturin

### Building from Source

```bash
# Clone repository
git clone https://github.com/yourusername/rdatacompy
cd rdatacompy

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install maturin
pip install maturin

# Build and install in development mode
maturin develop --release

# Run examples
python examples/basic_usage.py
```

### Running Tests

```bash
# Rust tests
cargo test

# Python examples
python examples/test_multi_dataframe_types.py
python examples/test_decimal_types.py
python examples/benchmark_large.py
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

Apache-2.0

## Acknowledgments

- Inspired by [Capital One's datacompy](https://github.com/capitalone/datacompy)
- Built with [Apache Arrow](https://arrow.apache.org/) and [PyO3](https://pyo3.rs/)

## Roadmap

See [TODO.md](TODO.md) for planned features and improvements.

