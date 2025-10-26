# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2025-10-26

### Added
- Initial release of RDataCompy
- Core comparison engine built in Rust
- PyO3 Python bindings for easy integration
- Support for PyArrow Table and RecordBatch
- Hash-based row matching on join columns
- Column-wise comparison with configurable tolerance
- Absolute and relative tolerance for numeric comparisons
- Comprehensive comparison reports with statistics
- Sample differences display
- Multi-dataframe type support:
  - PyArrow Table/RecordBatch (native)
  - Pandas DataFrame (auto-conversion)
  - PySpark DataFrame (auto-conversion)
  - Polars DataFrame (auto-conversion)
- Full decimal type support:
  - Decimal128 and Decimal256
  - Cross-precision compatibility (e.g., DECIMAL(28,12) vs DECIMAL(18,6))
  - Decimal vs Float/Int comparison
- Support for multiple data types:
  - Integers (int8-int64, uint8-uint64)
  - Floats (float32, float64)
  - Decimals (decimal128, decimal256)
  - Strings (utf8, large_utf8)
  - Booleans
  - Dates (date32, date64)
  - Timestamps (with timezone support)
- Performance optimizations:
  - Zero-copy operations using Apache Arrow
  - SIMD-optimized compute kernels
  - O(n) hash-based joins
  - 40-46M cells/second throughput
- Example scripts demonstrating various use cases

### Fixed
- Report statistics calculation bug (row-level equality tracking)

### Performance
- Benchmarked at 46M cells/second on 150k rows × 200 columns
- Memory overhead of only 16MB for comparison logic
- 100-1000x faster than Python-based alternatives

[Unreleased]: https://github.com/yourusername/rdatacompy/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/yourusername/rdatacompy/releases/tag/v0.1.0
