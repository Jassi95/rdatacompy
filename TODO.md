# RDataCompy TODO List

## Bugs

**✅ FIXED (Oct 28, 2025):**
1. ~~Input spark dataframe with dates print visual bug~~ - Fixed date/timestamp formatting in v0.1.10
   - Date32 now shows: `2024-07-09 (day 19898)` instead of `PrimitiveArray [2024-06-24,]`
   - Date64 supported: `Date64(1705269600000 ms, ~day 19736)`
   - Timestamps display cleanly: `2024-01-28 10:30:45`

2. ~~Spark datetime and timestamp that contains timezone~~ - Fixed timezone support in v0.1.10
   - Timestamps with timezone now display properly: `2024-01-28 10:30:45 UTC`
   - All TimeUnit variants supported (Second, Millisecond, Microsecond, Nanosecond)

## High Priority

### 1. Test Additional Data Types
- [ ] **Date/DateTime columns**
  - Test Date32, Date64 types
  - Test Timestamp with different time zones
  - Test comparison with tolerance for timestamps
  - Edge cases: null dates, different date formats
  
- [ ] **String columns**
  - Test Utf8 and LargeUtf8 types
  - Test case-sensitive vs case-insensitive comparison
  - Test with null strings and empty strings
  - Test with special characters and Unicode
  
- [ ] **Other Arrow data types**
  - Boolean columns
  - Binary/LargeBinary
  - ~~Decimal128/Decimal256 (high-precision numbers)~~ ✅ **DONE** - Full support with cross-precision compatibility
  - List/LargeList (nested arrays)
  - Struct types (nested records)
  - Dictionary-encoded columns
  - Duration types

### 2. Prints
- Better looking prints for wide column names on Columns with unequal values or types
- Key columns to start of print(now shows index...)


### 2. Publishing the Library

- [x] **Prepare for PyPI release (Oct 26, 2025)** ✅
  - [x] Updated pyproject.toml with comprehensive metadata
  - [x] Added classifiers and keywords
  - [x] Added project URLs section
  - [x] Version management setup (0.1.0)
  - [x] Python 3.11 support
- [x] **Documentation (Oct 26, 2025)** ✅
  - [x] Comprehensive README.md with:
    - Installation instructions (including optional dependencies)
    - Quick start guide for PyArrow, Pandas, PySpark
    - API reference
    - Performance benchmarks (46M cells/second)
    - Multiple usage examples
  - [x] Created CHANGELOG.md
  - [x] Created PUBLISHING_GUIDE.md (complete step-by-step)
  - [x] Created QUICKSTART_PUBLISH.md (TL;DR version)
  - [x] Created PUBLICATION_CHECKLIST.md
  
- [ ] **Testing before release**
  - [ ] Add unit tests for all Rust modules
  - [ ] Add integration tests
  - [ ] Test on multiple platforms (Linux, macOS, Windows) - needs GitHub Actions
  - [ ] Test with different Python versions (3.8, 3.9, 3.10, 3.11, 3.12)
  
- [ ] **CI/CD Setup**
  - [ ] GitHub Actions for automated testing
  - [x] Automated wheel building for multiple platforms (Linux, macOS, Windows)
  - [x] Automated PyPI publishing on release tags
  
- [x] **Legal/Licensing (Oct 26, 2025)** ✅
  - [x] Added LICENSE file (Apache-2.0)
  - [x] License declared in pyproject.toml
  - [x] All dependencies compatible (Apache Arrow: Apache-2.0, PyO3: Apache-2.0)

## Medium Priority

### 3. Feature Enhancements

- [ ] **Column-specific comparison options**
  - Allow different tolerance per column
  - Case-insensitive string comparison option
  - Ignore whitespace in strings option
  
- [ ] **Performance optimizations**
  - Parallel column comparison using rayon
  - Memory usage optimization for very large datasets
  - Streaming comparison for datasets larger than memory
  
- [ ] **Report enhancements**
  - JSON/CSV output formats
  - HTML report with interactive tables
  - Detailed statistics per column
  - Visualization of differences

### 4. API Improvements
- [x] **Multi-DataFrame Support (Oct 26-27, 2025)** ✅
  - PySpark DataFrame support (3.5+ via `toPandas()`, 4.0+ via `.toArrow()`)
  - Pandas DataFrame support via `pa.Table.from_pandas()`
  - Polars DataFrame support via `.to_arrow()`
  - Can mix different types in same comparison
  - All conversions happen before comparison logic
  - Automatic fallback for Spark 3.5 compatibility
  
- [ ] **Python API sugar**
  - Convenience methods: `compare.to_json()`, `compare.to_html()`
  - Better integration with pandas/polars workflows
  
- [ ] **Configuration options**
  - Max sample differences to collect per column
  - Custom equality functions
  - Column ignore patterns (regex)

## Low Priority

### 5. Quality of Life

- [ ] **Error messages**
  - More helpful error messages for common mistakes
  - Suggestions for fixing schema mismatches
  
- [ ] **Examples**
  - Add more example scripts showing different use cases
  - Jupyter notebook with interactive examples
  
- [ ] **Performance**
  - Benchmark against datacompy and other tools
  - Profile and optimize hotspots

## Completed ✅

- [x] Architecture design
- [x] Core comparison engine
- [x] Row matching (hash-based join)
- [x] Column comparison with numeric tolerance
- [x] PyO3 Python bindings
- [x] Report generation
- [x] Basic testing (small, realistic, large datasets)
- [x] Performance benchmarking (44M+ cells/second)
- [x] Fix report statistics bug
- [x] **Decimal type support (Oct 26, 2025)**
  - Full Decimal128/Decimal256 support
  - Cross-precision compatibility (e.g., DECIMAL(28,12) vs DECIMAL(18,6))
  - Decimal vs Float/Int comparison
  - Proper formatting in reports
  - Tolerance applied correctly to decimals
- [x] **Multi-DataFrame type support (Oct 26, 2025)**
  - PySpark DataFrame (via `.toArrow()`)
  - Pandas DataFrame (via `pa.Table.from_pandas()`)
  - Polars DataFrame (via `.to_arrow()`)
  - PyArrow Table/RecordBatch (native)
  - Mix different types in same comparison

---

## Notes

- Current performance: ~45M cells/second on 150k rows × 200 columns
- Successfully tested with NYC Taxi dataset
- Tolerance feature working correctly (absolute + relative)
- Memory overhead: ~16MB for comparison logic (data not duplicated)


Why RDataCompy is Faster:
Your Rust implementation:

✅ Columnar processing with Arrow - Processes columns using SIMD-optimized Arrow compute kernels
✅ No type inference overhead - Arrow types are known upfront
✅ Hash-based join - O(n) join using AHashMap, not Pandas merge which can be O(n²)
✅ Zero-copy - No data duplication, works directly on Arrow arrays
✅ Minimal allocations - Only stores differences, not entire merged dataframe
✅ Could parallelize - Rust makes it easy to compare columns in parallel (not implemented yet, but possible with rayon)
The key difference: datacompy merges entire dataframes then loops through columns in Python, while you match rows with hashing then compare columns directly on Arrow arrays without copying data.

With 200 columns, the overhead of 200 Python loops + 200 type inferences + doubled memory is why it's slow!
