"""
Test multi-dataframe type support in RDataCompy.

This demonstrates that you can now pass different dataframe types directly
to the comparison engine, without manually converting to PyArrow.
"""

import pyarrow as pa
import pandas as pd
from rdatacompy import Compare

print("=" * 80)
print("RDataCompy - Multi-DataFrame Type Support")
print("=" * 80)

# Test 1: PyArrow Table (original support)
print("\n" + "=" * 80)
print("Test 1: PyArrow Tables (Original)")
print("=" * 80)

arrow_t1 = pa.table({
    'id': [1, 2, 3, 4],
    'value': [100, 200, 300, 400],
    'name': ['Alice', 'Bob', 'Charlie', 'David']
})

arrow_t2 = pa.table({
    'id': [1, 2, 3, 4],
    'value': [100, 201, 300, 400],
    'name': ['Alice', 'Bob', 'Charlie', 'David']
})

comp = Compare(arrow_t1, arrow_t2, join_columns=['id'])
print(comp.report())

# Test 2: Pandas DataFrames
print("\n" + "=" * 80)
print("Test 2: Pandas DataFrames")
print("=" * 80)

pandas_df1 = pd.DataFrame({
    'id': [1, 2, 3, 4],
    'value': [100, 200, 300, 400],
    'name': ['Alice', 'Bob', 'Charlie', 'David']
})

pandas_df2 = pd.DataFrame({
    'id': [1, 2, 3, 4],
    'value': [100, 201, 300, 400],
    'name': ['Alice', 'Bob', 'Charlie', 'David']
})

print("\nDataFrame 1:")
print(pandas_df1)
print("\nDataFrame 2:")
print(pandas_df2)

# No conversion needed - just pass Pandas directly!
comp_pandas = Compare(
    pandas_df1, 
    pandas_df2, 
    join_columns=['id'],
    df1_name="source",
    df2_name="target"
)

print(comp_pandas.report())

# Test 3: Mixed types - Pandas vs PyArrow
print("\n" + "=" * 80)
print("Test 3: Mixed Types (Pandas vs PyArrow)")
print("=" * 80)

pandas_df = pd.DataFrame({
    'id': [1, 2, 3],
    'price': [10.5, 20.5, 30.5]
})

arrow_table = pa.table({
    'id': [1, 2, 3],
    'price': [10.5, 20.5, 30.5]
})

print("\nComparing Pandas DataFrame with PyArrow Table...")
print("They should match perfectly!")

comp_mixed = Compare(
    pandas_df,
    arrow_table,
    join_columns=['id'],
    df1_name="pandas",
    df2_name="arrow"
)

print(comp_mixed.report())

# Test 4: Pandas with decimals
print("\n" + "=" * 80)
print("Test 4: Pandas DataFrames with High-Precision Decimals")
print("=" * 80)

from decimal import Decimal

df1_decimal = pd.DataFrame({
    'id': [1, 2, 3],
    'amount': [Decimal('123.456789012345'), Decimal('999.999999999999'), Decimal('42.123456789012')]
})

df2_decimal = pd.DataFrame({
    'id': [1, 2, 3],
    'amount': [Decimal('123.456789'), Decimal('999.999998'), Decimal('42.123457')]
})

print("\nDataFrame 1 (high precision):")
print(df1_decimal)
print("\nDataFrame 2 (lower precision):")
print(df2_decimal)

comp_decimal = Compare(
    df1_decimal,
    df2_decimal,
    join_columns=['id'],
    abs_tol=0.00001,
    df1_name="high_prec",
    df2_name="std_prec"
)

print(comp_decimal.report())

# Test 5: Large Pandas DataFrame
print("\n" + "=" * 80)
print("Test 5: Large Pandas DataFrame Performance")
print("=" * 80)

import time
import numpy as np

n_rows = 50_000
print(f"\nCreating Pandas DataFrames with {n_rows:,} rows...")

df1_large = pd.DataFrame({
    'id': np.arange(n_rows),
    'value1': np.arange(n_rows) * 2,
    'value2': np.arange(n_rows) * 3,
    'value3': np.arange(n_rows) * 4.5,
    'name': [f'user_{i}' for i in range(n_rows)]
})

df2_large = pd.DataFrame({
    'id': np.arange(n_rows),
    'value1': np.arange(n_rows) * 2,
    'value2': np.arange(n_rows) * 3 + (np.arange(n_rows) < 100).astype(int),  # 100 differences
    'value3': np.arange(n_rows) * 4.5,
    'name': [f'user_{i}' for i in range(n_rows)]
})

print(f"✓ Created DataFrames: {len(df1_large):,} rows × {len(df1_large.columns)} columns")

start_time = time.time()

comp_large = Compare(
    df1_large,
    df2_large,
    join_columns=['id'],
    abs_tol=0.01
)

comparison_time = time.time() - start_time

print(f"\n✓ Comparison completed in {comparison_time:.3f} seconds")
print(f"  Throughput: {len(df1_large) * len(df1_large.columns) / comparison_time:,.0f} cells/second")

# Get summary
report = comp_large.report()
lines = report.split('\n')
for i, line in enumerate(lines):
    if 'Row Summary' in line:
        print('\n'.join(lines[i:i+15]))
        break

# Test 6: Error handling
print("\n" + "=" * 80)
print("Test 6: Error Handling for Unsupported Types")
print("=" * 80)

try:
    # Try to compare a list (not supported)
    comp_bad = Compare(
        [1, 2, 3],  # Not a dataframe!
        arrow_t1,
        join_columns=['id']
    )
except TypeError as e:
    print(f"\n✓ Correctly caught TypeError: {e}")

print("\n" + "=" * 80)
print("✓ All tests completed successfully!")
print("=" * 80)

print("\nSupported DataFrame types:")
print("  ✓ PyArrow Table")
print("  ✓ PyArrow RecordBatch")
print("  ✓ Pandas DataFrame")
print("  ✓ PySpark DataFrame (if pyspark installed)")
print("  ✓ Polars DataFrame (if polars installed)")
print("\nKey features:")
print("  • Automatic conversion to PyArrow under the hood")
print("  • Can mix different dataframe types in same comparison")
print("  • Fast performance maintained (Rust implementation)")
print("  • Full decimal support across all types")
