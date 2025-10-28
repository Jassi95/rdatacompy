"""
Test rdatacompy with realistic scenario
"""

import pyarrow.parquet as pq
import rdatacompy
from pathlib import Path

print("=" * 80)
print("RDataCompy Realistic Scenario Test")
print("=" * 80)

# Load the datasets from extras/test_data
print("\nLoading datasets...")
test_data_dir = Path(__file__).parent.parent / 'extras' / 'test_data'
target_df = pq.read_table(test_data_dir / 'target_df.parquet')
comparison_df = pq.read_table(test_data_dir / 'comparison_df.parquet')

print(f"Target DataFrame:     {target_df.num_rows} rows × {target_df.num_columns} columns")
print(f"Comparison DataFrame: {comparison_df.num_rows} rows × {comparison_df.num_columns} columns")

# Run comparison with tolerance
print("\n" + "=" * 80)
print("Running comparison with abs_tol=0.01...")
print("=" * 80)

compare = rdatacompy.Compare(
    target_df,
    comparison_df,
    join_columns='id',
    abs_tol=0.01,
    rel_tol=0.0,
    df1_name='target',
    df2_name='comparison'
)

# Print the full report
report = compare.report()
print(report)

# Additional statistics
print("\n" + "=" * 80)
print("Quick Stats:")
print("=" * 80)
print(f"DataFrames match: {compare.matches()}")
print(f"Common columns: {len(compare.intersect_columns)}")
print(f"Columns only in target: {len(compare.df1_unq_columns)}")
print(f"Columns only in comparison: {len(compare.df2_unq_columns)}")

if compare.df1_unq_rows is not None:
    print(f"Rows only in target: {compare.df1_unq_rows.num_rows}")
else:
    print("Rows only in target: 0")

if compare.df2_unq_rows is not None:
    print(f"Rows only in comparison: {compare.df2_unq_rows.num_rows}")
else:
    print("Rows only in comparison: 0")

print("=" * 80)
