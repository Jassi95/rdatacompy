"""
Test case to demonstrate the clarified statistics in comparison reports.

This test creates scenarios to show:
1. Rows where ALL compared columns are unequal
2. Columns where ALL values are unequal
3. Mixed scenarios with some equal and some unequal
"""

import pyarrow as pa
from rdatacompy import Compare

# Create test data with specific patterns:
# - Row 0: All 3 numeric columns differ
# - Row 1: 2 out of 3 columns differ
# - Row 2: 1 out of 3 columns differ
# - Row 3: All columns match
# - Row 4: All 3 numeric columns differ
# - Column 'always_diff': ALL 5 rows differ
# - Column 'sometimes_diff': 2 rows differ
# - Column 'rarely_diff': 1 row differs

target_data = {
    'id': [1, 2, 3, 4, 5],
    'always_diff': [10.0, 20.0, 30.0, 40.0, 50.0],      # ALL values will differ
    'sometimes_diff': [100.0, 200.0, 300.0, 400.0, 500.0],  # Some values differ
    'rarely_diff': [1000.0, 2000.0, 3000.0, 4000.0, 5000.0]  # One value differs
}

comparison_data = {
    'id': [1, 2, 3, 4, 5],
    'always_diff': [11.0, 22.0, 30.0, 40.0, 50.0],      # Rows 1,2 differ, rest match
    'sometimes_diff': [101.0, 200.0, 300.0, 400.0, 500.0],  # Only row 1 differs, rest match
    'rarely_diff': [1001.0, 2000.0, 3000.0, 4000.0, 5000.0]  # Only row 1 differs
}

target_table = pa.table(target_data)
comparison_table = pa.table(comparison_data)

print("=" * 80)
print("Test Case: Statistics Clarification")
print("=" * 80)
print("\nScenario Setup:")
print("-" * 80)
print("5 rows total with 3 numeric columns to compare")
print("\nColumn patterns:")
print("  • 'always_diff': 2 rows differ (rows 1, 2)")
print("  • 'sometimes_diff': 1 row differs (row 1)")
print("  • 'rarely_diff': 1 row differs (row 1)")
print("\nRow patterns:")
print("  • Row 1 (id=1): ALL 3 columns differ")
print("  • Row 2 (id=2): 1 out of 3 columns differ")
print("  • Row 3 (id=3): 0 columns differ (all match)")
print("  • Row 4 (id=4): 0 columns differ (all match)")
print("  • Row 5 (id=5): 0 columns differ (all match)")
print("\n" + "=" * 80)

# Run comparison
comp = Compare(
    target_table,
    comparison_table,
    join_columns=['id'],
    df1_name='target',
    df2_name='comparison',
    abs_tol=0.5,  # Small tolerance so our diffs are detected
    rel_tol=0.0
)

report = comp.report()
print("\n" + report)

print("\n" + "=" * 80)
print("Expected Statistics:")
print("=" * 80)
print("Row Summary should show:")
print("  ✓ Number of rows with all compared columns equal: 3 (rows 3, 4, 5)")
print("  ✓ Number of rows with some compared columns unequal: 2 (rows 1, 2)")
print("  ✓ Number of rows with all compared columns unequal: 1 (row 1)")
print("\nColumn Comparison should show:")
print("  ✓ Number of columns compared with all values equal: 0")
print("  ✓ Number of columns compared with some values unequal: 3")
print("  ✓ Number of columns compared with all values unequal: 0 (none)")
print("=" * 80)
