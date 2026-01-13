"""
Test sorting improvements in report output
"""

import pyarrow as pa
from rdatacompy import Compare

print("=" * 80)
print("Testing Sorting Improvements")
print("=" * 80)

# Create test data with columns that will sort differently
# when alphabetical vs by max_diff
df1 = pa.table({
    'id': [1, 2, 3, 4, 5],
    'zebra_col': [100.0, 200.0, 300.0, 400.0, 500.0],      # Z column, small diff
    'alpha_col': [10.0, 20.0, 30.0, 40.0, 50.0],           # A column, large diff
    'middle_col': [5.0, 10.0, 15.0, 20.0, 25.0],           # M column, medium diff
    'beta_col': [1.0, 2.0, 3.0, 4.0, 5.0],                 # B column, tiny diff
})

df2 = pa.table({
    'id': [1, 2, 3, 4, 5],
    'zebra_col': [101.5, 200.0, 301.5, 400.0, 501.5],      # Max diff: 1.5
    'alpha_col': [50.0, 20.0, 80.0, 40.0, 100.0],          # Max diff: 50.0 (LARGEST)
    'middle_col': [15.0, 10.0, 25.0, 20.0, 35.0],          # Max diff: 10.0 (MEDIUM)
    'beta_col': [1.2, 2.0, 3.2, 4.0, 5.2],                 # Max diff: 0.2 (SMALLEST)
})

comp = Compare(df1, df2, join_columns=['id'], df1_name='baseline', df2_name='current')
report = comp.report()

print("\n" + "=" * 80)
print("VERIFICATION:")
print("=" * 80)

print("\n1. Summary Table - Should be ALPHABETICAL:")
print("-" * 80)
lines = report.split('\n')
in_summary = False
for i, line in enumerate(lines):
    if "Columns with Unequal Values or Types" in line:
        in_summary = True
        print(line)
    elif in_summary:
        if line.strip() == "":
            break
        print(line)

print("\n2. Sample Diffs - Should be SORTED BY MAX DIFF (largest first):")
print("-" * 80)
in_samples = False
for i, line in enumerate(lines):
    if "Sample Rows with Unequal Values" in line:
        in_samples = True
        print(f"\n{line}")
        # Print the column name and a few lines
        for j in range(i+1, min(i+5, len(lines))):
            if "Sample Rows with Unequal Values" in lines[j]:
                break
            print(lines[j])

print("\n" + "=" * 80)
print("Expected Order:")
print("=" * 80)
print("Summary Table (Alphabetical):")
print("  1. alpha_col")
print("  2. beta_col")
print("  3. middle_col")
print("  4. zebra_col")
print("\nSample Diffs (By Max Diff - largest first):")
print("  1. alpha_col (Max Diff: 50.0)")
print("  2. middle_col (Max Diff: 10.0)")
print("  3. zebra_col (Max Diff: 1.5)")
print("  4. beta_col (Max Diff: 0.2)")
print("=" * 80)
