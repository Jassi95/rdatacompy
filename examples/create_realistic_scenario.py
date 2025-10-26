"""
Create realistic comparison scenario using Iris dataset
Expanded to ~200 columns with intentional differences
"""

import pyarrow as pa
import pyarrow.parquet as pq
from sklearn.datasets import load_iris
import numpy as np
from example_paths import TARGET_DF, COMPARISON_DF

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import numpy as np
import pandas as pd
from io import StringIO

def download_iris_dataset():
    """Download the classic Iris dataset as a starting point"""
    from sklearn import datasets
    iris = datasets.load_iris()
    df = pd.DataFrame(
        data=iris.data,
        columns=['sepal_length', 'sepal_width', 'petal_length', 'petal_width']
    )
    df['species'] = iris.target
    df['species_name'] = df['species'].map({0: 'setosa', 1: 'versicolor', 2: 'virginica'})
    return df

def duplicate_columns_to_reach_target(df, target_cols=200):
    """Duplicate columns with variations to reach target column count"""
    base_cols = list(df.columns)
    current_cols = len(base_cols)
    
    print(f"Starting with {current_cols} columns, expanding to {target_cols}...")
    
    duplicates_needed = target_cols - current_cols
    duplicate_round = 0
    
    while len(df.columns) < target_cols:
        for col in base_cols:
            if len(df.columns) >= target_cols:
                break
            
            new_col_name = f"{col}_dup{duplicate_round}"
            
            # Add slight variations based on type
            if df[col].dtype in ['float64', 'float32']:
                # Add small noise to numeric columns
                df[new_col_name] = df[col] + np.random.randn(len(df)) * 0.01
            elif df[col].dtype in ['int64', 'int32']:
                # Add small random offset to int columns
                df[new_col_name] = df[col] + np.random.randint(-2, 3, len(df))
            else:
                # For strings, just duplicate
                df[new_col_name] = df[col]
        
        duplicate_round += 1
    
    print(f"Created {len(df.columns)} columns")
    return df

def create_realistic_comparison_scenario():
    """
    Create realistic comparison scenario:
    1. Start with real data (Iris dataset)
    2. Duplicate columns to reach ~200 columns
    3. Create target_df and comparison_df with specific differences
    """
    print("=" * 80)
    print("Creating Realistic Comparison Scenario")
    print("=" * 80)
    
    # Load and expand dataset
    print("\n1. Loading and expanding dataset...")
    df = download_iris_dataset()
    print(f"Initial shape: {df.shape}")
    
    # Duplicate columns to reach 200
    df = duplicate_columns_to_reach_target(df, target_cols=200)
    print(f"Expanded shape: {df.shape}")
    
    # Add ID column
    df.insert(0, 'id', range(len(df)))
    
    # Create target_df (remove first row)
    print("\n2. Creating target_df (removing first row)...")
    target_df = df.iloc[1:].copy().reset_index(drop=True)
    print(f"Target_df shape: {target_df.shape}")
    
    # Create comparison_df (starts as copy of original)
    print("\n3. Creating comparison_df with modifications...")
    comparison_df = df.copy()
    
    # Modify values in top 100 rows for a few columns
    print("   - Modifying values in top 100 rows...")
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    numeric_cols.remove('id')  # Don't modify ID
    
    # Select 5 random numeric columns to modify
    cols_to_modify = np.random.choice(numeric_cols, min(5, len(numeric_cols)), replace=False)
    print(f"   - Selected columns to modify: {list(cols_to_modify)}")
    
    rows_to_modify = list(range(min(100, len(df))))
    modifications_made = 0
    
    for col in cols_to_modify:
        # Modify about 30% of the top 100 rows
        modify_these_rows = np.random.choice(rows_to_modify, size=30, replace=False)
        
        for row_idx in modify_these_rows:
            original_value = comparison_df.loc[row_idx, col]
            
            # For some rows, make changes within tolerance (should not be detected)
            # For others, make changes outside tolerance (should be detected)
            if modifications_made % 2 == 0:
                # Within tolerance: ±0.005 (within 0.01 tolerance)
                comparison_df.loc[row_idx, col] = original_value + np.random.uniform(-0.005, 0.005)
            else:
                # Outside tolerance: ±0.05 (outside 0.01 tolerance)
                comparison_df.loc[row_idx, col] = original_value + np.random.uniform(-0.05, 0.05)
            
            modifications_made += 1
    
    print(f"   - Made {modifications_made} modifications")
    
    # Remove 5% of rows from the bottom of comparison_df
    print("\n4. Removing 5% of rows from bottom of comparison_df...")
    rows_to_remove = int(len(comparison_df) * 0.05)
    comparison_df = comparison_df.iloc[:-rows_to_remove].copy().reset_index(drop=True)
    print(f"   - Removed {rows_to_remove} rows")
    print(f"   - Comparison_df shape: {comparison_df.shape}")
    
    # Convert to PyArrow tables
    target_table = pa.Table.from_pandas(target_df)
    comparison_table = pa.Table.from_pandas(comparison_df)
    
    # Save to parquet
    pq.write_table(target_table, str(TARGET_DF))
    pq.write_table(comparison_table, str(COMPARISON_DF))
    
    print("\n" + "=" * 80)
    print("Summary:")
    print("=" * 80)
    print(f"Target DataFrame:     {target_table.num_rows} rows × {target_table.num_columns} columns")
    print(f"Comparison DataFrame: {comparison_table.num_rows} rows × {comparison_table.num_columns} columns")
    print(f"\nExpected differences:")
    print(f"  - Rows only in target_df: 1 (row with id=0)")
    print(f"  - Rows only in comparison_df: {rows_to_remove} (bottom 5%)")
    print(f"  - Columns with differences: {len(cols_to_modify)}")
    print(f"  - Modified values: {modifications_made} (mix of within/outside tolerance)")
    print(f"  - With tolerance=0.01: ~{modifications_made//2} differences should be detected")
    print("\nFiles created:")
    print(f"  - {TARGET_DF.relative_to(TARGET_DF.parent.parent)}")
    print(f"  - {COMPARISON_DF.relative_to(COMPARISON_DF.parent.parent)}")
    print("=" * 80)
    
    return target_table, comparison_table

if __name__ == "__main__":
    try:
        create_realistic_comparison_scenario()
    except ImportError as e:
        print(f"\nError: {e}")
        print("\nPlease install scikit-learn:")
        print("  pip install scikit-learn")
