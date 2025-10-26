"""
Create large-scale realistic scenario with 100k+ rows
Using NYC Yellow Taxi Trip Data or similar large dataset
"""

import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import requests
from io import BytesIO
import time
from example_paths import LARGE_TARGET_DF, LARGE_COMPARISON_DF

def download_large_taxi_dataset():
    """
    Download NYC Yellow Taxi Trip data (parquet format)
    This dataset has 100k+ rows with ~20 columns
    """
    # URL for a sample of Yellow Taxi trip data (January 2023)
    # This is a smaller sample but still substantial
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    
    print("Downloading NYC Yellow Taxi data (Jan 2023)...")
    print(f"URL: {url}")
    print("This may take a minute...")
    
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    # Read directly from bytes
    df = pq.read_table(BytesIO(response.content))
    print(f"Downloaded: {df.num_rows:,} rows × {df.num_columns} columns")
    
    return df

def expand_to_200_columns(table):
    """Duplicate columns to reach ~200 columns"""
    df = table.to_pandas()
    base_cols = list(df.columns)
    current_cols = len(base_cols)
    
    print(f"\nExpanding from {current_cols} to 200 columns...")
    
    # Create new dataframe with duplicated columns
    new_cols = {}
    
    # Keep original columns
    for col in base_cols:
        new_cols[col] = df[col]
    
    # Duplicate numeric columns with slight variations
    duplicate_round = 0
    while len(new_cols) < 200:
        for col in base_cols:
            if len(new_cols) >= 200:
                break
            
            new_col_name = f"{col}_dup{duplicate_round}"
            
            if df[col].dtype in ['float64', 'float32']:
                # Add tiny noise to numeric columns
                new_cols[new_col_name] = df[col] + np.random.randn(len(df)) * 0.001
            elif df[col].dtype in ['int64', 'int32']:
                # Copy with small variations
                new_cols[new_col_name] = df[col] + np.random.randint(-1, 2, len(df))
            else:
                # For other types, just duplicate
                new_cols[new_col_name] = df[col]
        
        duplicate_round += 1
    
    df_expanded = pd.DataFrame(new_cols)
    print(f"Expanded to {len(df_expanded.columns)} columns")
    
    return pa.Table.from_pandas(df_expanded)

def create_large_scale_scenario():
    """Create large-scale comparison scenario"""
    print("=" * 80)
    print("Creating Large-Scale Realistic Scenario (100k+ rows)")
    print("=" * 80)
    
    # Download dataset
    print("\n1. Downloading dataset...")
    start_time = time.time()
    table = download_large_taxi_dataset()
    download_time = time.time() - start_time
    print(f"   Download took {download_time:.2f} seconds")
    
    # Take first 150k rows for consistency
    if table.num_rows > 150000:
        table = table.slice(0, 150000)
        print(f"   Using first 150,000 rows")
    
    # Expand to 200 columns
    print("\n2. Expanding columns...")
    table = expand_to_200_columns(table)
    
    # Convert to pandas for modifications
    df = table.to_pandas()
    
    # Add ID column if not exists
    if 'id' not in df.columns:
        df.insert(0, 'id', range(len(df)))
    
    print(f"\n3. Base dataset: {len(df):,} rows × {len(df.columns)} columns")
    
    # Create target_df (remove first row)
    print("\n4. Creating target_df (removing first row)...")
    target_df = df.iloc[1:].copy().reset_index(drop=True)
    print(f"   Target_df: {len(target_df):,} rows")
    
    # Create comparison_df
    print("\n5. Creating comparison_df with modifications...")
    comparison_df = df.copy()
    
    # Find numeric columns (excluding ID)
    numeric_cols = comparison_df.select_dtypes(include=[np.number]).columns.tolist()
    if 'id' in numeric_cols:
        numeric_cols.remove('id')
    
    # Select 10 random columns to modify
    num_cols_to_modify = min(10, len(numeric_cols))
    cols_to_modify = np.random.choice(numeric_cols, num_cols_to_modify, replace=False)
    print(f"   Selected {num_cols_to_modify} columns to modify")
    
    # Modify top 1000 rows (more realistic than 100 for large dataset)
    rows_to_modify_count = min(1000, len(comparison_df))
    modifications_made = 0
    
    print(f"   Modifying values in top {rows_to_modify_count} rows...")
    
    for col in cols_to_modify:
        # Modify 20% of the top N rows
        num_rows_to_change = int(rows_to_modify_count * 0.2)
        rows_to_change = np.random.choice(rows_to_modify_count, num_rows_to_change, replace=False)
        
        for row_idx in rows_to_change:
            original_value = comparison_df.loc[row_idx, col]
            
            # 50/50 split: within tolerance vs outside tolerance
            if modifications_made % 2 == 0:
                # Within tolerance: ±0.005
                comparison_df.loc[row_idx, col] = original_value + np.random.uniform(-0.005, 0.005)
            else:
                # Outside tolerance: ±0.05
                comparison_df.loc[row_idx, col] = original_value + np.random.uniform(-0.05, 0.05)
            
            modifications_made += 1
    
    print(f"   Made {modifications_made:,} modifications")
    
    # Remove 5% from bottom
    print("\n6. Removing 5% of rows from bottom of comparison_df...")
    rows_to_remove = int(len(comparison_df) * 0.05)
    comparison_df = comparison_df.iloc[:-rows_to_remove].copy()
    print(f"   Removed {rows_to_remove:,} rows")
    
    # Convert to PyArrow
    print("\n7. Converting to PyArrow format...")
    target_table = pa.Table.from_pandas(target_df)
    comparison_table = pa.Table.from_pandas(comparison_df)
    
    # Save to parquet
    print("\n8. Saving to parquet files...")
    pq.write_table(target_table, str(LARGE_TARGET_DF))
    pq.write_table(comparison_table, str(LARGE_COMPARISON_DF))
    
    # Get file sizes
    target_size = LARGE_TARGET_DF.stat().st_size / 1024 / 1024
    comp_size = LARGE_COMPARISON_DF.stat().st_size / 1024 / 1024
    
    print("\n" + "=" * 80)
    print("Summary:")
    print("=" * 80)
    print(f"Target DataFrame:     {target_table.num_rows:,} rows × {target_table.num_columns} columns")
    print(f"Comparison DataFrame: {comparison_table.num_rows:,} rows × {comparison_table.num_columns} columns")
    print(f"\nFile sizes:")
    print(f"  target_df:     {target_size:.2f} MB")
    print(f"  comparison_df: {comp_size:.2f} MB")
    print(f"\nExpected differences:")
    print(f"  - Rows only in target_df: 1")
    print(f"  - Rows only in comparison_df: {rows_to_remove:,}")
    print(f"  - Columns with differences: {num_cols_to_modify}")
    print(f"  - Modified values: {modifications_made:,}")
    print(f"  - With tolerance=0.01: ~{modifications_made//2:,} differences should be detected")
    print("\nFiles created:")
    print(f"  - {LARGE_TARGET_DF.relative_to(LARGE_TARGET_DF.parent.parent)}")
    print(f"  - {LARGE_COMPARISON_DF.relative_to(LARGE_COMPARISON_DF.parent.parent)}")
    print("=" * 80)

if __name__ == "__main__":
    try:
        create_large_scale_scenario()
    except Exception as e:
        print(f"\nError: {e}")
        print("\nTrying alternative: creating synthetic large dataset...")
        
        # Fallback: create synthetic data
        print("\nCreating synthetic dataset with 100k rows...")
        data = {'id': list(range(100000))}
        
        # Create 200 columns
        for i in range(199):
            if i % 3 == 0:
                data[f'col_{i:03d}'] = np.random.randn(100000) * 100
            elif i % 3 == 1:
                data[f'col_{i:03d}'] = np.random.randint(0, 1000, 100000)
            else:
                data[f'col_{i:03d}'] = np.random.choice(['A', 'B', 'C', 'D'], 100000)
        
        df = pd.DataFrame(data)
        
        # Create target (remove first row)
        target_df = df.iloc[1:].copy().reset_index(drop=True)
        
        # Create comparison (modify some values, remove bottom 5%)
        comparison_df = df.copy()
        
        # Modify some numeric columns
        for col in [f'col_{i:03d}' for i in range(0, 20, 3)]:
            for row in range(0, 1000, 10):
                comparison_df.loc[row, col] += 0.05
        
        comparison_df = comparison_df.iloc[:-5000].copy()
        
        # Save
        pq.write_table(pa.Table.from_pandas(target_df), str(LARGE_TARGET_DF))
        pq.write_table(pa.Table.from_pandas(comparison_df), str(LARGE_COMPARISON_DF))
        
        print(f"Created synthetic dataset: {len(target_df):,} rows × {len(target_df.columns)} columns")
