"""
Download and prepare benchmark datasets for testing rdatacompy
"""

import pyarrow as pa
import pyarrow.parquet as pq
import os

def download_online_retail_dataset():
    """
    Online Retail Dataset from UCI
    ~540k rows, but only ~8 columns
    Not ideal for column count but good for row testing
    """
    url = "https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx"
    print("Downloading Online Retail dataset...")
    print("Note: This requires pandas and openpyxl")
    
    import pandas as pd
    df = pd.read_excel(url)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, 'online_retail.parquet')
    print(f"Downloaded: {len(df)} rows, {len(df.columns)} columns")
    return table

def create_synthetic_wide_dataset(num_rows=50000, num_cols=250):
    """
    Create a synthetic dataset with many columns
    Perfect for testing column-wise comparison performance
    """
    import numpy as np
    
    print(f"Creating synthetic dataset: {num_rows} rows × {num_cols} columns...")
    
    # Create data dictionary
    data = {}
    
    # ID column
    data['id'] = list(range(num_rows))
    
    # Mix of different column types
    for i in range(num_cols - 1):
        if i % 4 == 0:
            # Integer columns
            data[f'int_col_{i}'] = np.random.randint(0, 1000, num_rows)
        elif i % 4 == 1:
            # Float columns
            data[f'float_col_{i}'] = np.random.randn(num_rows) * 100
        elif i % 4 == 2:
            # String columns
            choices = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
            data[f'str_col_{i}'] = np.random.choice(choices, num_rows)
        else:
            # Boolean columns
            data[f'bool_col_{i}'] = np.random.choice([True, False], num_rows)
    
    table = pa.table(data)
    
    # Save to parquet
    pq.write_table(table, 'synthetic_wide.parquet')
    print(f"Created: {num_rows} rows, {num_cols} columns")
    print(f"Saved to: synthetic_wide.parquet ({os.path.getsize('synthetic_wide.parquet') / 1024 / 1024:.2f} MB)")
    
    return table

def download_credit_card_fraud():
    """
    Credit Card Fraud Detection Dataset
    ~285k rows, 31 columns
    Good balance of rows and columns
    """
    print("Credit Card Fraud Dataset")
    print("Download from: https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud")
    print("(Requires Kaggle account)")
    print("File: creditcard.csv")
    return None

def create_modified_dataset(table, modification_rate=0.1):
    """
    Create a modified version of a dataset for comparison testing
    
    modification_rate: fraction of values to modify (0.0 to 1.0)
    """
    import numpy as np
    import pandas as pd
    
    print(f"\nCreating modified dataset with {modification_rate*100:.1f}% changes...")
    
    # Convert to pandas for easier manipulation
    df = table.to_pandas()
    df_modified = df.copy()
    
    # Modify some rows
    num_rows_to_modify = int(len(df) * modification_rate)
    rows_to_modify = np.random.choice(len(df), num_rows_to_modify, replace=False)
    
    # Modify some values in each selected row
    for row_idx in rows_to_modify:
        # Modify 1-3 random columns
        num_cols_to_modify = np.random.randint(1, min(4, len(df.columns)))
        cols_to_modify = np.random.choice(
            [c for c in df.columns if c != 'id'], 
            num_cols_to_modify, 
            replace=False
        )
        
        for col in cols_to_modify:
            if df[col].dtype in ['int64', 'int32']:
                df_modified.loc[row_idx, col] = df.loc[row_idx, col] + np.random.randint(1, 10)
            elif df[col].dtype in ['float64', 'float32']:
                df_modified.loc[row_idx, col] = df.loc[row_idx, col] * (1 + np.random.randn() * 0.1)
            elif df[col].dtype == 'object':
                # For strings, modify slightly
                current = str(df.loc[row_idx, col])
                df_modified.loc[row_idx, col] = current + '_mod'
            elif df[col].dtype == 'bool':
                df_modified.loc[row_idx, col] = not df.loc[row_idx, col]
    
    # Add some unique rows to df1
    unique_rows_1 = df.sample(int(len(df) * 0.05))  # 5% unique
    df_modified = pd.concat([df_modified, unique_rows_1]).reset_index(drop=True)
    
    # Remove some rows (will be unique to df2)
    df = df.drop(df.sample(int(len(df) * 0.05)).index).reset_index(drop=True)
    
    print(f"Original: {len(df)} rows")
    print(f"Modified: {len(df_modified)} rows")
    print(f"Expected differences: ~{num_rows_to_modify} rows with changes")
    
    return pa.Table.from_pandas(df), pa.Table.from_pandas(df_modified)

def main():
    print("=" * 80)
    print("Dataset Preparation for RDataCompy Benchmarking")
    print("=" * 80)
    
    # Create synthetic wide dataset (best for testing)
    print("\n1. Creating Synthetic Wide Dataset (RECOMMENDED)")
    print("-" * 80)
    table = create_synthetic_wide_dataset(num_rows=50000, num_cols=250)
    
    # Create modified versions for comparison
    df1, df2 = create_modified_dataset(table, modification_rate=0.05)
    
    # Save both versions
    pq.write_table(df1, 'synthetic_df1.parquet')
    pq.write_table(df2, 'synthetic_df2.parquet')
    
    print("\n" + "=" * 80)
    print("Files created:")
    print("  - synthetic_wide.parquet (original)")
    print("  - synthetic_df1.parquet (for comparison)")
    print("  - synthetic_df2.parquet (for comparison)")
    print("=" * 80)
    
    # Show other options
    print("\n2. Other Dataset Options:")
    print("-" * 80)
    print("Credit Card Fraud (285k rows, 31 cols):")
    print("  https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud")
    print()
    print("NYC Taxi Trip Data (millions of rows, ~20 cols):")
    print("  https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page")
    print()
    print("Airline Dataset (100M+ rows, 29 cols):")
    print("  http://stat-computing.org/dataexpo/2009/the-data.html")
    print()

if __name__ == "__main__":
    main()
