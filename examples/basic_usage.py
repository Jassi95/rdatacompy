"""
Simple example demonstrating rdatacompy usage
"""

import pyarrow as pa
import rdatacompy

def main():
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
    
    print("=" * 80)
    print("DataFrame 1:")
    print(df1.to_pandas())
    print("\nDataFrame 2:")
    print(df2.to_pandas())
    print("=" * 80)
    
    # Compare dataframes
    compare = rdatacompy.Compare(
        df1, 
        df2,
        join_columns='id',
        abs_tol=0.01,
        df1_name='original',
        df2_name='updated'
    )
    
    # Print comprehensive report
    print("\n" + compare.report())
    
    # Check individual properties
    print("=" * 80)
    print(f"DataFrames match: {compare.matches()}")
    print(f"Common columns: {compare.intersect_columns()}")
    print(f"Columns only in df1: {compare.df1_unq_columns()}")
    print(f"Columns only in df2: {compare.df2_unq_columns()}")
    print("=" * 80)

if __name__ == "__main__":
    main()
