"""
Benchmark rdatacompy against datacompy (pandas-based)
"""

import time
import pyarrow as pa
import pyarrow.parquet as pq
import rdatacompy
import psutil
import os

def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def benchmark_rdatacompy(df1, df2, join_cols='id', abs_tol=0.01):
    """Benchmark rdatacompy (Rust-based)"""
    mem_before = get_memory_usage()
    start_time = time.time()
    
    compare = rdatacompy.Compare(
        df1, df2,
        join_columns=join_cols,
        abs_tol=abs_tol,
        df1_name='df1',
        df2_name='df2'
    )
    
    # Generate report (tests all functionality)
    report = compare.report()
    matches = compare.matches()
    
    end_time = time.time()
    mem_after = get_memory_usage()
    
    return {
        'time': end_time - start_time,
        'memory': mem_after - mem_before,
        'report_length': len(report),
        'matches': matches
    }

def benchmark_datacompy(df1, df2, join_cols='id', abs_tol=0.01):
    """Benchmark datacompy (Pandas-based) if available"""
    try:
        import datacompy
        import pandas as pd
        
        # Convert to pandas
        pdf1 = df1.to_pandas()
        pdf2 = df2.to_pandas()
        
        mem_before = get_memory_usage()
        start_time = time.time()
        
        compare = datacompy.Compare(
            pdf1, pdf2,
            join_columns=join_cols,
            abs_tol=abs_tol,
            df1_name='df1',
            df2_name='df2'
        )
        
        report = compare.report()
        matches = compare.matches()
        
        end_time = time.time()
        mem_after = get_memory_usage()
        
        return {
            'time': end_time - start_time,
            'memory': mem_after - mem_before,
            'report_length': len(report),
            'matches': matches
        }
    except ImportError:
        print("datacompy not installed. Install with: pip install datacompy")
        return None

def run_benchmark():
    print("=" * 80)
    print("RDataCompy Performance Benchmark")
    print("=" * 80)
    
    # Check if datasets exist
    if not os.path.exists('synthetic_df1.parquet'):
        print("\nDatasets not found! Run prepare_datasets.py first:")
        print("  python examples/prepare_datasets.py")
        return
    
    # Load datasets
    print("\nLoading datasets...")
    df1 = pq.read_table('synthetic_df1.parquet')
    df2 = pq.read_table('synthetic_df2.parquet')
    
    print(f"Dataset 1: {df1.num_rows} rows × {df1.num_columns} columns")
    print(f"Dataset 2: {df2.num_rows} rows × {df2.num_columns} columns")
    print(f"File sizes: {os.path.getsize('synthetic_df1.parquet')/1024/1024:.2f} MB + "
          f"{os.path.getsize('synthetic_df2.parquet')/1024/1024:.2f} MB")
    
    # Benchmark rdatacompy
    print("\n" + "-" * 80)
    print("Benchmarking rdatacompy (Rust)...")
    print("-" * 80)
    
    results_rust = benchmark_rdatacompy(df1, df2)
    
    print(f"✓ Time: {results_rust['time']:.3f} seconds")
    print(f"✓ Memory: {results_rust['memory']:.2f} MB")
    print(f"✓ Matches: {results_rust['matches']}")
    print(f"✓ Report length: {results_rust['report_length']} characters")
    
    # Benchmark datacompy if available
    print("\n" + "-" * 80)
    print("Benchmarking datacompy (Pandas)...")
    print("-" * 80)
    
    results_pandas = benchmark_datacompy(df1, df2)
    
    if results_pandas:
        print(f"✓ Time: {results_pandas['time']:.3f} seconds")
        print(f"✓ Memory: {results_pandas['memory']:.2f} MB")
        print(f"✓ Matches: {results_pandas['matches']}")
        print(f"✓ Report length: {results_pandas['report_length']} characters")
        
        # Comparison
        print("\n" + "=" * 80)
        print("Performance Comparison")
        print("=" * 80)
        
        speedup = results_pandas['time'] / results_rust['time']
        mem_reduction = (results_pandas['memory'] - results_rust['memory']) / results_pandas['memory'] * 100
        
        print(f"🚀 Speedup: {speedup:.2f}x faster")
        print(f"💾 Memory: {mem_reduction:.1f}% less memory used")
        
        if speedup > 1:
            print(f"\n✨ rdatacompy is {speedup:.2f}x FASTER than datacompy!")
        else:
            print(f"\n⚠️  datacompy is {1/speedup:.2f}x faster (optimization needed)")
    else:
        print("Skipping pandas comparison (datacompy not installed)")
    
    # Show sample of report
    print("\n" + "=" * 80)
    print("Sample Report Output (first 1000 chars):")
    print("=" * 80)
    
    compare = rdatacompy.Compare(df1, df2, join_columns='id', abs_tol=0.01)
    report = compare.report()
    print(report[:1000])
    if len(report) > 1000:
        print(f"\n... ({len(report) - 1000} more characters)")
    
    print("\n" + "=" * 80)

if __name__ == "__main__":
    run_benchmark()
