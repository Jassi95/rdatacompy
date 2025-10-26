"""
Benchmark rdatacompy with large-scale dataset
Includes detailed performance metrics
"""

import pyarrow.parquet as pq
import rdatacompy
import time
import psutil
import os
from example_paths import LARGE_TARGET_DF, LARGE_COMPARISON_DF, LARGE_REPORT

def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def format_number(num):
    """Format large numbers with commas"""
    return f"{num:,}"

def benchmark_large_scale():
    print("=" * 80)
    print("RDataCompy Large-Scale Performance Benchmark")
    print("=" * 80)
    
    # Check if files exist
    if not LARGE_TARGET_DF.exists():
        print("\nDatasets not found! Run create_large_scenario.py first:")
        print("  python examples/create_large_scenario.py")
        return
    
    # Get file sizes
    target_size = LARGE_TARGET_DF.stat().st_size / 1024 / 1024
    comp_size = LARGE_COMPARISON_DF.stat().st_size / 1024 / 1024
    
    print(f"\nFile sizes:")
    print(f"  large_target_df.parquet:     {target_size:.2f} MB")
    print(f"  large_comparison_df.parquet: {comp_size:.2f} MB")
    print(f"  Total:                        {target_size + comp_size:.2f} MB")
    
    # Load datasets
    print("\n" + "-" * 80)
    print("Loading datasets...")
    print("-" * 80)
    
    mem_before_load = get_memory_usage()
    load_start = time.time()
    
    target_df = pq.read_table(str(LARGE_TARGET_DF))
    comparison_df = pq.read_table(str(LARGE_COMPARISON_DF))
    
    load_time = time.time() - load_start
    mem_after_load = get_memory_usage()
    mem_for_data = mem_after_load - mem_before_load
    
    print(f"✓ Loaded in {load_time:.3f} seconds")
    print(f"✓ Memory for data: {mem_for_data:.2f} MB")
    print(f"✓ Target DataFrame:     {format_number(target_df.num_rows)} rows × {target_df.num_columns} columns")
    print(f"✓ Comparison DataFrame: {format_number(comparison_df.num_rows)} rows × {comparison_df.num_columns} columns")
    
    total_cells = target_df.num_rows * target_df.num_columns + comparison_df.num_rows * comparison_df.num_columns
    print(f"✓ Total data points to process: {format_number(total_cells)}")
    
    # Benchmark rdatacompy
    print("\n" + "=" * 80)
    print("Running RDataCompy Comparison...")
    print("=" * 80)
    
    mem_before_compare = get_memory_usage()
    compare_start = time.time()
    
    compare = rdatacompy.Compare(
        target_df,
        comparison_df,
        join_columns='id',
        abs_tol=0.01,
        rel_tol=0.0,
        df1_name='target',
        df2_name='comparison'
    )
    
    compare_time = time.time() - compare_start
    mem_after_compare = get_memory_usage()
    mem_for_compare = mem_after_compare - mem_before_compare
    
    # Generate report
    print("\nGenerating report...")
    report_start = time.time()
    report = compare.report()
    report_time = time.time() - report_start
    
    total_time = compare_time + report_time
    
    # Performance metrics
    print("\n" + "=" * 80)
    print("PERFORMANCE METRICS")
    print("=" * 80)
    
    print("\n⏱️  TIME:")
    print(f"  Loading data:        {load_time:.3f} seconds")
    print(f"  Comparison:          {compare_time:.3f} seconds")
    print(f"  Report generation:   {report_time:.3f} seconds")
    print(f"  Total processing:    {total_time:.3f} seconds")
    
    print("\n💾 MEMORY:")
    print(f"  Data loaded:         {mem_for_data:.2f} MB")
    print(f"  Comparison overhead: {mem_for_compare:.2f} MB")
    print(f"  Total memory:        {mem_after_compare:.2f} MB")
    
    print("\n📊 THROUGHPUT:")
    cells_per_sec = total_cells / compare_time
    rows_per_sec = (target_df.num_rows + comparison_df.num_rows) / compare_time
    print(f"  Data points/sec:     {format_number(int(cells_per_sec))}")
    print(f"  Rows/sec:            {format_number(int(rows_per_sec))}")
    
    print("\n📈 COMPARISON RESULTS:")
    print(f"  DataFrames match:    {compare.matches()}")
    print(f"  Common columns:      {len(compare.intersect_columns)}")
    
    if compare.df1_unq_rows is not None:
        print(f"  Unique in target:    {format_number(compare.df1_unq_rows.num_rows)} rows")
    else:
        print(f"  Unique in target:    0 rows")
    
    if compare.df2_unq_rows is not None:
        print(f"  Unique in comparison: {format_number(compare.df2_unq_rows.num_rows)} rows")
    else:
        print(f"  Unique in comparison: 0 rows")
    
    # Show abbreviated report
    print("\n" + "=" * 80)
    print("COMPARISON REPORT (first 2000 characters):")
    print("=" * 80)
    print(report[:2000])
    if len(report) > 2000:
        print(f"\n... ({len(report) - 2000} more characters)")
    
    # Save full report
    with open(str(LARGE_REPORT), 'w') as f:
        f.write(report)
    print(f"\n✓ Full report saved to: {LARGE_REPORT.relative_to(LARGE_REPORT.parent.parent)}")
    
    print("\n" + "=" * 80)
    print("🚀 BENCHMARK COMPLETE!")
    print("=" * 80)
    print(f"\n⚡ Compared {format_number(total_cells)} data points in {total_time:.3f} seconds")
    print(f"   ({format_number(int(cells_per_sec))} cells/second)")
    print("=" * 80)

if __name__ == "__main__":
    benchmark_large_scale()
