"""
Example: Using rdatacompy with PySpark 3.5

This demonstrates how to use rdatacompy with PySpark 3.5.x, which doesn't have the .toArrow() method.
Instead, rdatacompy will automatically fall back to using .toPandas() + PyArrow conversion.
"""

from pyspark.sql import SparkSession
from rdatacompy import Compare
from decimal import Decimal

print("=" * 80)
print("RDataCompy - PySpark 3.5 Compatibility Example")
print("=" * 80)

# Important: For Spark 3.5, enable Arrow-based columnar data transfers for better performance
spark = SparkSession.builder \
    .appName("RDataCompy Spark 3.5 Example") \
    .master("local[*]") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
    .getOrCreate()

print(f"\n✓ Spark session created")
print(f"  Spark version: {spark.version}")
print(f"  Arrow enabled: {spark.conf.get('spark.sql.execution.arrow.pyspark.enabled')}")

# Create sample DataFrames
print("\n" + "=" * 80)
print("Creating Sample DataFrames")
print("=" * 80)

df1 = spark.createDataFrame([
    (1, 100.50, "Alice", "2024-01-01"),
    (2, 200.75, "Bob", "2024-01-02"), 
    (3, 300.25, "Charlie", "2024-01-03"),
    (4, 400.00, "David", "2024-01-04")
], ["id", "amount", "name", "date"])

df2 = spark.createDataFrame([
    (1, 100.50, "Alice", "2024-01-01"),
    (2, 201.00, "Bob", "2024-01-02"),    # Different amount
    (3, 300.25, "Chuck", "2024-01-03"),   # Different name
    (4, 400.00, "David", "2024-01-04")
], ["id", "amount", "name", "date"])

print("\nDataFrame 1:")
df1.show()

print("DataFrame 2:")
df2.show()

# Compare DataFrames
print("\n" + "=" * 80)
print("Comparing DataFrames")
print("=" * 80)

print("Note: Since this is Spark 3.5, rdatacompy will automatically use:")
print("  1. Try df.toArrow() (not available in 3.5)")
print("  2. Fall back to df.toPandas() + pa.Table.from_pandas()")
print("  3. This requires Arrow to be enabled (done above)\n")

try:
    compare = Compare(
        df1,
        df2,
        join_columns=['id'],
        abs_tol=0.01,
        df1_name='source',
        df2_name='target'
    )
    
    print("✓ Comparison successful!")
    print("\nComparison Report:")
    print("-" * 40)
    print(compare.report())
    
except Exception as e:
    print(f"✗ Error during comparison: {e}")
    print("\nTroubleshooting tips:")
    print("1. Make sure pandas is installed: pip install pandas")
    print("2. Make sure pyarrow is installed: pip install pyarrow")
    print("3. Ensure Arrow is enabled in Spark config (see above)")

# Test with larger dataset
print("\n" + "=" * 80)
print("Performance Test with Larger Dataset")
print("=" * 80)

import time

# Create larger dataset
n_rows = 10000
print(f"Creating DataFrames with {n_rows:,} rows...")

df1_large = spark.range(n_rows).selectExpr(
    "id",
    "id * 1.5 as value1", 
    "id * 2.3 as value2",
    "concat('user_', id) as username"
)

df2_large = spark.range(n_rows).selectExpr(
    "id",
    "id * 1.5 + (CASE WHEN id < 100 THEN 0.01 ELSE 0 END) as value1",  # 100 small diffs
    "id * 2.3 as value2",
    "concat('user_', id) as username"
)

print("✓ Large DataFrames created")

start_time = time.time()

try:
    compare_large = Compare(
        df1_large,
        df2_large,
        join_columns=['id'],
        abs_tol=0.005,
        df1_name='source_large',
        df2_name='target_large'
    )
    
    conversion_time = time.time() - start_time
    print(f"✓ Conversion completed in {conversion_time:.3f} seconds")
    print(f"  Throughput: {n_rows / conversion_time:,.0f} rows/second")
    
    # Get summary stats
    matches = compare_large.matches()
    print(f"\nQuick Results:")
    print(f"  DataFrames match: {matches}")
    print(f"  Common columns: {len(compare_large.intersect_columns())}")
    
except Exception as e:
    print(f"✗ Error with large dataset: {e}")

# Cleanup
spark.stop()

print("\n" + "=" * 80)
print("✓ Spark 3.5 compatibility test completed!")
print("=" * 80)

print("\nKey points for Spark 3.5 users:")
print("1. ✓ rdatacompy works with Spark 3.5.x")
print("2. ✓ Automatic fallback from toArrow() to toPandas()")
print("3. ✓ Enable Arrow for better performance:")
print("     spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', 'true')")
print("4. ✓ All functionality works the same as Spark 4.0+")
print("5. ⚠ Slightly slower than native toArrow() but still fast")