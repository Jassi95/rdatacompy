"""
Test PySpark DataFrame support in RDataCompy.

This demonstrates that you can now pass PySpark DataFrames directly
to the comparison engine, without manually converting to PyArrow.
"""

from pyspark.sql import SparkSession
from rdatacompy import Compare
from decimal import Decimal

print("=" * 80)
print("RDataCompy - PySpark DataFrame Support")
print("=" * 80)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RDataCompy PySpark Test") \
    .master("local[*]") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

print("\n✓ Spark session created")
print(f"  Spark version: {spark.version}")

# Test 1: Basic integer comparison
print("\n" + "=" * 80)
print("Test 1: Basic PySpark DataFrame Comparison")
print("=" * 80)

df1_spark = spark.createDataFrame([
    (1, 100, "Alice"),
    (2, 200, "Bob"),
    (3, 300, "Charlie"),
    (4, 400, "David")
], ["id", "amount", "name"])

df2_spark = spark.createDataFrame([
    (1, 100, "Alice"),
    (2, 201, "Bob"),      # Different amount
    (3, 300, "Charlie"),
    (4, 400, "David")
], ["id", "amount", "name"])

print("\nDataFrame 1:")
df1_spark.show()

print("DataFrame 2:")
df2_spark.show()

# Compare directly - no manual conversion needed!
comp = Compare(
    df1_spark,
    df2_spark,
    join_columns=['id'],
    df1_name="source",
    df2_name="target"
)

print(comp.report())

# Test 2: Decimal types (common in Spark)
print("\n" + "=" * 80)
print("Test 2: Spark DECIMAL Types")
print("=" * 80)

from pyspark.sql.types import StructType, StructField, IntegerType, DecimalType

schema1 = StructType([
    StructField("id", IntegerType(), False),
    StructField("price", DecimalType(28, 12), False),  # High precision
    StructField("tax", DecimalType(10, 4), False)
])

schema2 = StructType([
    StructField("id", IntegerType(), False),
    StructField("price", DecimalType(18, 6), False),   # Lower precision
    StructField("tax", DecimalType(10, 4), False)
])

df1_decimal = spark.createDataFrame([
    (1, Decimal('123.456789012345'), Decimal('12.3456')),
    (2, Decimal('999.999999999999'), Decimal('99.9999')),
    (3, Decimal('42.123456789012'), Decimal('4.2123'))
], schema=schema1)

df2_decimal = spark.createDataFrame([
    (1, Decimal('123.456789'), Decimal('12.3456')),  # Same within precision
    (2, Decimal('999.999998'), Decimal('99.9999')),  # Slightly different
    (3, Decimal('42.123457'), Decimal('4.2123'))     # Slightly different
], schema=schema2)

print("\nDataFrame 1 Schema:")
df1_decimal.printSchema()

print("\nDataFrame 2 Schema:")
df2_decimal.printSchema()

print("\nDataFrame 1 Data:")
df1_decimal.show()

print("DataFrame 2 Data:")
df2_decimal.show()

# Compare with tolerance
comp_decimal = Compare(
    df1_decimal,
    df2_decimal,
    join_columns=['id'],
    abs_tol=0.00001,
    df1_name="high_precision",
    df2_name="standard_precision"
)

print(comp_decimal.report())

# Test 3: Large dataset performance
print("\n" + "=" * 80)
print("Test 3: Large Spark DataFrame Performance")
print("=" * 80)

import time

# Create a larger dataset
n_rows = 100_000
print(f"\nCreating Spark DataFrames with {n_rows:,} rows...")

# Create data with multiple columns
df1_large = spark.range(n_rows).selectExpr(
    "id",
    "id * 2 as value1",
    "id * 3 as value2",
    "id * 4.5 as value3",
    "concat('user_', id) as name"
)

df2_large = spark.range(n_rows).selectExpr(
    "id",
    "id * 2 as value1",
    "id * 3 + (CASE WHEN id < 100 THEN 1 ELSE 0 END) as value2",  # 100 differences
    "id * 4.5 as value3",
    "concat('user_', id) as name"
)

print(f"✓ Created DataFrames: {df1_large.count():,} rows × {len(df1_large.columns)} columns")

start_time = time.time()

comp_large = Compare(
    df1_large,
    df2_large,
    join_columns=['id'],
    abs_tol=0.01,
    df1_name="source_large",
    df2_name="target_large"
)

comparison_time = time.time() - start_time

print(f"\n✓ Comparison completed in {comparison_time:.3f} seconds")
print(f"  Throughput: {n_rows * len(df1_large.columns) / comparison_time:,.0f} cells/second")

# Get summary
report = comp_large.report()
# Print just the summary part
lines = report.split('\n')
for i, line in enumerate(lines):
    if 'Row Summary' in line:
        print('\n'.join(lines[i:i+15]))
        break

print("\n" + "=" * 80)
print("Test 4: Mixed DataFrame Types")
print("=" * 80)

# Compare Spark DataFrame with PyArrow Table
import pyarrow as pa

spark_df = spark.createDataFrame([
    (1, 10.5),
    (2, 20.5),
    (3, 30.5)
], ["id", "value"])

arrow_table = pa.table({
    'id': [1, 2, 3],
    'value': [10.5, 20.5, 30.5]
})

print("\nComparing PySpark DataFrame with PyArrow Table...")

comp_mixed = Compare(
    spark_df,
    arrow_table,
    join_columns=['id'],
    df1_name="spark",
    df2_name="arrow"
)

print(comp_mixed.report())

# Cleanup
spark.stop()

print("\n" + "=" * 80)
print("✓ All PySpark tests completed successfully!")
print("=" * 80)
print("\nKey takeaways:")
print("  • PySpark DataFrames work directly with RDataCompy")
print("  • Automatic conversion to PyArrow under the hood")
print("  • Full support for Spark DECIMAL types")
print("  • Can mix PySpark with PyArrow Tables")
print("  • Fast performance maintained")
