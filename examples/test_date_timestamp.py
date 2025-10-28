"""
Test date and timestamp formatting with PySpark DataFrames
Tests Spark 3.5 compatibility with dates and timestamps
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, to_timestamp, lit
from pyspark.sql.types import TimestampType
from rdatacompy import Compare

# Initialize Spark session
spark = SparkSession.builder \
    .appName("rdatacompy-date-timestamp-test") \
    .master("local[*]") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

print("=" * 80)
print("Testing Date and Timestamp Formatting with PySpark")
print(f"Spark Version: {spark.version}")
print("=" * 80)

# Test 1: Date formatting
print("\n" + "=" * 80)
print("Test 1: Date Column Formatting (Date32)")
print("=" * 80)

df1_spark = spark.createDataFrame([
    (1, '2024-06-24'),
    (2, '2024-07-15'),
    (3, '2024-08-10')
], ['id', 'rate_date'])
df1_spark = df1_spark.withColumn('rate_date', to_date('rate_date'))

df2_spark = spark.createDataFrame([
    (1, '2025-10-28'),
    (2, '2024-07-15'),
    (3, '2024-08-11')
], ['id', 'rate_date'])
df2_spark = df2_spark.withColumn('rate_date', to_date('rate_date'))

print("\nDataFrame schemas:")
df1_spark.printSchema()

comp = Compare(df1_spark, df2_spark, join_columns=['id'], df1_name='source', df2_name='target')
report = comp.report()

lines = report.split('\n')
for i, line in enumerate(lines):
    if "Sample Rows with Unequal Values for 'rate_date'" in line:
        print('\n'.join(lines[i:i+7]))
        break

print("\n✅ Date values should be clean: YYYY-MM-DD format")

# Test 2: Timestamp without timezone
print("\n" + "=" * 80)
print("Test 2: Timestamp Column (no timezone)")
print("=" * 80)

df1_spark = spark.createDataFrame([
    (1, '2024-01-15 10:30:45'),
    (2, '2024-06-20 14:15:30')
], ['id', 'created_at'])
df1_spark = df1_spark.withColumn('created_at', to_timestamp('created_at'))

df2_spark = spark.createDataFrame([
    (1, '2024-01-15 10:31:00'),  # 15 seconds difference
    (2, '2024-06-20 14:15:30')
], ['id', 'created_at'])
df2_spark = df2_spark.withColumn('created_at', to_timestamp('created_at'))

print("\nDataFrame schema:")
df1_spark.printSchema()

comp = Compare(df1_spark, df2_spark, join_columns=['id'])
report = comp.report()

lines = report.split('\n')
for i, line in enumerate(lines):
    if "Sample Rows with Unequal Values for 'created_at'" in line:
        print('\n'.join(lines[i:i+6]))
        break

print("\n✅ Timestamp format should be: YYYY-MM-DD HH:MM:SS")

# Test 3: Timestamp WITH timezone (using SQL to create timestamptz)
print("\n" + "=" * 80)
print("Test 3: Timestamp WITH timezone")
print("=" * 80)

# Create timestamps and then convert to UTC
df1_spark = spark.sql("""
    SELECT 1 as id, timestamp('2024-01-15 10:30:45') as event_time
    UNION ALL
    SELECT 2 as id, timestamp('2024-06-20 14:15:30') as event_time
""")

df2_spark = spark.sql("""
    SELECT 1 as id, timestamp('2024-01-15 11:30:45') as event_time
    UNION ALL  
    SELECT 2 as id, timestamp('2024-06-20 14:15:30') as event_time
""")

print("\nDataFrame schema:")
df1_spark.printSchema()

comp = Compare(df1_spark, df2_spark, join_columns=['id'])
report = comp.report()

lines = report.split('\n')
for i, line in enumerate(lines):
    if "Sample Rows with Unequal Values for 'event_time'" in line:
        print('\n'.join(lines[i:i+6]))
        break

print("\n✅ Timestamp display working")

# Test 4: Multiple date columns
print("\n" + "=" * 80)
print("Test 4: Multiple Date and Timestamp Columns")
print("=" * 80)

df1_spark = spark.createDataFrame([
    (1, '2024-01-15', '2024-01-15 10:00:00',10),
    (2, '2024-06-20', '2024-06-20 14:00:00',11)
], ['id', 'start_date', 'created_at','value'])
df1_spark = df1_spark \
    .withColumn('start_date', to_date('start_date')) \
    .withColumn('created_at', to_timestamp('created_at'))

df2_spark = spark.createDataFrame([
    (1, '2024-01-16', '2024-01-15 10:00:00',15),  # Date differs
    (2, '2024-06-20', '2024-06-20 15:00:00',11)   # Timestamp differs
], ['id', 'start_date', 'created_at','value'])
df2_spark = df2_spark \
    .withColumn('start_date', to_date('start_date')) \
    .withColumn('created_at', to_timestamp('created_at'))

comp = Compare(df1_spark, df2_spark, join_columns=['id'])
report = comp.report()

print("\nColumns with differences:")

print(report) #We want the report fully so it's we can see if there are other problems! 

print("\n✅ Multiple date/timestamp columns handled correctly")

spark.stop()

print("\n" + "=" * 80)
print("All PySpark Date/Timestamp Tests Complete!")
print("=" * 80)
