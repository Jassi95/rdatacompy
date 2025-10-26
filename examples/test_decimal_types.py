"""
Test decimal type support - comparing DECIMAL columns with different precision/scale.

This simulates a real-world scenario where data types change during migration:
- Source system uses DECIMAL(28,12) for high precision
- Target system uses DECIMAL(18,6) for standard precision
- Should still be able to compare them!
"""

import pyarrow as pa
from rdatacompy import Compare
from decimal import Decimal

print("=" * 80)
print("Testing Decimal Type Support")
print("=" * 80)

# Create test data with different decimal precisions
# Simulating a data migration scenario
target_data = {
    'id': [1, 2, 3, 4, 5],
    # DECIMAL(28,12) - high precision (e.g., from Spark)
    'amount_high_prec': [
        Decimal('123.456789012345'),
        Decimal('999.999999999999'),
        Decimal('0.000000000001'),
        Decimal('1234567890.123456789012'),
        Decimal('42.123456789012')
    ],
    # Regular float
    'amount_float': [123.45, 999.99, 0.000001, 1234567890.12, 42.12]
}

comparison_data = {
    'id': [1, 2, 3, 4, 5],
    # DECIMAL(18,6) - standard precision (e.g., from PostgreSQL)
    'amount_high_prec': [
        Decimal('123.456789'),      # Same within 6 decimals
        Decimal('999.999998'),      # Slightly different
        Decimal('0.000001'),        # Different precision
        Decimal('1234567890.123457'), # Slightly different
        Decimal('42.123457')        # Slightly different
    ],
    # Regular float
    'amount_float': [123.45, 999.99, 0.000001, 1234567890.12, 42.12]
}

# Convert to PyArrow with decimal types
target_schema = pa.schema([
    ('id', pa.int64()),
    ('amount_high_prec', pa.decimal128(28, 12)),  # High precision
    ('amount_float', pa.float64())
])

comparison_schema = pa.schema([
    ('id', pa.int64()),
    ('amount_high_prec', pa.decimal128(18, 6)),   # Lower precision
    ('amount_float', pa.float64())
])

target_table = pa.table(target_data, schema=target_schema)
comparison_table = pa.table(comparison_data, schema=comparison_schema)

print("\nTarget DataFrame Schema:")
print(target_table.schema)
print("\nComparison DataFrame Schema:")
print(comparison_table.schema)

print("\n" + "=" * 80)
print("Test 1: Exact comparison (no tolerance) - should find differences")
print("=" * 80)

comp = Compare(
    target_table,
    comparison_table,
    join_columns=['id'],
    abs_tol=0.0,
    rel_tol=0.0
)

print(comp.report())

print("\n" + "=" * 80)
print("Test 2: With tolerance - should match most values")
print("=" * 80)

comp_with_tol = Compare(
    target_table,
    comparison_table,
    join_columns=['id'],
    abs_tol=0.00001,  # 1e-5 tolerance
    rel_tol=0.0
)

print(comp_with_tol.report())

print("\n" + "=" * 80)
print("Test 3: Cross-type comparison (DECIMAL vs FLOAT)")
print("=" * 80)

# Create mixed types - comparing decimal with float
mixed_target = pa.table({
    'id': [1, 2, 3],
    'value': pa.array([Decimal('123.45'), Decimal('999.99'), Decimal('42.12')], 
                      type=pa.decimal128(10, 2))
}, schema=pa.schema([
    ('id', pa.int64()),
    ('value', pa.decimal128(10, 2))
]))

mixed_comparison = pa.table({
    'id': [1, 2, 3],
    'value': [123.45, 999.99, 42.12]  # Float64
})

print("\nTarget type: DECIMAL(10,2)")
print("Comparison type: FLOAT64")

comp_mixed = Compare(
    mixed_target,
    mixed_comparison,
    join_columns=['id'],
    abs_tol=0.01
)

print(comp_mixed.report())

print("\n" + "=" * 80)
print("Test 4: Very large decimals (DECIMAL256)")
print("=" * 80)

# Test with very large decimal values
large_target = pa.table({
    'id': [1, 2],
    'big_value': pa.array([
        Decimal('123456789012345678901234567890.123456789012'),
        Decimal('999999999999999999999999999999.999999999999')
    ], type=pa.decimal256(50, 12))
})

large_comparison = pa.table({
    'id': [1, 2],
    'big_value': pa.array([
        Decimal('123456789012345678901234567890.123456'),  # Less precision
        Decimal('999999999999999999999999999999.999999')
    ], type=pa.decimal256(50, 6))
})

print("\nComparing DECIMAL256(50,12) vs DECIMAL256(50,6)")

comp_large = Compare(
    large_target,
    large_comparison,
    join_columns=['id'],
    abs_tol=0.001
)

print(comp_large.report())

print("\n" + "=" * 80)
print("✓ All decimal tests completed!")
print("=" * 80)
