use arrow::datatypes::DataType;

/// Convert Arrow DataType to a readable string for reporting
pub fn arrow_type_to_string(data_type: &DataType) -> String {
    match data_type {
        DataType::Int8 => "int8".to_string(),
        DataType::Int16 => "int16".to_string(),
        DataType::Int32 => "int32".to_string(),
        DataType::Int64 => "int64".to_string(),
        DataType::UInt8 => "uint8".to_string(),
        DataType::UInt16 => "uint16".to_string(),
        DataType::UInt32 => "uint32".to_string(),
        DataType::UInt64 => "uint64".to_string(),
        DataType::Float32 => "float32".to_string(),
        DataType::Float64 => "float64".to_string(),
        DataType::Decimal128(precision, scale) => format!("decimal128({}, {})", precision, scale),
        DataType::Decimal256(precision, scale) => format!("decimal256({}, {})", precision, scale),
        DataType::Utf8 => "string".to_string(),
        DataType::LargeUtf8 => "large_string".to_string(),
        DataType::Boolean => "bool".to_string(),
        DataType::Date32 => "date32".to_string(),
        DataType::Date64 => "date64".to_string(),
        DataType::Timestamp(unit, tz) => {
            if let Some(tz) = tz {
                format!("timestamp[{:?}, {}]", unit, tz)
            } else {
                format!("timestamp[{:?}]", unit)
            }
        }
        other => format!("{:?}", other),
    }
}

/// Check if two data types are compatible for comparison
pub fn types_compatible(type1: &DataType, type2: &DataType) -> bool {
    // Exact match
    if type1 == type2 {
        return true;
    }
    
    // All numeric types are compatible with each other (for data migration scenarios)
    if is_numeric_type(type1) && is_numeric_type(type2) {
        return true;
    }
    
    // String types are compatible
    match (type1, type2) {
        (DataType::Utf8, DataType::LargeUtf8) | (DataType::LargeUtf8, DataType::Utf8) => true,
        _ => false,
    }
}

/// Check if a data type is numeric (including decimals)
pub fn is_numeric_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
        DataType::Float32 | DataType::Float64 |
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
    )
}
