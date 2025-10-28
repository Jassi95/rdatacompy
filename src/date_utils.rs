/// Utilities for date/timestamp formatting

/// Convert days since Unix epoch (1970-01-01) to (year, month, day)
/// 
/// Uses a proleptic Gregorian calendar algorithm for accurate date conversion.
pub fn days_to_date(days: i32) -> (i32, u32, u32) {
    // Civil calendar algorithm
    let z = days + 719468; // Adjust for epoch difference
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i32 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    (year, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_days_to_date() {
        // Test epoch (1970-01-01)
        assert_eq!(days_to_date(0), (1970, 1, 1));
        
        // Test some known dates
        assert_eq!(days_to_date(19898), (2024, 7, 9));
        assert_eq!(days_to_date(365), (1971, 1, 1));
        assert_eq!(days_to_date(-1), (1969, 12, 31));
    }
}
