/// Handles numeric comparison with absolute and relative tolerance
#[derive(Debug, Clone, Copy)]
pub struct ToleranceChecker {
    pub abs_tol: f64,
    pub rel_tol: f64,
}

impl ToleranceChecker {
    pub fn new(abs_tol: f64, rel_tol: f64) -> Self {
        Self { abs_tol, rel_tol }
    }
    
    /// Check if two values are within tolerance
    pub fn within_tolerance(&self, a: f64, b: f64) -> bool {
        // Handle NaN and infinity
        if a.is_nan() && b.is_nan() {
            return true;
        }
        if a.is_nan() || b.is_nan() {
            return false;
        }
        if a.is_infinite() && b.is_infinite() {
            return a.is_sign_positive() == b.is_sign_positive();
        }
        if a.is_infinite() || b.is_infinite() {
            return false;
        }
        
        let diff = (a - b).abs();
        
        // Check absolute tolerance
        if diff <= self.abs_tol {
            return true;
        }
        
        // Check relative tolerance
        if self.rel_tol > 0.0 {
            let relative_diff = diff / b.abs();
            if relative_diff <= self.rel_tol {
                return true;
            }
        }
        
        false
    }
    
    /// Calculate the absolute difference between two values
    pub fn difference(&self, a: f64, b: f64) -> f64 {
        (a - b).abs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_exact_match() {
        let checker = ToleranceChecker::new(0.0, 0.0);
        assert!(checker.within_tolerance(1.0, 1.0));
        assert!(!checker.within_tolerance(1.0, 1.1));
    }
    
    #[test]
    fn test_absolute_tolerance() {
        let checker = ToleranceChecker::new(0.1, 0.0);
        assert!(checker.within_tolerance(1.0, 1.05));
        assert!(checker.within_tolerance(1.0, 0.95));
        assert!(!checker.within_tolerance(1.0, 1.2));
    }
    
    #[test]
    fn test_relative_tolerance() {
        let checker = ToleranceChecker::new(0.0, 0.01); // 1% tolerance
        assert!(checker.within_tolerance(100.0, 100.5));
        assert!(!checker.within_tolerance(100.0, 102.0));
    }
    
    #[test]
    fn test_nan_handling() {
        let checker = ToleranceChecker::new(0.0, 0.0);
        assert!(checker.within_tolerance(f64::NAN, f64::NAN));
        assert!(!checker.within_tolerance(f64::NAN, 1.0));
        assert!(!checker.within_tolerance(1.0, f64::NAN));
    }
    
    #[test]
    fn test_infinity_handling() {
        let checker = ToleranceChecker::new(0.0, 0.0);
        assert!(checker.within_tolerance(f64::INFINITY, f64::INFINITY));
        assert!(checker.within_tolerance(f64::NEG_INFINITY, f64::NEG_INFINITY));
        assert!(!checker.within_tolerance(f64::INFINITY, f64::NEG_INFINITY));
        assert!(!checker.within_tolerance(f64::INFINITY, 1.0));
    }
}
