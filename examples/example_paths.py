"""
Path utilities for RDataCompy examples.

Provides consistent paths for test data and reports across all examples.
"""

import os
from pathlib import Path

# Get the project root (parent of examples/)
PROJECT_ROOT = Path(__file__).parent.parent

# Define standard directories
EXTRAS_DIR = PROJECT_ROOT / "extras"
TEST_DATA_DIR = EXTRAS_DIR / "test_data"
REPORTS_DIR = EXTRAS_DIR / "reports"

# Ensure directories exist
TEST_DATA_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def get_test_data_path(filename: str) -> Path:
    """Get path for test data file."""
    return TEST_DATA_DIR / filename


def get_report_path(filename: str) -> Path:
    """Get path for report file."""
    return REPORTS_DIR / filename


# Standard test data files
SYNTHETIC_DF1 = get_test_data_path("synthetic_df1.parquet")
SYNTHETIC_DF2 = get_test_data_path("synthetic_df2.parquet")
SYNTHETIC_WIDE = get_test_data_path("synthetic_wide.parquet")

TARGET_DF = get_test_data_path("target_df.parquet")
COMPARISON_DF = get_test_data_path("comparison_df.parquet")

LARGE_TARGET_DF = get_test_data_path("large_target_df.parquet")
LARGE_COMPARISON_DF = get_test_data_path("large_comparison_df.parquet")

# Standard reports
LARGE_REPORT = get_report_path("large_comparison_report.txt")
