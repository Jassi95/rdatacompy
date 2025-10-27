"""
RDataCompy - Lightning-fast dataframe comparison for PyArrow

This package provides high-performance dataframe comparison capabilities,
implemented in Rust with Python bindings.
"""

import pyarrow as pa
from typing import Union, List, Optional

# Import the Rust implementation
from ._rdatacompy import Compare as _RustCompare

__version__ = "0.1.9"


def _to_arrow_table(df, name: str = "dataframe") -> pa.Table:
    """
    Convert various dataframe types to PyArrow Table.
    
    Supports:
    - PyArrow Table
    - PyArrow RecordBatch
    - PySpark DataFrame (3.5+ via toPandas, 4.0+ via toArrow)
    - Pandas DataFrame
    - Polars DataFrame
    
    Parameters
    ----------
    df : DataFrame-like
        The dataframe to convert
    name : str
        Name for error messages
        
    Returns
    -------
    pa.Table
        PyArrow Table
    """
    # Already a PyArrow Table
    if isinstance(df, pa.Table):
        return df
    
    # PyArrow RecordBatch
    if isinstance(df, pa.RecordBatch):
        return pa.Table.from_batches([df])
    
    # PySpark DataFrame
    try:
        from pyspark.sql import DataFrame as SparkDataFrame
        if isinstance(df, SparkDataFrame):
            # Try to use toArrow() first (Spark 4.0+)
            if hasattr(df, 'toArrow'):
                try:
                    return df.toArrow()
                except Exception:
                    # Fall back to toPandas if toArrow fails
                    pass
            
            # Fallback for Spark 3.5 and earlier: convert via Pandas
            # This requires enabling Arrow-based columnar data transfers
            try:
                pandas_df = df.toPandas()
                return pa.Table.from_pandas(pandas_df)
            except Exception as e:
                raise RuntimeError(
                    f"Failed to convert PySpark DataFrame to PyArrow. "
                    f"For Spark 3.5, ensure 'spark.sql.execution.arrow.pyspark.enabled' is set to 'true'. "
                    f"Error: {e}"
                )
    except ImportError:
        pass  # PySpark not installed
    
    # Pandas DataFrame
    try:
        import pandas as pd
        if isinstance(df, pd.DataFrame):
            return pa.Table.from_pandas(df)
    except ImportError:
        pass  # Pandas not installed
    
    # Polars DataFrame
    try:
        import polars as pl
        if isinstance(df, pl.DataFrame):
            return df.to_arrow()
    except ImportError:
        pass  # Polars not installed
    
    # If we get here, unsupported type
    raise TypeError(
        f"{name} must be a PyArrow Table, RecordBatch, PySpark DataFrame, "
        f"Pandas DataFrame, or Polars DataFrame. Got: {type(df)}"
    )


class Compare:
    """
    Compare two dataframes for equality.
    
    Supports multiple dataframe types:
    - PyArrow Table/RecordBatch
    - PySpark DataFrame (3.5+ via toPandas, 4.0+ via .toArrow())
    - Pandas DataFrame (converted via pa.Table.from_pandas())
    - Polars DataFrame (converted via .to_arrow())
    
    All comparisons are done on PyArrow Tables for maximum performance.
    
    Parameters
    ----------
    df1 : DataFrame-like
        First dataframe to compare
    df2 : DataFrame-like
        Second dataframe to compare
    join_columns : List[str]
        Column names to use as join keys
    abs_tol : float, optional
        Absolute tolerance for numeric comparisons (default: 0.0)
    rel_tol : float, optional
        Relative tolerance for numeric comparisons (default: 0.0)
    df1_name : str, optional
        Name for first dataframe in reports (default: "df1")
    df2_name : str, optional
        Name for second dataframe in reports (default: "df2")
        
    Examples
    --------
    Compare PyArrow tables:
    
    >>> import pyarrow as pa
    >>> from rdatacompy import Compare
    >>> t1 = pa.table({'id': [1, 2], 'val': [10, 20]})
    >>> t2 = pa.table({'id': [1, 2], 'val': [10, 21]})
    >>> comp = Compare(t1, t2, join_columns=['id'])
    >>> print(comp.report())
    
    Compare PySpark DataFrames:
    
    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.getOrCreate()
    >>> df1 = spark.createDataFrame([(1, 10), (2, 20)], ['id', 'val'])
    >>> df2 = spark.createDataFrame([(1, 10), (2, 21)], ['id', 'val'])
    >>> comp = Compare(df1, df2, join_columns=['id'])
    >>> print(comp.report())
    
    Compare Pandas DataFrames:
    
    >>> import pandas as pd
    >>> df1 = pd.DataFrame({'id': [1, 2], 'val': [10, 20]})
    >>> df2 = pd.DataFrame({'id': [1, 2], 'val': [10, 21]})
    >>> comp = Compare(df1, df2, join_columns=['id'], abs_tol=0.01)
    >>> print(comp.report())
    """
    
    def __init__(
        self,
        df1,
        df2,
        join_columns: List[str],
        abs_tol: float = 0.0,
        rel_tol: float = 0.0,
        df1_name: str = "df1",
        df2_name: str = "df2"
    ):
        # Convert both dataframes to PyArrow Tables
        arrow_df1 = _to_arrow_table(df1, "df1")
        arrow_df2 = _to_arrow_table(df2, "df2")
        
        # Create the Rust comparison object
        self._compare = _RustCompare(
            arrow_df1,
            arrow_df2,
            join_columns,
            abs_tol,
            rel_tol,
            df1_name,
            df2_name
        )
    
    def report(self) -> str:
        """
        Generate a human-readable comparison report.
        
        Returns
        -------
        str
            Formatted report showing differences between dataframes
        """
        return self._compare.report()
    
    def matches(self) -> bool:
        """
        Check if the dataframes match completely.
        
        Returns
        -------
        bool
            True if dataframes are equal, False otherwise
        """
        return self._compare.matches()
    
    def intersect_columns(self) -> List[str]:
        """
        Get the list of columns that exist in both dataframes.
        
        Returns
        -------
        List[str]
            Column names present in both dataframes
        """
        return list(self._compare.intersect_columns)
    
    def df1_unq_columns(self) -> List[str]:
        """
        Get columns that only exist in df1.
        
        Returns
        -------
        List[str]
            Column names unique to df1
        """
        return list(self._compare.df1_unq_columns)
    
    def df2_unq_columns(self) -> List[str]:
        """
        Get columns that only exist in df2.
        
        Returns
        -------
        List[str]
            Column names unique to df2
        """
        return list(self._compare.df2_unq_columns)


__all__ = ["Compare"]

