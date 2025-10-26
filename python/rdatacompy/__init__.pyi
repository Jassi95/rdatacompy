"""Type stubs for rdatacompy"""

from typing import Union, List, Set, Optional
import pyarrow as pa

class Compare:
    """
    Compare two PyArrow tables/dataframes.
    
    Parameters
    ----------
    df1 : pyarrow.Table or pyarrow.RecordBatch
        First dataframe to compare
    df2 : pyarrow.Table or pyarrow.RecordBatch
        Second dataframe to compare
    join_columns : str or list of str
        Column(s) to join on
    abs_tol : float, default 0.0
        Absolute tolerance for numeric comparison
    rel_tol : float, default 0.0
        Relative tolerance for numeric comparison
    df1_name : str, default "df1"
        Name for first dataframe in reports
    df2_name : str, default "df2"
        Name for second dataframe in reports
        
    Examples
    --------
    >>> import pyarrow as pa
    >>> import rdatacompy
    >>> df1 = pa.table({'id': [1, 2, 3], 'value': [10, 20, 30]})
    >>> df2 = pa.table({'id': [1, 2, 4], 'value': [10, 20, 40]})
    >>> compare = rdatacompy.Compare(df1, df2, join_columns='id')
    >>> print(compare.report())
    >>> print(compare.matches())
    False
    """
    
    def __init__(
        self,
        df1: Union[pa.Table, pa.RecordBatch],
        df2: Union[pa.Table, pa.RecordBatch],
        join_columns: Union[str, List[str]],
        abs_tol: float = 0.0,
        rel_tol: float = 0.0,
        df1_name: str = "df1",
        df2_name: str = "df2",
    ) -> None: ...
    
    def report(self) -> str:
        """
        Generate a comprehensive comparison report.
        
        Returns
        -------
        str
            A formatted text report showing dataframe and column summaries,
            row matching statistics, and details about differences.
        """
        ...
    
    def matches(self) -> bool:
        """
        Return True if dataframes match exactly.
        
        Returns
        -------
        bool
            True if all rows and columns match within tolerance, False otherwise.
        """
        ...
    
    @property
    def intersect_columns(self) -> Set[str]:
        """
        Columns present in both dataframes.
        
        Returns
        -------
        Set[str]
            Set of column names that exist in both dataframes.
        """
        ...
    
    @property
    def df1_unq_columns(self) -> Set[str]:
        """
        Columns only in df1.
        
        Returns
        -------
        Set[str]
            Set of column names that only exist in the first dataframe.
        """
        ...
    
    @property
    def df2_unq_columns(self) -> Set[str]:
        """
        Columns only in df2.
        
        Returns
        -------
        Set[str]
            Set of column names that only exist in the second dataframe.
        """
        ...
    
    @property
    def df1_unq_rows(self) -> Optional[pa.Table]:
        """
        Rows only in df1.
        
        Returns
        -------
        Optional[pyarrow.Table]
            PyArrow table containing rows that only exist in the first dataframe,
            or None if there are no unique rows.
        """
        ...
    
    @property
    def df2_unq_rows(self) -> Optional[pa.Table]:
        """
        Rows only in df2.
        
        Returns
        -------
        Optional[pyarrow.Table]
            PyArrow table containing rows that only exist in the second dataframe,
            or None if there are no unique rows.
        """
        ...
