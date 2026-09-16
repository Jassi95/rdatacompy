import sys

import pandas as pd
import pyarrow as pa
import pytest

from rdatacompy import _to_arrow_table


def test_to_arrow_table_uses_pandas_for_pre_4_spark(monkeypatch):
    class FakeSparkDataFrame:
        def toPandas(self):
            return pd.DataFrame({"id": [1], "value": ["a"]})

    fake_sql_module = type("FakeSQLModule", (), {"DataFrame": FakeSparkDataFrame})
    fake_pyspark_module = type("FakePySparkModule", (), {"sql": fake_sql_module})

    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)

    table = _to_arrow_table(FakeSparkDataFrame())

    assert isinstance(table, pa.Table)
    assert table.to_pydict() == {"id": [1], "value": ["a"]}


def test_to_arrow_table_falls_back_when_toarrow_fails(monkeypatch):
    class FakeSparkDataFrame:
        def toArrow(self):
            raise RuntimeError("toArrow unavailable")

        def toPandas(self):
            return pd.DataFrame({"id": [1], "value": ["fallback"]})

    fake_sql_module = type("FakeSQLModule", (), {"DataFrame": FakeSparkDataFrame})
    fake_pyspark_module = type("FakePySparkModule", (), {"sql": fake_sql_module})

    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)

    table = _to_arrow_table(FakeSparkDataFrame())

    assert isinstance(table, pa.Table)
    assert table.to_pydict() == {"id": [1], "value": ["fallback"]}


def test_to_arrow_table_raises_helpful_error_for_distutils(monkeypatch):
    class FakeSparkDataFrame:
        def toPandas(self):
            raise ModuleNotFoundError("No module named 'distutils'")

    fake_sql_module = type("FakeSQLModule", (), {"DataFrame": FakeSparkDataFrame})
    fake_pyspark_module = type("FakePySparkModule", (), {"sql": fake_sql_module})

    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)

    with pytest.raises(RuntimeError, match="PySpark <4.0 may require 'distutils'"):
        _to_arrow_table(FakeSparkDataFrame())


def test_to_arrow_table_raises_helpful_error_after_toarrow_failure(monkeypatch):
    class FakeSparkDataFrame:
        def toArrow(self):
            raise RuntimeError("toArrow unavailable")

        def toPandas(self):
            raise ModuleNotFoundError("No module named 'distutils'")

    fake_sql_module = type("FakeSQLModule", (), {"DataFrame": FakeSparkDataFrame})
    fake_pyspark_module = type("FakePySparkModule", (), {"sql": fake_sql_module})

    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)

    with pytest.raises(RuntimeError, match="PySpark <4.0 may require 'distutils'"):
        _to_arrow_table(FakeSparkDataFrame())
