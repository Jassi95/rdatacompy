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


def test_to_arrow_table_propagates_toarrow_failures(monkeypatch):
    class FakeSparkDataFrame:
        def toArrow(self):
            raise RuntimeError("Arrow collection failed")

        def toPandas(self):
            raise AssertionError("toPandas should not be called")

    fake_sql_module = type("FakeSQLModule", (), {"DataFrame": FakeSparkDataFrame})
    fake_pyspark_module = type("FakePySparkModule", (), {"sql": fake_sql_module})

    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)

    with pytest.raises(RuntimeError, match="Arrow collection failed"):
        _to_arrow_table(FakeSparkDataFrame())


def test_to_arrow_table_raises_helpful_error_for_distutils(monkeypatch):
    class FakeSparkDataFrame:
        def toPandas(self):
            raise ModuleNotFoundError("No module named 'distutils'")

    fake_sql_module = type("FakeSQLModule", (), {"DataFrame": FakeSparkDataFrame})
    fake_pyspark_module = type("FakePySparkModule", (), {"sql": fake_sql_module})

    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)

    with pytest.raises(RuntimeError, match="PySpark fallback conversion hit a missing 'distutils' dependency"):
        _to_arrow_table(FakeSparkDataFrame())


def test_to_arrow_table_raises_helpful_error_for_wrapped_distutils(monkeypatch):
    class FakeSparkDataFrame:
        def toPandas(self):
            try:
                raise ModuleNotFoundError("No module named 'distutils'")
            except ModuleNotFoundError as exc:
                raise RuntimeError("wrapped") from exc

    fake_sql_module = type("FakeSQLModule", (), {"DataFrame": FakeSparkDataFrame})
    fake_pyspark_module = type("FakePySparkModule", (), {"sql": fake_sql_module})

    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)

    with pytest.raises(RuntimeError, match="PySpark fallback conversion hit a missing 'distutils' dependency"):
        _to_arrow_table(FakeSparkDataFrame())


def test_to_arrow_table_propagates_other_module_not_found_errors(monkeypatch):
    class FakeSparkDataFrame:
        def toPandas(self):
            raise ModuleNotFoundError("No module named 'not_distutils'")

    fake_sql_module = type("FakeSQLModule", (), {"DataFrame": FakeSparkDataFrame})
    fake_pyspark_module = type("FakePySparkModule", (), {"sql": fake_sql_module})

    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)

    with pytest.raises(ModuleNotFoundError, match="not_distutils"):
        _to_arrow_table(FakeSparkDataFrame())


def test_to_arrow_table_reports_topandas_fallback_failures(monkeypatch):
    class FakeSparkDataFrame:
        def toPandas(self):
            raise ValueError("boom")

    fake_sql_module = type("FakeSQLModule", (), {"DataFrame": FakeSparkDataFrame})
    fake_pyspark_module = type("FakePySparkModule", (), {"sql": fake_sql_module})

    monkeypatch.setitem(sys.modules, "pyspark", fake_pyspark_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)

    with pytest.raises(RuntimeError, match="toPandas\\(\\) fallback"):
        _to_arrow_table(FakeSparkDataFrame())
