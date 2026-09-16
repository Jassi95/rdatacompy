import sys

import pandas as pd
import pyarrow as pa

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
