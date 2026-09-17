import pyarrow as pa

from rdatacompy import Compare


def test_compare_includes_all_arrow_batches():
    first_batch = pa.record_batch(
        {"id": range(12_000), "value": range(12_000)}
    )
    second_batch_left = pa.record_batch(
        {"id": range(12_000, 24_000), "value": range(12_000, 24_000)}
    )
    second_batch_right = pa.record_batch(
        {
            "id": range(12_000, 24_000),
            "value": [*range(12_000, 23_999), -1],
        }
    )

    comparison = Compare(
        pa.Table.from_batches([first_batch, second_batch_left]),
        pa.Table.from_batches([first_batch, second_batch_right]),
        join_columns=["id"],
    )

    report = comparison.report()

    assert not comparison.matches()
    assert "Number of rows in common: 24000" in report
    assert "Number of rows in df1 but not in df2: 0" in report
    assert "Number of rows in df2 but not in df1: 0" in report
    assert "Total number of values which compare unequal: 1" in report