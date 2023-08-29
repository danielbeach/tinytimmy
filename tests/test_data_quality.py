import polars as pl
import pytest
from tinytimmy.data_quality import DataQuality


def test_null_check():
    dq = DataQuality()
    df = pl.DataFrame({"a": [1, None, 3], "b": [None, 5, 6]})
    expected_result = pl.DataFrame({"check_type": ['null_check_a', 'null_check_b'], "check_value": [1, 1]})
    results = dq.null_check(df.lazy())
    assert expected_result.frame_equal(results)


def test_distinct_check():
    dq = DataQuality()
    df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    dq.dataframe = df.lazy()
    already_results = pl.DataFrame({"check_type": [''], "check_value": [1]})
    output = dq.distinct_check(already_results)
    output = output.filter(pl.col("check_type") != '')
    expected_result = pl.DataFrame({"check_type": ['total_count', 'distinct_count'], "check_value": [3, 3]})
    assert output.frame_equal(expected_result)
