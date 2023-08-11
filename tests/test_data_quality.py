import polars as pl
import pytest
from tinytimmy.data_quality import DataQuality


def test_null_check():
    dq = DataQuality()

    # No null values test
    df = pl.DataFrame({
        "a": [1, 2, 3],
        "b": [4, 5, 6]
    })

    expected_result = pl.DataFrame({})
    assert dq.null_check(df.lazy()) == expected_result

    # Some null values test
    df = pl.DataFrame({
        "a": [1, None, 3],
        "b": [None, 5, 6]
    })
    
    expected_result = pl.DataFrame({
        "a_null_count": [1],
        "b_null_count": [1]
    })
    
    assert dq.null_check(df.lazy()) == expected_result


def test_distinct_check():
    dq = DataQuality()
    
    # All distinct rows test
    df = pl.DataFrame({
        "a": [1, 2, 3],
        "b": [4, 5, 6]
    })

    dq.dataframe = df.lazy()

    result_df = pl.DataFrame({
        "a_null_count": [0],
        "b_null_count": [0],
        "total_count": [3],
        "distinct_count": [3]
    })

    assert dq.distinct_check(result_df) == result_df

    # Some duplicates test
    df = pl.DataFrame({
        "a": [1, 1, 2],
        "b": [4, 4, 5]
    })

    dq.dataframe = df.lazy()

    result_df = pl.DataFrame({
        "a_null_count": [0],
        "b_null_count": [0],
        "total_count": [3],
        "distinct_count": [2]
    })

    assert dq.distinct_check(result_df) == result_df


def test_default_checks():
    dq = DataQuality()

    # Just a basic test to ensure it combines the results of the other functions
    df = pl.DataFrame({
        "a": [1, 2, 3],
        "b": [4, 5, 6]
    })

    dq.dataframe = df.lazy()

    result_df = pl.DataFrame({
        "a_null_count": [0],
        "b_null_count": [0],
        "total_count": [3],
        "distinct_count": [3]
    })

    assert dq.default_checks() == result_df

# Optionally, you can add more test cases for other methods as well.

# Execute tests
pytest.main(["-v", "-s"])
