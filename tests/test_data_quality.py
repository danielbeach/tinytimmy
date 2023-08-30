import polars as pl
from polars.testing import assert_frame_equal
import pytest
from tinytimmy.data_quality import DataQuality
from loguru import logger

def test_no_nulls():
    # ARRANGE
    dq = DataQuality()
    input_df = pl.LazyFrame({
        "a": [1, 2, 3], 
        "b": [4, 5, 6]
    })

    # ACT
    output_result = dq.null_check(input_df)
    expected_result = pl.DataFrame([
        {
            "check_type": "null_check_a",
            "check_value": 0
        },
        {
            "check_type": "null_check_b",
            "check_value": 0
        },
    ])

    # ASSERT
    assert_frame_equal(output_result, expected_result)


def test_null_check():
    pass

    # # Some null values test
    # df = pl.DataFrame({"a": [1, None, 3], "b": [None, 5, 6]})

    # expected_result = pl.DataFrame({"a_null_count": [1], "b_null_count": [1]})

    # assert dq.null_check(df.lazy()) == expected_result


def test_distinct_check():
    dq = DataQuality()
    df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    dq.dataframe = df.lazy()

    result_df = pl.DataFrame(
        {
            "a_null_count": [0],
            "b_null_count": [0],
            "total_count": [3],
            "distinct_count": [3],
        }
    )

    assert dq.distinct_check(result_df) == result_df

    # Some duplicates test
    df = pl.DataFrame({"a": [1, 1, 2], "b": [4, 4, 5]})

    dq.dataframe = df.lazy()

    result_df = pl.DataFrame(
        {
            "a_null_count": [0],
            "b_null_count": [0],
            "total_count": [3],
            "distinct_count": [2],
        }
    )

    assert dq.distinct_check(result_df) == result_df


def test_default_checks():
    dq = DataQuality()

    # Just a basic test to ensure it combines the results of the other functions
    df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

    dq.dataframe = df.lazy()

    result_df = pl.DataFrame(
        {
            "a_null_count": [0],
            "b_null_count": [0],
            "total_count": [3],
            "distinct_count": [3],
        }
    )

    assert dq.default_checks() == result_df
