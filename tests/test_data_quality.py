import polars as pl
from polars.testing import assert_frame_equal
from tinytimmy.data_quality import DataQuality


def test_no_nulls():
    """
    Test that when there are no nulls in the data set, we return 0's for the
    check_values
    """
    # ARRANGE
    dq = DataQuality()
    input_df = pl.LazyFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

    # ACT
    output_result = dq.null_check(input_df)
    expected_result = pl.DataFrame(
        [
            {"check_type": "null_check_a", "check_value": 0},
            {"check_type": "null_check_b", "check_value": 0},
        ]
    )

    # ASSERT
    assert_frame_equal(output_result, expected_result)


def test_some_nulls():
    """
    Test that when there are a few nulls in the data set, we return the count of
    null values for the check_values
    """
    # ARRANGE
    dq = DataQuality()
    input_df = pl.LazyFrame({"a": [1, None, 3], "b": [None, 5, 6]})

    # ACT
    output_result = dq.null_check(input_df)
    expected_result = pl.DataFrame(
        [
            {"check_type": "null_check_a", "check_value": 1},
            {"check_type": "null_check_b", "check_value": 1},
        ]
    )

    # ASSERT
    assert_frame_equal(output_result, expected_result)


def test_distinct_check():
    """
    Test that we can return the approximate number of distinct values
    for each column in a dataset
    """
    # ARRANGE
    dq = DataQuality()
    input_frame = pl.LazyFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

    # ACT
    output_result = dq.distinct_check(input_frame)
    expected_result = pl.DataFrame(
        [
            {"check_type": "distinct_count_a", "check_value": 3},
            {"check_type": "distinct_count_b", "check_value": 3},
        ]
    )

    # ASSERT
    assert_frame_equal(output_result, expected_result)


def test_default_checks():
    """
    Test that we can return the null checks and the distinct checks altogether.
    """
    # ARRANGE
    dq = DataQuality()
    input_frame = pl.LazyFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

    # ACT
    output_result = dq.default_checks(input_frame)
    expected_result = pl.DataFrame(
        [
            {"check_type": "null_check_a", "check_value": 0},
            {"check_type": "null_check_b", "check_value": 0},
            {"check_type": "distinct_count_a", "check_value": 3},
            {"check_type": "distinct_count_b", "check_value": 3},
        ]
    )

    # ASSERT
    assert_frame_equal(output_result, expected_result)
