import pytest
import polars as pl
from pyspark.sql import SparkSession
from tinytimmy.tinytim import TinyTim
import pandas as pd

# Fixtures
@pytest.fixture
def spark_session():
    return SparkSession.builder.appName("TinyTimTests").getOrCreate()

@pytest.fixture
def sample_pandas_df():
    return pd.DataFrame({
        'A': [1, 2, 3],
        'B': [4, 5, 6]
    })

@pytest.fixture
def sample_spark_df(spark_session):
    return spark_session.createDataFrame([(1, 4), (2, 5), (3, 6)], ["A", "B"])

@pytest.fixture
def sample_polars_df():
    return pl.DataFrame({
        'A': [1, 2, 3],
        'B': [4, 5, 6]
    })

# Test cases
def test_check_source_with_invalid_type():
    with pytest.raises(ValueError, match="Source type must be one of:"):
        TinyTim(source_type="unknown")

def test_check_source_without_file_path():
    with pytest.raises(ValueError, match="File path must be provided for csv and parquet sources"):
        TinyTim(source_type="csv")

def test_convert_from_pandas(sample_pandas_df):
    tiny_tim = TinyTim(source_type="pandas", dataframe=sample_pandas_df)
    assert isinstance(tiny_tim.dataframe, pl.LazyFrame)

def test_convert_from_polars(sample_polars_df):
    tiny_tim = TinyTim(source_type="polars", dataframe=sample_polars_df)
    assert isinstance(tiny_tim.dataframe, pl.LazyFrame)

def test_convert_from_csv():
    tiny_tim = TinyTim(source_type="csv", file_path="tests/example.csv")
    assert isinstance(tiny_tim.dataframe, pl.LazyFrame)
