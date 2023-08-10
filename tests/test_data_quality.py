import polars as pl
import pytest
from tinytimmy.data_quality import DataQuality

@pytest.fixture
def sample_dataframe():
    return pl.DataFrame({
        'A': [1, 2, None, 4, 5],
        'B': [None, 2, 3, 4, 5],
        'C': [1, 2, 3, 4, 5]
    })

@pytest.fixture
def data_quality():
    return DataQuality()

def test_null_check(sample_dataframe, data_quality, capsys):
    result = data_quality.null_check(sample_dataframe.lazy())
    assert "A" in result.columns
    assert "B" in result.columns
    assert "Column A has 1 null values" in capsys.readouterr().out

def test_distinct_check(sample_dataframe, data_quality, capsys):
    data_quality.dataframe = sample_dataframe.lazy()
    data_quality.distinct_check()
    assert "Your dataset has no duplicates" in capsys.readouterr().out

def test_default_checks(sample_dataframe, data_quality, capsys):
    data_quality.dataframe = sample_dataframe.lazy()
    result = data_quality.default_checks()
    assert "A" in result.columns
    assert "B" in result.columns
    assert "Your dataset has no duplicates" in capsys.readouterr().out

def test_run_custom_check(sample_dataframe, data_quality, capsys):
    data_quality.dataframe = sample_dataframe.lazy()
    result = data_quality.run_custom_check("A is NULL")
    assert result.shape[0] == 1
    assert "Your custom check found 1 records" in capsys.readouterr().out

