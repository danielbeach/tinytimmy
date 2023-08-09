### Tiny Tim

<img src="https://github.com/danielbeach/tinytim/blob/main/imgs/tinytim.png" width="300">


A simple and easy to use Data Quality (DQ) tool built with Python.

`Tiny Tim` uses the Python bindings for Polars a `Rust` based DataFrame tool.

Support includes ...
- `polars`
- `pandas`
- `pyspark`
- `csv` files
- `parquet` files

Both `dataframe` and `file` support. Simply "point and shoot."


#### Usage
You can pass Tiny Tim a `dataframe` while specificy what type it is (`pandas`, `polars`, `pyspark`)
and ask for `default_checks`, also you can simply pass a file uri to a `csv` or `parquet` file.

You can also pass custom DQ checks in the form of `SQL` statements that would be found
in a nomral `WHERE` clause. Results of your checks are returned as a `Polars` dataframe.

Current functionality ...
- `default_checks()`
    - check all columns for `null` values
    - check if dataset is distinct or contains duplicates
- `run_custom_check("{some SQL WHERE clause})`

#### Example Usage

`CSV` support.
```
tm = TinyTim(source_type="csv", file_path="202306-divvy-tripdata.csv")
results = tm.default_checks()
>> Column start_station_name has 978 null values
>> Column start_station_id has 978 null values
>> Column end_station_name has 978 null values
>> Column end_station_id has 978 null values
>> Your dataset has 45 duplicates
```

`Pandas` support.
```
df = pd.read_csv("202306-divvy-tripdata.csv")
tm = TinyTim(source_type="pandas", dataframe=df)
results = tm.default_checks()
>> Column start_station_name has 978 null values
>> Column start_station_id has 978 null values
>> Column end_station_name has 978 null values
>> Column end_station_id has 978 null values
>> Your dataset has no duplicates
```

`Custom` Data Quality checks are supported in a `SQL` based format. 
They are given as they would appear in a `WHERE` clause.

```
tm = TinyTim(source_type="csv", file_path="202306-divvy-tripdata.csv")
tm.default_checks()
results = tm.run_custom_check("start_station_name IS NULL")
>> Your custom check found 978 records that match your filter statement
```