### Tiny Timmy

<img src="https://github.com/danielbeach/tinytimmy/blob/main/imgs/tinytim.png" width="300">


A dead simple and easy to use Data Quality (DQ) tool built for Dataframes and Files with Python.

`Tiny Timmy` uses the Python bindings for Polars a `Rust` based DataFrame tool.

Support includes ...
- `polars`
- `pandas`
- `pyspark`
- `csv` files
- `parquet` files

Both `dataframe` and `file` support. Simply "point and shoot."


### Installation
Install Tiny Timmy with `pip`
```
pip install tinytimmy
```

### Usage
Create an instance of Tiny Timmy.
 - specify `source_type`
    - `polars`
    - `pandas`
    - `pyspark`
    - `csv`
    - `parquet`
 - specify either `file_path` or `dataframe`

```
from tinytimmy.tinytim import TinyTim
tm = TinyTim(source_type="csv", file_path="202306-divvy-tripdata.csv")
```

Then call either the default checks or a custom check.
```
results = tm.default_checks()
results = tm.run_custom_check(["{SQL filter}", "{SQL filter}"])
```

You can pass Tiny Timmy a `dataframe` while specifying what type it is (`pandas`, `polars`, `pyspark`)
and ask for `default_checks`, also you can simply pass a file uri to a `csv` or `parquet` file.

You can also pass custom DQ checks as a list of `SQL` statements in a normal `WHERE` clause.  

`Tiny Timmy` returns check results as a `Polars` `dataframe` by default, you can 
request the results as a `pandas` or `pyspark` `dataframe`.

```
results = tm.default_checks(return_as='pandas')
```

For example.
```
┌───────────────────────────────────┬─────────────┐
│ check_type                        ┆ check_value │
│ ---                               ┆ ---         │
│ str                               ┆ i64         │
╞═══════════════════════════════════╪═════════════╡
│ null_check_start_station_name     ┆ 978         │
│ null_check_start_station_id       ┆ 978         │
│ …                                 ┆ …           │
│ started_at_whitespace_count       ┆ 1000        │
│ ended_at_whitespace_count         ┆ 1000        │
│ start_station_name_whitespace_co… ┆ 22          │
│ end_station_name_whitespace_coun… ┆ 22          │
```

Current functionality ...
- `default_checks()`
    - check all columns for `null` values
    - check if dataset is distinct or contains duplicates
    - check if columns have whitespace
    - check for leading or trailing whitespace
- `run_custom_check(["{some SQL WHERE clause}"])`

### Example Usage

`CSV` support.
```
from tinytimmy.tinytim import TinyTim
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
from tinytimmy.tinytim import TinyTim
df = pd.read_csv("202306-divvy-tripdata.csv")
tm = TinyTim(source_type="pandas", dataframe=df)
results = tm.default_checks()
>> Column start_station_name has 978 null values
>> Column start_station_id has 978 null values
>> Column end_station_name has 978 null values
>> Column end_station_id has 978 null values
>> Your dataset has no duplicates
```

`Custom` Data Quality checks are supported as a `list` of `SQL` based formats.
They are given as they would appear in a `WHERE` clause.
You can pass one or more checks in the list.

```
from tinytimmy.tinytim import TinyTim
tm = TinyTim(source_type="csv", file_path="202306-divvy-tripdata.csv")
tm.default_checks()
results = tm.run_custom_check(["start_station_name IS NULL", "end_station_name IS NULL"])
Column start_station_name has 978 null values
Column start_station_id has 978 null values
Column end_station_name has 978 null values
Column end_station_id has 978 null values
Your dataset has no duplicates
Column started_at has 1000 whitespace values
Column ended_at has 1000 whitespace values
Column start_station_name has 22 whitespace values
Column end_station_name has 22 whitespace values
No leading or trailing whitespace values found
shape: (10, 2)
┌───────────────────────────────────┬─────────────┐
│ check_type                        ┆ check_value │
│ ---                               ┆ ---         │
│ str                               ┆ i64         │
╞═══════════════════════════════════╪═════════════╡
│ null_check_start_station_name     ┆ 978         │
│ null_check_start_station_id       ┆ 978         │
│ null_check_end_station_name       ┆ 978         │
│ null_check_end_station_id         ┆ 978         │
│ …                                 ┆ …           │
└───────────────────────────────────┴─────────────┘
Your custom check start_station_name IS NULL found 978 records that match your filter statement
Your custom check end_station_name IS NULL found 978 records that match your filter statement
```

### Tests / Local Setup / Contributions.
To develop and work on TinyTimmy locally, a `Docker` image and `docker-compose` is provided.

First, build the image
`docker build --tag=tinytimmy .`

To run the local unit tests run ...
`docker-compose up test`

To simply work inside the Docker container run ...
`docker run -it tinytimmy /bin/bash`
