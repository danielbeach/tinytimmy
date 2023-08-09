from tinytim import TinyTim
import pandas as pd
import polars as pl


def main():
    # CSV example
    tm = TinyTim(source_type="csv", file_path="202306-divvy-tripdata.csv")
    tm.default_checks()

    # pandas example
    df = pd.read_csv("202306-divvy-tripdata.csv")
    tm = TinyTim(source_type="pandas", dataframe=df)
    tm.default_checks()

    # polars example
    df = pl.read_csv("202306-divvy-tripdata.csv", infer_schema_length=10000)
    tm = TinyTim(source_type="polars", dataframe=df)
    tm.default_checks()

    # customer filter check example
    tm = TinyTim(source_type="csv", file_path="202306-divvy-tripdata.csv")
    tm.default_checks()
    tm.run_custom_check("start_station_name IS NULL")


if __name__ == "__main__":
    main()
