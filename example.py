from tinytimmy.tinytim import TinyTim
import pandas as pd
import polars as pl


def main():
    # CSV example
    tm = TinyTim(source_type="csv", file_path="tinytimmy/202306-divvy-tripdata.csv")
    results = tm.default_checks()
    print(results)

    # pandas example
    df = pd.read_csv("tinytimmy/202306-divvy-tripdata.csv")
    tm = TinyTim(source_type="pandas", dataframe=df)
    results = tm.default_checks()
    print(results)

    # polars example
    df = pl.read_csv("tinytimmy/202306-divvy-tripdata.csv", infer_schema_length=10000)
    tm = TinyTim(source_type="polars", dataframe=df)
    results = tm.default_checks()
    print(results)

    # customer filter check example
    tm = TinyTim(source_type="csv", file_path="tinytimmy/202306-divvy-tripdata.csv")
    results = tm.default_checks()
    print(results)
    results = tm.run_custom_check(
        ["start_station_name IS NULL", "end_station_name IS NULL"]
    )
    print(results)

    # # Pandas output example
    # tm = TinyTim(source_type="csv", return_type="pandas", file_path="tinytimmy/202306-divvy-tripdata.csv")
    # results = tm.default_checks()
    # print(results)


if __name__ == "__main__":
    main()
