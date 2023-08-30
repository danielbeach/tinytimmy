import polars as pl


class DataQuality:
    def __init__(self, custom_checks: dict = None) -> None:
        self.custom_checks = custom_checks
        self.dataframe = None
        self.results = None

    def null_check(self, df: pl.LazyFrame) -> pl.DataFrame:
        null_dict = {"check_type": None, "check_value": None}
        schema = {"check_type": pl.Utf8, "check_value": pl.Int64}
        null_dataframe = pl.DataFrame(null_dict, schema=schema)
        df = df.collect()
        for column in df.columns:
            null_count = df[column].is_null().sum()
            if null_count > 0:
                null_dataframe = null_dataframe.vstack(
                    pl.DataFrame(
                        {
                            "check_type": f"null_check_{column}",
                            "check_value": null_count,
                        }
                    )
                )
                print(f"Column {column} has {null_count} null values")
        if null_dataframe.shape[1] == 0:
            print("No null values found")
        return null_dataframe

    def distinct_check(self, results: pl.DataFrame):
        df = self.dataframe.collect()
        inital_count = df.shape[0]
        distinct_count = df.select(pl.count())[0, 0]
        results = results.vstack(
            pl.DataFrame({"check_type": "total_count", "check_value": inital_count})
        )
        results = results.vstack(
            pl.DataFrame(
                {"check_type": "distinct_count", "check_value": distinct_count}
            )
        )
        if inital_count == distinct_count:
            print("Your dataset has no duplicates")
        else:
            x = inital_count - distinct_count
            print(f"Your dataset has {x} duplicates")
        return results

    def check_columns_for_whitespace(self, results: pl.LazyFrame) -> pl.DataFrame:
        df = self.dataframe.collect()
        found_whitespace = False
        for column in df.columns:
            whitespace_count = df[column].str.contains(" ").sum()
            if whitespace_count > 0:
                results = results.vstack(
                    pl.DataFrame(
                        {
                            "check_type": f"{column}_whitespace_count",
                            "check_value": whitespace_count,
                        }
                    )
                )
                print(f"Column {column} has {whitespace_count} whitespace values")
                found_whitespace = True
        if not found_whitespace:
            print("No whitespace values found")
        return results

    def check_columns_for_leading_trailing_whitespace(
        self, results: pl.LazyFrame
    ) -> pl.DataFrame:
        df = self.dataframe.collect()
        found_whitespace = False
        for column in df.columns:
            if True in df[column].str.starts_with(" ") or True in df[
                column
            ].str.ends_with(" "):
                results = results.vstack(
                    pl.DataFrame(
                        {
                            "check_type": f"{column}_leading_whitespace",
                            "check_value": pl.lit(True),
                        }
                    )
                )
                print(f"Column {column} has leading or trailing whitespace values")
                found_whitespace = True
        if not found_whitespace:
            print("No leading or trailing whitespace values found")
        return results
    
    def check_datetime_format(self, 
                                   results: pl.LazyFrame, 
                                   datetime_dict: dict) -> pl.DataFrame: 
        df = self.dataframe.collect()
        found_malformed_date = False
        for column in datetime_dict.keys():
            print('processing date column: ', column)
            date_format = datetime_dict[column]
            malformed_count = (
                df.select(column)
                .to_series().str.strptime(pl.Datetime, date_format, strict=True)
                .is_null().sum()
                )
            if malformed_count > 0:
                results = results.vstack(
                    pl.DataFrame(
                        {
                            "check_type": f"{column}_malformed_date_format",
                            "check_value": malformed_count,
                        }
                    )
                )
                print(f"Column {column} does not match the date format {date_format}")
                found_malformed_date = True
        if not found_malformed_date:
            print("No malformed date values found")
        return results
        
    def default_checks(self, datetime_dict: dict = None) -> pl.DataFrame:
        print(self.dataframe.schema)
        results = self.null_check(self.dataframe)
        results = self.distinct_check(results)
        results = self.check_columns_for_whitespace(results)
        results = self.check_columns_for_leading_trailing_whitespace(results)
        if datetime_dict: 
            results = self.check_datetime_format(results, datetime_dict)
        else: 
            print("No datetime columns provided")
        self.results = results
        return results.filter(~pl.col("check_type").is_null())

    def run_custom_check(self, sql_filter_statements: list) -> pl.DataFrame:
        # must be in the form of a SQL WHERE statement
        results = self.results
        for sql_filter_statement in sql_filter_statements:
            ctx = pl.SQLContext()
            ctx.register("frame", self.dataframe)
            res = ctx.execute(
                f"""
                                SELECT *  FROM frame WHERE {sql_filter_statement}
                                    """
            ).collect()
            x = res.shape[0]
            if x > 0:
                print(
                    f"Your custom check {sql_filter_statement} found {x} records that match your filter statement"
                )
                results = results.vstack(
                    pl.DataFrame(
                        {"check_type": f"{sql_filter_statement}", "check_value": x}
                    )
                )
        return results
         