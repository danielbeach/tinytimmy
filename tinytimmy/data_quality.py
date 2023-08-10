import polars as pl


class DataQuality:
    def __init__(self, custom_checks: dict = None) -> None:
        self.custom_checks = custom_checks
        self.dataframe = None

    def null_check(self, df: pl.LazyFrame) -> pl.DataFrame:
        null_dict = {}
        df = df.collect()
        for column in df.columns:
            null_count = df[column].is_null().sum()
            if null_count > 0:
                null_dict[column+'_null_count'] = null_count
                print(f"Column {column} has {null_count} null values")
        null_dataframe = pl.DataFrame(null_dict)
        if null_dataframe.shape[1] == 0:
            print("No null values found")
        return null_dataframe

    def distinct_check(self, results: pl.DataFrame):
        df = self.dataframe.collect()
        inital_count = df.shape[0]
        distinct_count = df.select(pl.count())[0, 0]
        results = results.with_columns(pl.lit(inital_count).alias("total_count"))
        results = results.with_columns(pl.lit(distinct_count).alias("distinct_count"))
        if inital_count == distinct_count:
            print("Your dataset has no duplicates")
        else:
            x = inital_count - distinct_count
            print(f"Your dataset has {x} duplicates")
        return results

    def default_checks(self) -> pl.DataFrame:
        print(self.dataframe.schema)
        results = self.null_check(self.dataframe)
        results = self.distinct_check(results)
        return results

    def run_custom_check(self, sql_filter_statements: list) -> pl.DataFrame:
        # must be in the form of a SQL WHERE statement
        custom_checks_dict = {}
        for sql_filter_statement in sql_filter_statements:
            ctx = pl.SQLContext()
            ctx.register("frame", self.dataframe)
            res = ctx.execute(
                f"""
                                SELECT *  FROM frame WHERE {sql_filter_statement}
                                    """
            ).collect()
            x = res.shape[0]
            custom_checks_dict[sql_filter_statement+' custom_check'] = x
            custom_checks_df = pl.DataFrame(custom_checks_dict)
            if x > 0:
                print(
                    f"Your custom check {sql_filter_statement} found {x} records that match your filter statement"
                )
        return custom_checks_df
