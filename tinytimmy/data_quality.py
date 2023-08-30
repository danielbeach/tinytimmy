import polars as pl
from loguru import logger


class DataQuality:
    def __init__(self, custom_checks: dict = None) -> None:
        self.custom_checks = custom_checks
        self.dataframe = None
        self.results = None

    def null_check(self, input_frame: pl.LazyFrame) -> pl.DataFrame:
        """
        Compute the count of nulls in each column in the input Polars LazyFrame.
        """
        null_counts = input_frame.select(pl.all().is_null().sum()).collect()
        column_null_check_results = []
        for column_name in null_counts.columns:
            null_count = null_counts[column_name][0]
            if null_count > 0:
                logger.info(f"Column {column_name} has {null_count} null values")
            column_null_check_results.append({
                "check_type": f"null_check_{column_name}", 
                "check_value": null_count
            })
        return (
            pl.DataFrame(
                column_null_check_results, 
                schema={
                    "check_type": pl.Utf8, 
                    "check_value": pl.Int64
                }
            )
            # .pivot(
            #     values="check_value",
            #     columns="check_type",
            #     index="check_type"
            # )
        )

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
        return results.filter(~pl.col("check_type").is_null())

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

    def default_checks(self, 
                       return_as: str = 'polars', 
                       spark_session: SparkSession =  None
                       ) -> pl.DataFrame:
        print(self.dataframe.schema)
        results = self.null_check(self.dataframe)
        results = self.distinct_check(results)
        results = self.check_columns_for_whitespace(results)
        results = self.check_columns_for_leading_trailing_whitespace(results)
        results = results.filter(~pl.col("check_type").is_null())
        self.results = results.filter(~pl.col("check_type").is_null())
        if return_as == 'polars':
            return self.results
        elif return_as == 'pandas':
            return self.results.to_pandas()
        elif return_as == 'spark':
            return  spark_session.createDataFrame(self.results.to_pandas())
        else:
            raise ValueError(f"Unknown return type {return_as}")

    def run_custom_check(self, 
                         sql_filter_statements: list,
                         return_as: str = 'polars', 
                         spark_session: SparkSession =  None
                         ) -> pl.DataFrame:
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
        if return_as == 'polars':
            return results
        elif return_as == 'pandas':
            return results.to_pandas()
        elif return_as == 'spark':
            return  spark_session.createDataFrame(self.results.to_pandas())
        else:
            raise ValueError(f"Unknown return type {return_as}")
