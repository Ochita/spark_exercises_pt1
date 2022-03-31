from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def pipeline(df: DataFrame) -> DataFrame:
    return df.groupBy(f.col('group')).agg(f.max('id').alias('max_id'))
