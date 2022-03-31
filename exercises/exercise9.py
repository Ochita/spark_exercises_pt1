from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window as w


def pipeline(df: DataFrame) -> DataFrame:
    window = w.partitionBy('department').orderBy('time')
    return df.withColumn('diff', f.col('running_total') - f.coalesce(
        f.lag('running_total').over(window), f.lit(0)))
