from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f


def generate_dates(spark: SparkSession) -> DataFrame:
    now = date.today()
    past = now - timedelta(days=24 * 30)
    df = spark.createDataFrame([(past, now)], ('past', 'now'))
    return df.select(
        f.explode(f.expr('sequence(now, past, interval -1 month)'))
        .alias('dates')) \
        .select(f.date_format(f.col('dates'), 'yyyyMM')
                .alias('year_month'))


def pipeline(df: DataFrame, range_df: DataFrame) -> DataFrame:
    group = df.groupBy('year_month').agg(f.sum('amount').alias('amount'))
    result = range_df.join(group, 'year_month', 'left') \
        .withColumn('amount', f.coalesce(f.col('amount'), f.lit(0)))
    return result
