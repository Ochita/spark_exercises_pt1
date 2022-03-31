from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window as w


def pipeline(df: DataFrame) -> DataFrame:
    window = w.partitionBy().orderBy(f.col('Salary'))
    return df.withColumn('Percentage', f.percent_rank().over(window)) \
        .withColumn('Rank', f.when(f.col('Percentage') < 0.3, f.lit('High'))
                    .when(f.col('Percentage') < 0.7,
                          f.lit('Low')).otherwise('Average'))
