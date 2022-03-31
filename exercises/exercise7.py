from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window as w


def pipeline(df: DataFrame) -> DataFrame:
    window = w.partitionBy('department')
    return df.withColumn('diff',
                         f.max('salary').over(window) - f.col('salary'))
