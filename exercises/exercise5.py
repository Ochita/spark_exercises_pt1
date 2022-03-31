from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def pipeline(df: DataFrame) -> DataFrame:
    return df.withColumn('future', f.expr('date_add(date, number_of_days)'))
