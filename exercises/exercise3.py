from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def pipeline(df: DataFrame) -> DataFrame:
    result = df.withColumn('date_date',
                           f.to_date(f.col('date_string'), 'dd/MM/yyyy')) \
        .withColumn('to_date', f.current_date()) \
        .select('date_string', 'to_date',
                f.datediff(f.col('to_date'), f.col('date_date')).alias('diff'))
    return result
