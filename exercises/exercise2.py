from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window as w


def pipeline(df: DataFrame) -> DataFrame:
    window = w.partitionBy('country').orderBy(f.col('int').desc())
    result = df.withColumn('int',
                           f.regexp_replace(f.col('population'),
                                            ' ', '').cast('integer')) \
        .withColumn('position', f.row_number().over(window)) \
        .filter(f.col('position') == 1) \
        .select('name', 'country', 'population')
    return result
