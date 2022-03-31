from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def pipeline(df: DataFrame) -> DataFrame:
    result = df.select(f.col('id'),
                       f.split(f.col('words'), ',').alias('w_list')) \
        .select(f.col('id'), f.explode(f.col('w_list')).alias('w')) \
        .groupBy(f.col('w')).agg(f.collect_list('id').alias('ids')) \
        .filter(f.col('w').isin(['five', 'one', 'seven', 'six']))
    return result
