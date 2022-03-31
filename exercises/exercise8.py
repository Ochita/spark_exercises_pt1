from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window as w


def pipeline(df: DataFrame) -> DataFrame:
    window = w.partitionBy('department').orderBy('time') \
        .rowsBetween(w.unboundedPreceding, w.currentRow)
    return df.withColumn('running_total', f.sum('items_sold').over(window))
