import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
spark = SparkSession.builder.getOrCreate()

df = pd.DataFrame({"id": ["A","A","A", "B","B","B"], "val": [1,2,3,4,5,6]})

sdf = spark.createDataFrame(df)
sdf = sdf.withColumn("val", sf.col("val") + sf.lit(1))\
    .groupBy("id")\
    .mean()

sdf.show()