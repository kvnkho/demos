from pyspark.sql import SparkSession
import pyspark.sql.functions as sf 

spark = (SparkSession
        .builder
        .config("spark.executor.cores",4)
        .config("fugue.dummy","dummy")
        .getOrCreate())

df = spark.createDataFrame()

df = df.withColumn('test', sf.lit(1))