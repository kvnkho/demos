from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

df = spark.read.format('csv').options(header='true', inferSchema='true').load('s3://kaggle-data-instacart/products.csv')

df.show()

df.groupby("aisle_id").count().show()