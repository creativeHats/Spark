from pyspark import SparkContext
from pyspark.sql import SparkSession

my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.coll") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.coll") \
    .getOrCreate()

dataframe = my_spark.read.csv('data.csv', header=True)
dataframe.write.format("com.mongodb.spark.sql.DefaultSource") \
    .mode("append").option("database", "mydb").option("collection", "myColl").save()
