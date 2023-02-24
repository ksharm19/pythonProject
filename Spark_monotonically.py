from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

my_conf = SparkConf()
my_conf.set( "spark.app.name", "my first application" )
my_conf.set( "spark.master", "local[*]" )

spark = SparkSession.builder.config( conf=my_conf ).enableHiveSupport().getOrCreate()

df = spark.read.format("csv") \
    .option("inferSchema", True) \
    .option("path", "/Users/vaibhav/Documents/Spark_doc/orders.csv") \
    .load()

#ordersDf = df.toDF("orderid","orderdate","customerid","status")
newDf = df\
    .withColumn("date1",unix_timestamp(col("orderdate")))\
    .withColumn("newid",monotonically_increasing_id())\
    .dropDuplicates(["orderdate","customerid"])\
    .drop("orderid")\
    .sort("orderdate")

newDf.show()

#ordersDf.show()

