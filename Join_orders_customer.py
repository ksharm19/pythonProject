from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *

my_conf = SparkConf()
my_conf.set( "spark.app.name", "my first application" )
my_conf.set( "spark.master", "local[*]" )

spark = SparkSession.builder.config( conf=my_conf ).enableHiveSupport().getOrCreate()

orderDf = spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema", True)\
    .option("path", "/Users/vaibhav/Documents/Spark_doc/week12/orders_data.csv")\
    .load()

customersDf = spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema", True)\
    .option("path", "/Users/vaibhav/Documents/Spark_doc/week12/customers_ds.csv")\
    .load()

joinCondition = orderDf.order_customer_id == customersDf.customer_id
joinType = "inner"
joinedDf = orderDf.join(customersDf,joinCondition,joinType)


joinedDf.show()