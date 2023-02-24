from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name","my first application")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).enableHiveSupport().getOrCreate()

orderDf = spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","/Users/vaibhav/Documents/Spark_doc/orders.csv")\
    .load()

# orderDf.createOrReplaceTempView("orders")
#
# result_df = spark.sql("""select order_customer_id, count(*) as total_orders from orders
# where order_status = 'CLOSED'
# group by order_customer_id order by total_orders desc""").show()

spark.sql("create database if not exists retail")
orderDf.write.format("csv")\
    .mode("overwrite")\
    .bucketBy(4,"order_customer_id")\
    .sortBy("order_customer_id")\
    .saveAsTable("retail.orders4")


