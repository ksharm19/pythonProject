from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,TimestampType,StringType

my_conf = SparkConf()
my_conf.set("spark.app.name","my first application")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

'''orderSchema = StructType([
    StructField("orderid",IntegerType()),
    StructField("orderdate",TimestampType()),
    StructField("customerid",IntegerType()),
    StructField("status",StringType())])'''

ordersDDL = """orderidnew Integer, orderdate Timestamp, customerid Integer, status string"""



orderDf = spark.read.format("csv")\
    .option("header",True)\
    .schema(ordersDDL)\
    .option("path","/Users/vaibhav/Documents/Spark_doc/orders.csv")\
    .load()


'''groupedDf = orderDf.repartition(4)\
.where("order_customer_id > 10000")\
.select("order_id","order_customer_id")\
.groupby("order_customer_id")\
.count()'''

orderDf.printSchema()
orderDf.show()
#groupedDf.show()
spark.stop()