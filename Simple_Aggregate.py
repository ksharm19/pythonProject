from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

my_conf = SparkConf()
my_conf.set( "spark.app.name", "my first application" )
my_conf.set( "spark.master", "local[*]" )

spark = SparkSession.builder.config( conf=my_conf ).enableHiveSupport().getOrCreate()

invoiceDF = spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema", True)\
    .option("path", "/Users/vaibhav/Documents/Spark_doc//week12/order_data_2.csv")\
    .load()

##Column object expression

invoiceDF.select(
    count("*").alias("RowCount"),
    sum("Quantity").alias("AvgPrice"),
    avg("UnitPrice").alias("AvgPrice"),
    count("InvoiceNo").alias("CountDistinct")
).show()


##Column String Expresion

invoiceDF.selectExpr(
    "count(*) as RowCount",
    "sum(Quantity) as AvgPrice",
    "avg(UnitPrice) as AvgPrice",
    "count(InvoiceNo) as CountDistinct"
).show()

# passing sql query

invoiceDF.createOrReplaceTempView("sales")
spark.sql("select count(*),sum(Quantity),avg(UnitPrice),count(distinct(InvoiceNo)) from sales").show()
#df.show(20)