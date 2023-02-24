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
# Column Object Expression
summaryDF = invoiceDF.groupby("Country","InvoiceNo")\
    .agg(sum("Quantity").alias("TotalQuantity"),
         sum(expr("Quantity * UnitPrice")).alias("InvoiceValue"))
summaryDF.show()

#String Expression

summaryDF1 = invoiceDF.groupby("Country","InvoiceNo")\
    .agg(expr("sum(Quantity) as TotalQuantity"),
        expr("sum(Quantity * UnitPrice) as InvoiceValue")).show()

#Spark Sql
invoiceDF.createOrReplaceTempView("sales")

spark.sql("""select country,InvoiceNo,sum(Quantity) as totalquantity, sum(quantity * UnitPrice) as InvoiceValue
from sales group by country,InvoiceNo""").show()