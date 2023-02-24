from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *

my_conf = SparkConf()
my_conf.set( "spark.app.name", "my first application" )
my_conf.set( "spark.master", "local[*]" )

spark = SparkSession.builder.config( conf=my_conf ).enableHiveSupport().getOrCreate()

invoiceDF = spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema", True)\
    .option("path", "/Users/vaibhav/Documents/Spark_doc/week12/windowdata.csv")\
    .load()

mywindows = Window.partitionBy("country")\
    .orderBy("weeknum")\
    .rowsBetween(Window.unboundedPreceding,Window.currentRow)

mydf = invoiceDF.withColumn("RunningTotal",sum("invoicevalue").over(mywindows))

mydf.show()