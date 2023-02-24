from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession

my_conf = SparkConf()
my_conf.set("spark.app.name","my first application")
my_conf.set("spark.master","local[*]")
my_conf.set("spark.jars","/Users/vaibhav/Downloads/spark-core_2.13-3.3.1.jars")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

orderDf = spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .option("path","/Users/vaibhav/Documents/Spark_doc/orders.csv")\
    .load()



#print("number of partitions are: ",orderDf.rdd.getNumPartitions())
#ordersRep = orderDf.repartition(4)

#ordersRep.write.format("csv").partitionBy("order_status")\
orderDf.write.format("avro")\
    .mode("overwrite")\
    .option("path","/Users/vaibhav/Documents/Spark_doc/output_folder")\
    .save()