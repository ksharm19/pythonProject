from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

my_conf = SparkConf()
my_conf.set("spark.app.name","my first application")
my_conf.set("spark.master","local[*]")

spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

#myregex = r'^(\S+) (\S+)\t(\S+)\,(\S+)'

line_df = spark.read.text("Users/vaibhav/Documents/Spark_doc/week12/orders_new.csv")


line_df.printSchema()
line_df.show()