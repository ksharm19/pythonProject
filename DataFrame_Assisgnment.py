from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,TimestampType,StringType


#Step 1
my_conf = SparkConf()
my_conf.set("spark.app.name","my first application")
my_conf.set("spark.master","local[*]")
spark = SparkSession.builder.config(conf=my_conf).getOrCreate()

#Step2
#Logger.getLogger("org").setLevel(Level.ERROR)
spark.sparkContext.setLogLevel("Error")

windowdataSchema = StructType([
    StructField("Country", StringType()),
    StructField("weeknum", IntegerType()),
    StructField("numinvoices", IntegerType()),
    StructField("totalquantity", IntegerType()),
    StructField("invoicevalue", IntegerType()),
])

#step 3 Loading the file and creation of dataframe using dataframe reader API, using explicitly specified schema

windowsdataDf = spark.read.format("csv")\
.schema(windowdataSchema)\
.option("path","/Users/vaibhav/Documents/Spark_doc/windowsdata.csv")\
.load()

#windowsdataDf.show()

'''Step4: Saving the data in parquet format using DataFrame writer API
Data is two-level partitioned on Country and weeknum column, these columns have low cardinality
Default output format is parquet
'''

windowsdataDf.write\
.partitionBy("Country","weeknum")\
.mode("overwrite").save("/Users/vaibhav/Documents/Spark_doc/windowsdata_output")
#.option("path","/Users/vaibhav/Documents/Spark_doc/windowsdata_output")

spark.stop()