
from pyspark import SparkContext

sc = SparkContext("local[*]","Keywordcount")
input = sc.textFile("/Users/vaibhav/Documents/Spark_doc/boringwords.txt")

result = input.collect()

for a in result:
    print(a)