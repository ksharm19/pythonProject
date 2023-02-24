from pyspark import SparkContext

sc = SparkContext("local[*]", "Movie-Data")

sc.setLogLevel("INFO")

if __name__ == "__main__":
    myList = ["WARN: Tuesday 4 September 0405",
          "Error: Tuesday 4 September 0405",
          "Error: Tuesday 4 September 0405",
          "Info: Tuesday 4 September 0405"]

    originalLogsRdd = sc.parallelize(myList)
else:
    originalLogsRdd = sc.textFile("/Users/vaibhav/Documents/Spark_doc/logsample.txt")
    print("inside else part")

newPairRdd = originalLogsRdd.map(lambda x: (x.split(":")[0],1))

resultanatRDD = newPairRdd.reduceByKey(lambda x,y: x+y)

result = resultanatRDD.collect()

for x in result:
    print(x)