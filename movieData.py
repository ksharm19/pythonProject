from pyspark import SparkContext

sc = SparkContext("local[*]", "Movie-Data")

lines = sc.textFile("/Users/vaibhav/Documents/Spark_doc/movie-data.data")

ratings = lines.map(lambda x:(x.split("\t")[2],1))

result = ratings.reduceByKey(lambda x,y: x+y).collect()

for a in result:
    print(a)