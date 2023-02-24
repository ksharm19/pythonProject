from pyspark import SparkContext
from sys import stdin

if __name__ == '__main__':
    sc = SparkContext("local[*]", "wordcount")
    sc.setLogLevel("ERROR")
    input = sc.textFile("/Users/vaibhav/Documents/Spark_doc/Search_Data.txt")

    words = input.flatMap(lambda x: x.split(" "))
    word_count = words.map(lambda x: (x.lower()), 1)

    final_count = word_count.reduceByKey(lambda x, y: x + y)

    results = final_count.sortBy(lambda x: x[1], False)

    for a in results:
        print(a)

else:
    print("Not executed directly")

stdin.readline()
