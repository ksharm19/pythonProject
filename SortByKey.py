from pyspark import SparkContext
from sys import stdin

if __name__ == '__main__':
    sc = SparkContext("local[*]","wordcount")

    input = sc.textFile("/Users/vaibhav/Documents/Spark_doc/Search_Data.txt")

    words = input.flatMap(lambda x:x.split(" "))
    word_count = words.map(lambda x:(x.lower(),1))

    final_count = word_count.reduceByKey(lambda x,y : x+y).map(lambda x:(x[1],x[0]))

    result = final_count.sortByKey(False).map(lambda x:(x[1],x[0])).collect()

    for a in result:
        print(a)

else:
    print("Not executed directly")

stdin.readline()