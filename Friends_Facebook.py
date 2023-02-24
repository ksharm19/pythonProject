
def parseLine(line):
    fields = line.split(",")
    age = int(fields[2])
    numFriends = int(fields[3])

    return(age,numFriends)


from pyspark import SparkContext

sc = SparkContext("local[*]", "FriendsByAge")

lines = sc.textFile("/Users/vaibhav/Documents/Spark_doc/friends-data.csv")
rdd = lines.map(parseLine)
#(33,385)
#output: (33,(385,1))
#in scala we accesss the elements of tuple using x._1,x._2
#in python we access the elements of tuple using x[0],x[2]

totalsByAge = rdd.mapValues(lambda x: (x,1)).reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
averagesByAge = totalsByAge.mapValues(lambda x:x[0]/x[1])
result = averagesByAge.collect()

for a in result:
    print(a)
