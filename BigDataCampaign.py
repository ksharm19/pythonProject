from pyspark import SparkContext


def loadBoringWord():
    boring_words = set(line.strip() for line in open ("/Users/vaibhav/Documents/Spark_doc/boringwords.txt"))
    return boring_words
sc = SparkContext("local[*]","Keywordcount")
nameset = sc.broadcast(loadBoringWord())
initial_rdd = sc.textFile("/Users/vaibhav/Documents/Spark_doc/bigdatacampaigndata.csv")


#rddVariable = x.split(",")[10],x.split(",")[0]
mapped_input = initial_rdd.map(lambda x: (float(x.split(",")[10]),x.split(",")[0]))
#(300,"big data Trainer")
#(300,"big")(300, "data"), (300,"trainer")

words = mapped_input.flatMapValues(lambda x: x.split(" "))
final_mapped = words.map(lambda x:(x[1].lower(), x[0]))

filtered_rdd = final_mapped.filter(lambda x: x[0] not in nameset.value)
total = filtered_rdd.reduceByKey(lambda x,y : x+y)

sorted = total.sortBy(lambda x: x[1], False)

result = sorted.take(20)

for x in result:
    print(x)






