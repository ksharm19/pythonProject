from pyspark import SparkContext

def blankLineChecker(line):
    if(len(line)==0):
        myaccum.add(1)
sc = SparkContext("local[*]","Accumulator")

myrdd = sc.textFile("/Users/vaibhav/Documents/Spark_doc/samplefile.txt")

myaccum = sc.accumulator(0)

myrdd.foreach(blankLineChecker)

print(myaccum.value)