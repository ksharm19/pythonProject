from pyspark import SparkContext,StorageLevel


sc = SparkContext("local[*]", "PremiumCustomer")

base_rdd = sc.textFile("/Users/vaibhav/Documents/Spark_doc/customer-orders.csv")

mapped_input = base_rdd.map(lambda x: (x.split(",")[0],float(x.split(",")[2])))

total_by_customer = mapped_input.reduceByKey(lambda x,y: x+y)

premium_customers = total_by_customer.filter(lambda x: x[1] > 5000)

double_amount = premium_customers.map(lambda x:(x[0],x[1]*2)).persist(StorageLevel.MEMORY_ONLY)
result = double_amount.collect()

for x in result:
    print(x)
print("test")

print(doubled_amount.count())
