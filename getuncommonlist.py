from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("PysparkLocalExample").getOrCreate()

sc = spark.sparkContext
list1 = [1,2,3,4,5,6]
rddA = sc.parallelize(list1)
list2 = [5,6,7,8,9]
rddB = sc.parallelize(list2)

result_rdd_A = rddA.subtract(rddB)
result_rdd_B = rddB.subtract(rddA)

result = result_rdd_A.union(result_rdd_B)

result = result.collect()

print(result)

spark.stop()

