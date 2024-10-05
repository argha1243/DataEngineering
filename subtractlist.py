from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("PySparkLocalExample").getOrCreate()

list1 = [1,2,3,4,5]
list2 = [4,5,6,7,8]
sc = spark.sparkContext
rdd_A = sc.parallelize(list1)
rdd_B = sc.parallelize(list2)

result_rdd = rdd_A.subtract(rdd_B)
# print(result_rdd)
result_list = result_rdd.collect()
# print(result_rdd)
print(result_list)
spark.stop()