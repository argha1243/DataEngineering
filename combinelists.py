from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("PySparkLocalExample").getOrCreate()

list1 = ["a","b","c","d"]
list2 = [1,2,3,4]
print(zip(list1,list2))
print(list(zip(list1,list2)))
rdd = spark.sparkContext.parallelize(list(zip(list1,list2)))
df = rdd.toDF(["Col1","Col2"])
df.show()

spark.stop()