from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("PysparkTest").getOrCreate()

data = [("A", 10),("B",20),("C",30),("D",40),("E",50),("F",50)]
df = spark.createDataFrame(data,["Name","Age"])
# df.show()
# df = df.select("*",(df["Age"]*0).alias("Minimum"))
# df = df.withColumn("25th",df.Age*25/100)
# df = df.select("*",(df["Age"]*.5).alias("50th"))
# df = df.select("*",(df["Age"]*.75).alias("75th"))
# df = df.select("*",(df["Age"]*1).alias("Maximum"))
# df.show()

quantiles = df.approxQuantile("Age",[0.0,0.25,0.5,0.75,1.0],0.1)
print(quantiles)
spark.stop()