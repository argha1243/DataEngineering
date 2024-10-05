from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("Testdropnull").getOrCreate()

data = [("A",1,None),
        ("B",None,"123"),
        ("C",None,"456"),
        ("D",None,None)]
df = spark.createDataFrame(data,["name","roll","id"])
df.show()

col_list = ["roll"]
df.dropna(subset=col_list).show()
# df.show()
spark.stop()