from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("TestColRenamed").getOrCreate()

data = [("A","1","12000"),
        ("B","2","12000"),
        ("C","3","12000"),
        ("D","4","12000"),
        ("E","5","12000")]
cols = ["Name","Id","Sal"]

df = spark.createDataFrame(data,cols)

df.show()
# df = df.withColumnRenamed("Name","EmpName")
# df.show()
new_cols = ["EmpName","EmpId","EmpSal"]
for col,new_col in zip(cols,new_cols):
    # print(col,new_col)
    df = df.withColumnRenamed(col,new_col)
df.show()
spark.stop()