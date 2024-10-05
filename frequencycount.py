from pyspark.sql import SparkSession
from pyspark.sql.functions import count,when,col
spark = SparkSession.builder.master("local[*]").appName("PysparkTest").getOrCreate()

data = [('John','Engineer'),
        ('John','Engineer'),
        ('Mary','Scientist'),
        ('Bob','Engineer'),
        ('Bob','Engineer'),
        ('Bob','Scientist'),
        ('Sam','Doctor')
        ]
df = spark.createDataFrame(data,['Name','Job'])
col_name = 'Job'
df2 = df.groupBy(col_name).agg(count(col_name).alias('frequency')).orderBy('frequency',ascending=False).limit(2)
# l1 = list(df2["Job"])
# print(l1)
# df2.show()
df3 = df.join(df2,on="Job",how="left")
# df4 = df3.withColumn("Jobs",when(col("frequency").isNull(),'Others').otherwise(col("Job")))
df5 = df3.select("Name",when(col("frequency").isNull(),'Others').otherwise(col("Job")).alias("Jobs"))
# df4.show()
df5.show()
spark.stop()