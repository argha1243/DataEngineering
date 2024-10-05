from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.master("local[*]").appName("PySparkLocalExample").getOrCreate()

# Create a simple DataFrame
data = [("John", 28), ("Jane", 35), ("Sam", 22)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# Stop the SparkSession
# spark.stop()
