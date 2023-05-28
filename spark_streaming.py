from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import from_json, col

# Define the Kafka broker and topic
bootstrap_server = "localhost:29092"
topic = "stock_analyzer"

spark = SparkSession.builder.appName("StockDashboard").getOrCreate()

# Define the schema for the stock data
schema = "value string"

# # Read the streaming data from Kafka
# df = spark.readStream.format("kafka")\
#     .option("kafka.bootstrap.servers", bootstrap_server)\
#     .option("subscribe", topic).load()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", bootstrap_server) \
  .option("subscribe", topic) \
  .load()

# Convert the value column from binary to string
df = df.withColumn("value", df["value"].cast("string"))

# Print the data to the console
query = df.writeStream.outputMode("append").format("console").start()

# Wait for the termination of the streaming query
query.awaitTermination()


# # Apply the schema and convert the value column from Kafka to JSON format
# df = df.withColumn("value", col("value").cast("string"))
# df = df.withColumn("jsonData", from_json("value", schema))

# # # Extract the relevant fields from the JSON data
# # df = df.select("jsonData.*")

# # Transformation
# df = df.selectExpr("s", "explode(c) as c", "explode(h) as h", "explode(l) as l", "explode(o) as o", "explode(t) as t", "explode(v) as v")

# # Write the processed data to the desired sink

# # query = df.writeStream.format("json") \
# #     .option("path", "stock_data.json") \
# #     .option("checkpointLocation", "checkpoint") \
# #     .start()

# query = df.writeStream.format("console").start()

# # Start the streaming query
# query.start().awaitTermination()
