from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import from_json, col

# Define the Kafka broker and topic
bootstrap_server = "localhost:29092"
topic = "stock_analyzer"

spark = SparkSession.builder.appName("StockDashboard").getOrCreate()

# Define the schema for the stock data
schema = "value string"

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

