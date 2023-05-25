from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

def process_streaming_data(bootstrap_servers, topic):
    spark = SparkSession.builder.appName("StockDashboard").getOrCreate()

    # Define the schema for the stock data
    schema = "value string"

    # Read the streaming data from Kafka
    df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", bootstrap_servers).option("subscribe", topic).load()

    # Apply the schema and convert the value column from Kafka to JSON format
    df = df.withColumn("value", col("value").cast("string"))
    df = df.withColumn("jsonData", from_json("value", schema))

    # Extract the relevant fields from the JSON data
    df = df.select("jsonData.*")

    # Perform any desired transformations or computations on the streaming data
    
    # Write the processed data to the desired sink

    query = df.writeStream...

    # Start the streaming query
    query.start().awaitTermination()
