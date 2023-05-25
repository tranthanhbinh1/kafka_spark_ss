import finnhub, time, json
from kafka import KafkaProducer



# Setup finnhub api
api_key = "chbqth1r01quf00vuei0chbqth1r01quf00vueig"
finnhub_client = finnhub.Client(api_key=api_key)
symbols = ['AAPL', 'AMZN', 'GOOG', 'MSFT', 'TSLA', 'NVDA', 'NFLX']

# Calculate the start and end timestamps for a 1-minute interval
end_timestamp = int(time.time())
start_timestamp = end_timestamp - 60

# Convert the timestamps to the required format for the function call
start_timestamp = int(start_timestamp)
end_timestamp = int(end_timestamp)

# Create a Kafka Producer instance
bootstrap_servers = "localhost:29092"
topic = "stock_analyzer"
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Publish data to Kafka
def publish_to_kafka(topic, data):
    producer.send(topic, data.encode('utf-8'))
    producer.flush()

# Call the API function and publish data to Kafka
while True:
    for symbol in symbols:
        res = finnhub_client.stock_candles(symbol, 'D', start_timestamp, end_timestamp)
        data = json.dumps(res)
        publish_to_kafka(topic, data)
    time.sleep(10)


