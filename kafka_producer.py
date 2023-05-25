from kafka import KafkaProducer

def publish_to_kafka(bootstrap_servers, topic, data):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    producer.send(topic, value=str(data).encode('utf-8'))
    producer.close()
