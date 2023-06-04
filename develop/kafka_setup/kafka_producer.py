from confluent_kafka import Producer
import json

# Kafka broker configuration
bootstrap_servers = 'localhost:9092'
topic = 'my-topic'

# Create Kafka producer
producer = Producer({'bootstrap.servers': bootstrap_servers})

# Sample JSON messages
messages = [
    {'name': 'John', 'age': 30},
    {'name': 'Alice', 'age': 25},
    {'name': 'Bob', 'age': 35}
]

# Produce JSON messages to Kafka topic
for message in messages:
    # Serialize message as JSON
    json_message = json.dumps(message)

    # Produce message to Kafka topic
    producer.produce(topic, value=json_message.encode('utf-8'))

    # Flush producer to ensure message delivery
    producer.flush()

print('Messages produced successfully')