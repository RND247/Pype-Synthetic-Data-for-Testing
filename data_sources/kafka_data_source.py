import json
import time
from multiprocessing import Pool

import boto3
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from confluent_kafka import Producer

from data_sources.data_source import DataSource

# Kafka broker configuration
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'my-topic'


class KafkaDataSource(DataSource):
    def __init__(
        self,
        kafka_endpoint,
        kafka_topic,
        s3_bucket,
        read_timeout_secs=300,
        batch_size=1024,
        decode_type="utf-8",
    ):
        super().__init__(s3_bucket=s3_bucket)
        self.kafka_endpoint = kafka_endpoint
        self.kafka_topic = kafka_topic
        self.read_timeout_secs = read_timeout_secs
        self.batch_size = batch_size
        self.decode_type = decode_type

    def read_data_into_s3(self, file_size=1024 * 1024, divider_column=None):
        # Configure Kafka consumer
        consumer_conf = {
            "bootstrap.servers": self.kafka_endpoint,
            "group.id": "tdsd-group",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_conf)
        consumer.subscribe([self.kafka_topic])

        # Initialize variables
        current_file_size = 0
        current_file_data = []
        s3 = boto3.client('s3')

        start_time = time.time()
        try:
            while (time.time() - start_time) < self.read_timeout_secs:
                messages = consumer.consume(self.batch_size, timeout=1.0)

                if not messages:
                    continue
                for message in messages:
                    if message.error():
                        if message.error().code() == KafkaError._PARTITION_EOF:
                            # End of partition event
                            break
                        else:
                            print(f"Error: {message.error().str()}")
                            continue

                    data = json.loads(message.value().decode(self.decode_type))
                    data_size = len(data)

                    # Check if the data will exceed the file size limit (1MB)
                    if current_file_size + data_size > file_size:
                        self.write_to_s3(current_file_data, s3, divider_column)
                        current_file_data = []
                        current_file_size = 0

                    # Add data to the current file
                    current_file_data.append(data)
                    current_file_size += data_size

        finally:
            consumer.close()

            # Write any remaining data to S3
        if current_file_data:
            self.write_to_s3(current_file_data, s3, divider_column)

    def create_synthetic_data(self, num_processes=1, divider_column=None):
        pool = Pool(num_processes)

        try:
            # Start multiple instances of the consumer function in the process pool
            pool.map(lambda x: self.read_data_into_s3(divider_column=divider_column), range(num_processes))
        except KeyboardInterrupt:
            # Terminate the pool upon interrupt
            pool.terminate()
        finally:
            # Close the pool
            pool.close()
            pool.join()

    def _write_df_to_data_source(self, df: pd.DataFrame, topic=TOPIC, **kwargs):
        producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})
        for row in range(df.shape[0]):
            json_data = json.dumps(df.loc[row].to_dict())
            producer.produce(topic, value=json_data.encode('utf-8'))
            producer.flush()
