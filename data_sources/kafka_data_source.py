import json
import time
from multiprocessing import Pool, Manager
from functools import partial
from utils.shared_memory_manager import SharedMemoryManager

import boto3
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

from data_sources.data_source import DataSource

# Kafka broker configuration
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'my-topic-bb'


class KafkaDataSource(DataSource):
    def __init__(
        self,
        kafka_endpoint,
        kafka_topic,
        s3_bucket,
        read_timeout_secs=300,
        batch_size=1,
        decode_type="utf-8",
    ):
        super().__init__(s3_bucket=s3_bucket)
        self.kafka_endpoint = kafka_endpoint
        self.kafka_topic = kafka_topic
        self.read_timeout_secs = read_timeout_secs
        self.batch_size = batch_size
        self.decode_type = decode_type

    def read_data_into_s3(self, file_size=1024 * 1024, divider_column=None, is_synthetic=False, config_yml_path=None,
                          shared_memory=None):
        # Configure Kafka consumer
        consumer_conf = {
            "bootstrap.servers": self.kafka_endpoint,
            "group.id": "pype-group",
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
                messages = consumer.consume(num_messages=self.batch_size, timeout=1.0)

                if not messages:
                    print(f"{file_size}: No messages to read")
                    continue
                for message in messages:
                    print(f"{file_size}: got messages")
                    if message.error():
                        if message.error().code() == KafkaError._PARTITION_EOF:
                            # End of partition event
                            break
                        else:
                            continue

                    data = json.loads(message.value().decode(self.decode_type))
                    data_size = len(data)

                    # Check if the data will exceed the file size limit (1MB)
                    if current_file_size + data_size > file_size:
                        self.write_to_s3(current_file_data, s3, divider_column, is_synthetic=is_synthetic,
                                         config_yml_path=config_yml_path, shared_memory=shared_memory)
                        current_file_data = []
                        current_file_size = 0

                    # Add data to the current file
                    current_file_data.append(data)
                    current_file_size += data_size

        finally:
            consumer.close()

            # Write any remaining data to S3
        if current_file_data:
            self.write_to_s3(current_file_data, s3, divider_column, is_synthetic=is_synthetic,
                             config_yml_path=config_yml_path, shared_memory=shared_memory)

    def create_intermediate_data(self, num_processes=1, divider_column=None, is_synthetic=False, config_yml_path=None):
        pool = Pool(num_processes)

        try:
            if is_synthetic:
                shared_memory = SharedMemoryManager()
            else:
                shared_memory = None
            # Start multiple instances of the consumer function in the process pool
            partial_func = partial(self.read_data_into_s3, divider_column=divider_column, is_synthetic=is_synthetic,
                                   config_yml_path=config_yml_path, shared_dict=shared_memory)
            pool.map(partial_func, [1024*1024]*num_processes)
        except KeyboardInterrupt:
            # Terminate the pool upon interrupt
            pool.terminate()
        finally:
            # Close the pool
            pool.close()
            pool.join()

    def _write_df_to_data_source(self, df: pd.DataFrame, topic=None, **kwargs):
        topic = topic or self.kafka_topic
        producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS})
        admin_client = AdminClient({'bootstrap.servers': BOOTSTRAP_SERVERS})
        new_topic = NewTopic(topic, num_partitions=3, replication_factor=1)

        admin_client.create_topics([new_topic])
        for row in range(df.shape[0]):
            json_data = json.dumps(df.loc[row].to_dict())
            producer.produce(topic, value=json_data.encode('utf-8'))
            producer.flush()
