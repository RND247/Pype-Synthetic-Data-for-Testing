import logging
from io import BytesIO

import boto3
import pandas as pd
from botocore.exceptions import NoCredentialsError

BUCKET_NAME = "test-pype"
LOG_PATH = 'logs/s3_logs.log'

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler = logging.FileHandler(LOG_PATH)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


class S3Handler:
    def __init__(self, s3_client, bucket_name=BUCKET_NAME, divider_column=None):
        self.s3_handler = s3_client
        self.bucket_name = bucket_name
        self.divider_column = divider_column

    def upload(self, key, data):
        try:
            # If there's no divider, upload it as one file into the bucket
            if self.divider_column is None:
                self.s3_handler.put_object(Bucket=self.bucket_name, Key=key, Body=data.getvalue())
            else:
                partitions = self._divide_data_into_partitions(data)
                for partition, partition_data in partitions.items():
                    self.s3_handler.put_object(Bucket=self.bucket_name, Key=f"{partition}/{key}",
                                               Body=partition_data.getvalue())
            logger.info(f"Uploaded to S3: s3://{self.bucket_name}/{key}")
        except NoCredentialsError:
            logger.error("Failed to write to S3. Check your AWS credentials.")

    def _divide_data_into_partitions(self, data):
        partitions = {}
        df = pd.read_parquet(data, engine="pyarrow")
        grouped = df.groupby(pd.Grouper(key=self.divider_column))
        for group_name, group_data in grouped:
            parquet_buffer = BytesIO()
            group_data.to_parquet(parquet_buffer, engine="pyarrow")
            partitions[group_name] = parquet_buffer
        return partitions
