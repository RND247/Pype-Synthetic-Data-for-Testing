from abc import ABC, abstractmethod
from typing import List, Dict

from develop.s3_handler import S3Handler
import uuid
import pandas as pd
from io import BytesIO


class DataSource(ABC):
    def __init__(self, s3_bucket):
        # Initialize S3 client
        self.s3_bucket = s3_bucket

    @abstractmethod
    def read_data_into_s3(self, file_size):
        pass

    @abstractmethod
    def create_synthetic_data(self, num_processes):
        pass

    def write_to_s3(self, data: List[Dict], s3, divider_column):
        s3_handler = S3Handler(bucket_name=self.s3_bucket, s3_client=s3, divider_column=divider_column)
        # Convert data into parquet
        df = pd.DataFrame(data)
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, engine="pyarrow")

        # Upload to s3
        s3_handler.upload(f"data_{str(uuid.uuid4())}.json", parquet_buffer)