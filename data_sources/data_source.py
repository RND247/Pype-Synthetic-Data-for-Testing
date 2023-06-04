from abc import ABC, abstractmethod
from typing import List, Dict

import boto3
from botocore.exceptions import NoCredentialsError
import uuid
import pandas as pd
from io import BytesIO


class DataSource(ABC):
    def __init__(self, s3_bucket):
        self.s3_bucket = s3_bucket

    @abstractmethod
    def read_data_into_s3(self, file_size):
        pass

    def write_to_s3(self, data: List[Dict]):
        # Initialize S3 client
        s3 = boto3.client("s3")

        # Convert data into parquet
        df = pd.DataFrame(data)
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, engine="pyarrow")

        try:
            s3.put_object(
                Body=parquet_buffer,
                Bucket=self.s3_bucket,
                Key=f"data_{str(uuid.uuid4())}.parquet",
            )
        except NoCredentialsError:
            print("Failed to write to S3. Check your AWS credentials.")
