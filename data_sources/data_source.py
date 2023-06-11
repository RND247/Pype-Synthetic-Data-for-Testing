import uuid
from abc import ABC, abstractmethod
from functools import partial
from io import BytesIO
from multiprocessing import Pool
from typing import List, Dict

import yaml

from generator import DataGenerator

import pandas as pd

from develop.s3_handler import S3Handler


class DataSource(ABC):
    def __init__(self, s3_bucket):
        # Initialize S3 client
        self.s3_bucket = s3_bucket

    @abstractmethod
    def read_data_into_s3(self, file_size):
        pass

    @abstractmethod
    def create_intermediate_data(self, num_processes):
        pass

    def _write_df_to_data_source(self, df, **kwargs):
        pass

    def write_parquet_to_data_source(self, parquet_path, **kwargs):
        df = pd.read_parquet(parquet_path, engine="pyarrow")
        self._write_df_to_data_source(df)

    def write_dfs_to_data_source(self, dfs, num_processes=1, **kwargs):
        pool = Pool(num_processes)

        try:
            # Start multiple instances of the consumer function in the process pool
            pool.map(partial(self._write_df_to_data_source, **kwargs), dfs)
        except KeyboardInterrupt:
            # Terminate the pool upon interrupt
            pool.terminate()
        finally:
            # Close the pool
            pool.close()
            pool.join()

    def write_parquets_to_data_source(self, parquet_paths, num_processes=1, **kwargs):
        dfs = [pd.read_parquet(parquet_path, engine="pyarrow") for parquet_path in parquet_paths]
        self.write_dfs_to_data_source(dfs, num_processes=num_processes, **kwargs)

    def write_to_s3(self, data: List[Dict], s3, divider_column=None, is_synthetic=False, config_yml_path=None):
        s3_handler = S3Handler(bucket_name=self.s3_bucket, s3_client=s3, divider_column=divider_column)
        # Convert data into parquet
        df = pd.DataFrame(data)
        if is_synthetic:
            with open(config_yml_path, 'r') as file:
                column_config = yaml.safe_load(file)
            dg = DataGenerator(df, column_config)
            df = dg.generate_data()
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, engine="pyarrow")

        # Upload to s3
        s3_handler.upload(f"data_{str(uuid.uuid4())}.parquet", parquet_buffer)
