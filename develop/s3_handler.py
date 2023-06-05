import boto3
from botocore.exceptions import NoCredentialsError
import logging

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
    def __init__(self, bucket_name=BUCKET_NAME):
        self.s3_handler = boto3.client("s3")
        self.bucket_name = bucket_name

        # Configure the logging

    def upload(self, key, data):
        try:
            self.s3_handler.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=data
            )
            logger.info(f"Uploaded to S3: s3://{self.bucket_name}/{key}")
        except NoCredentialsError:
            logger.error("Failed to write to S3. Check your AWS credentials.")

    def upload_file(self, file_path, file_name):
        with open(file_path, "rb") as f:
            self.s3_handler.upload_fileobj(f, self.bucket_name, file_name)
