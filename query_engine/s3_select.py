import boto3
import json
import pandas as pd


class S3Select:
    def __init__(self, bucket_name, aws_region):
        self.bucket_name = bucket_name
        self.aws_region = aws_region

    def execute_query(self, key, query_expression):
        session = boto3.Session(
            region_name=self.aws_region
        )

        s3_client = session.client('s3')
        response = s3_client.select_object_content(
            Bucket=self.bucket_name,
            Key=key,
            ExpressionType='SQL',
            Expression=query_expression,
            InputSerialization={
                'Parquet': {
                }
            },
            OutputSerialization={
                'JSON': {}
            }
        )

        for event in response['Payload']:
            if 'Records' in event:
                records = event['Records']['Payload'].decode('utf-8')
                records = records.split("\n")[0:-1]
                json_list = list(map(lambda x: json.loads(x), records))
                return pd.DataFrame(json_list)
            elif 'Stats' in event:
                stats_details = event['Stats']['Details']
                # Handle stats details as needed
                print(stats_details)

    def execute_query_for_prefix(self, prefix, query_expression, limit=None):
        session = boto3.Session(
            region_name=self.aws_region
        )
        s3_client = session.client('s3')

        response = s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        results = None

        for object_key in response['Contents']:
            key = object_key['Key']
            if key.endswith('.parquet'):
                if results is None:
                    results = self.execute_query(key, query_expression)
                else:
                    results = pd.concat([results, self.execute_query(key, query_expression)], ignore_index=True)
                if limit and len(results) >= limit:
                    return results
        return results


if __name__ == '__main__':
    # Example usage
    s3_select = S3Select(
        bucket_name='pype-yuval-test',
        aws_region='eu-central-1'
    )

    key = 'pype-test/myfile1.parquet'
    query_expression = 'SELECT * FROM s3object where age > 26'
    s3_select.execute_query_for_prefix('pype-test', query_expression)