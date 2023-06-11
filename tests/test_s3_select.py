from src.query_engine.s3_select import S3Select


def test_s3_select():
    # Example usage
    s3_select = S3Select(
        bucket_name='pype-yuval-test',
        aws_region='eu-central-1'
    )

    key = 'pype-test/myfile1.parquet'
    query_expression = 'SELECT * FROM s3object where age > 26'
    results = s3_select.execute_query('pype-test/myfile1.parquet', query_expression)
    assert len(results) == 2, f"Expected len to be 2, value was {len(results)}"
