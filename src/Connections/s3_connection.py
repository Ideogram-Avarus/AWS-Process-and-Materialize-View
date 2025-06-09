import boto3
from botocore.client import Config

class S3Connection():
    def __init__(self, region_name: str = "us-east-1") -> None:
        self.region_name = region_name
        self.client = self.get_client()


    def get_client(self):
        config = Config(read_timeout=900, connect_timeout=900, retries={'max_attempts': 3})
        session = boto3.session.Session()
        client = session.client(
            service_name='s3',
            region_name=self.region_name,
            config=config
        )
        return client