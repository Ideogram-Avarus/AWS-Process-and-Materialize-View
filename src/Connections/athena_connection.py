import boto3
from botocore.client import Config

class AthenaConnection():
    def __init__(self, region_name: str = "us-east-1") -> None:
        self.region_name = region_name
        self.client = self.get_client()


    def get_client(self):
        session = boto3.session.Session()
        client = session.client(
            service_name='athena',
            region_name=self.region_name,
        )
        return client