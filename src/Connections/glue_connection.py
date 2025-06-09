import boto3

class GlueConnection():
    def __init__(self, region_name: str = "us-east-1") -> None:
        self.region_name = region_name

    def connect(self):
        client = boto3.client(
            service_name='glue',
            region_name=self.region_name
        )
        return client