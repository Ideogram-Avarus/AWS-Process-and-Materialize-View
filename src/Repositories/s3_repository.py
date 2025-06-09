import json

from Connections.s3_connection import S3Connection

class S3Repository():
    def __init__(self):
        connection = S3Connection()
        self.client = connection.client

    def put_object(self, bucket_name, key, json_object):
        self.client.put_object(
            Body=bytes(json.dumps(json_object, default=str).encode('UTF-8')),
            Bucket=bucket_name,
            Key=key
        )

    def get_object(self,bucket_name,key):
        response = self.client.get_object(
            Bucket=bucket_name,
            Key=key
        )
        return response