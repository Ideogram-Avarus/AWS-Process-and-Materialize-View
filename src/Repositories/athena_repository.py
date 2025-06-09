import time
import os

from Connections.athena_connection import AthenaConnection

class AthenaRepository():
    def __init__(self):
        connection = AthenaConnection()
        self.client = connection.client
        self.output_location = os.environ.get('ATHENA_OUTPUT_LOCATION')

    def get_query(self, query):
        response = self.client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': os.environ.get('ATHENA_DATABASE')
        },
        ResultConfiguration={
            'OutputLocation': self.output_location,
        }
    )
        query_execution_id = response['QueryExecutionId']

        query_status = None
        while True:
            query_status_response = self.client.get_query_execution(QueryExecutionId=query_execution_id)
            query_status = query_status_response['QueryExecution']['Status']['State']
            if query_status == 'FAILED' or query_status == 'CANCELLED':
                raise Exception(f"Query {query_status}: {query_status_response['QueryExecution']['Status']['StateChangeReason']}")
            if query_status == 'SUCCEEDED':
                break
            
            time.sleep(10)
    
        result_location = f"{self.output_location}{query_execution_id}.csv"

        return result_location