from SQL.query import querys
from Repositories.athena_repository import AthenaRepository
from Repositories.s3_repository import S3Repository
from Services.crawler_service import CrawlerService
from Controller.map_data_types_controller import MapDataTypesController
from Utils.utils import convert_type,convert_to_columnar
import pyarrow as pa
import pyarrow.parquet as pq
from s3fs import S3FileSystem
from io import StringIO
import csv
import time
import os
from botocore.exceptions import ClientError


class uploadProcess():
    def __init__(self) -> None:
        self.athena = AthenaRepository()
        self.s3 = S3Repository()
        self.crawler = CrawlerService()

    def process_data(self, dt_string: str):
        the_query = querys.query(dt_string)
        result_location_query_athena = self.athena.get_query(the_query)
        bucket_name = result_location_query_athena.split('/')[2]
        key = '/'.join(result_location_query_athena.split('/')[3:])
        
        max_attempts = 5
        attempt = 0
        while attempt < max_attempts:
            try:
                print(f'Tentando baixar o arquivo do S3 (tentativa {attempt + 1}/{max_attempts})')
                print(f'key:{key}')
                s3_object = self.s3.get_object(bucket_name,key)
                csv_content = s3_object['Body'].read()

                csv_buffer = StringIO(csv_content.decode('utf-8'))
                csv_reader = csv.DictReader(csv_buffer)
                data_types = MapDataTypesController().map_data_types()
                break

            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    attempt += 1
                    if attempt < max_attempts:
                        print('Arquivo não encontrado. Tentando novamente em 10 segundos...')
                        time.sleep(10)
                    else:
                        print(f'Erro: Arquivo não encontrado após {max_attempts} tentativas.')
                        raise FileNotFoundError(f'Arquivo {key} não encontrado no bucket {bucket_name}')

        values_with_errors = []
        values_with_success = []

        data_ranking_dict = [row for row in csv_reader]


        for row in data_ranking_dict:

            try:
                for key, value in row.items():
                    if key in data_types:
                        row[key] = convert_type(value, data_types[key])
                    else:
                        raise Exception(f"ERROR: Error on line '{row['n']}' -> Key '{key}' not found in data_types")

                values_with_success.append(row)
            except Exception as error:
                error_name = f"ERROR: Error on line '{row.get('n', 'None')}' when converting value '{value}' for key '{key}' -> {error}"

                row['error'] = error_name
                values_with_errors.append(row)


        print('Convertendo o CSV lido para pyarrow.Table')
        table_name = os.environ.get('TABLE_NAME')
        year = dt_string.split('-')[0]
        month = dt_string.split('-')[1]


        bucket_name = os.environ.get('BUCKET_NAME')
        destination_path = f's3://{bucket_name}/{table_name}/ano_ref={year}/mes_ref={int(month)}/{table_name}.parquet'

        self.process_success_data(values_with_success,destination_path)

        print("Arquivo enviado para o S3 com sucesso!")
        time.sleep(15)
        print('Rodando crawler')
        self.crawler.run()


    def process_success_data(self, values_with_success,destination_path):
        print('     Processing success data', end='...')
        data = convert_to_columnar(values_with_success)
        table = pa.Table.from_pydict(data)
        self.write_parquet(table, destination_path)
        print('[ OK ]')


    def write_parquet(self, table, destination_path):
        pq.write_table(
            table,
            destination_path,
            filesystem= S3FileSystem(),
            compression='snappy'
        )