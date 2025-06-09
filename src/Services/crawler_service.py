from time import sleep
from Repositories.glue_repository import GlueRepository
import os

class CrawlerService():
    def __init__(self) -> None:
        self.crawler_name = os.environ.get('CRAWLER_NAME')
        self.glue_repository = GlueRepository(crawler_name=self.crawler_name)

    def run(self, ):
        try:
            self.glue_repository.start_crawler()
        except Exception as error:
            raise Exception(f'Erro ao executar o Crawler: {str(error)}')

        # self.run_monitoring()

    def run_monitoring(self, ):
        try:
            # Verificando o status do Crawler até que ele termine
            while True:
                response = self.glue_repository.get_crawler()
                status = response['Crawler']['State']
                print(f'    Status: {status}')

                if status == 'READY':
                    print(f'Crawler {self.crawler_name} concluído com sucesso.')
                    break

                if status == 'FAILED':
                    raise Exception(f'Falha ao executar o Crawler {self.crawler_name}.')

                sleep(40)

        except Exception as error:
            raise Exception(f'Erro ao monitorar o Crawler: {str(error)}')