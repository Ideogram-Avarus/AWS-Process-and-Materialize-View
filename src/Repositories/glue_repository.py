from Connections.glue_connection import GlueConnection

class GlueRepository():
    def __init__(self, crawler_name) -> None:
        self.client = GlueConnection().connect()
        self.crawler_name = crawler_name

    def start_crawler(self,):
        self.client.start_crawler(Name=self.crawler_name)
        print(f'Crawler {self.crawler_name} iniciado com sucesso.')

    def get_crawler(self,):
        response = self.client.get_crawler(Name=self.crawler_name)
        return response