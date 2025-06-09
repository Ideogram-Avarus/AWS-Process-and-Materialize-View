from Utils.utils import open_file

class querys():

    def query(data_ref):
        query = open_file('SQL/ocorrencias_mensais.sql').format(dt_string=data_ref)


        from pprint import pprint
        pprint(query)
        return query