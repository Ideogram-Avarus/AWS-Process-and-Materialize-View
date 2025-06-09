from datetime import date

class MapDataTypesController:
    def __init__(self) -> None:
        pass

    def map_data_types(self):
        data_types = {
            'id_funcionario': str,
            'nm_funcionario': str,
            'id_funcao': str,
            'nm_funcao': str,
            'cd_funcao': str,
            'id_empresa': str,
            'nm_empresa': str,
            'nr_cpf': str,
            'nr_cracha': str,
            'dt_data': date,
            'vl_beneficio': float,
            'nr_valor': float,
            'faltas': int,
            'atestado': int,
            'suspensao': int,
        }

        return data_types

