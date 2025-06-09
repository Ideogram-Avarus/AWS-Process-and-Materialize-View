import os
from Services.upload import uploadProcess
from datetime import datetime
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta

def lambda_handler(event, context):
    print('Código iniciado')
    now = datetime.now()
    data_1_mes_atras = now - relativedelta(months=1)
    data_1_mes_atras = data_1_mes_atras.strftime("%Y-%m-%d")
    dt_string = event.get('data_ref') if event.get('month') else data_1_mes_atras
    uploadProcess().process_data(dt_string)


if __name__ == "__main__":
    lambda_handler({}, None)