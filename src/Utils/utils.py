import os
from collections import defaultdict
from datetime import date
def convert_type(value, dtype):
    if dtype == date:
        return date.fromisoformat(value)
    if type(value) == str:
        value = str(value).strip().replace("'", '')
    if value is None or value in ('', 'None'):
        return None

    return dtype(value)

def open_file(file_path):
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    full_path = os.path.join(base_path, file_path)
    with open(full_path, 'r') as file:
        return file.read()
    

def convert_to_columnar(data):
        columnar_dict = defaultdict(list)

        for entry in data:
            for key, value in entry.items():
                columnar_dict[key].append(value)

        return dict(columnar_dict)