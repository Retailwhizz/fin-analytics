from re import sub

import pandas as pd
from flask_restx import RestError
from src.common.constant import Constant

ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}


def camel_case(s):
    s = sub(r"(_|-)+", " ", s).title().replace(" ", "")
    return ''.join([s[0].lower(), s[1:]])


def read_file(file, file_type, skip_rows, delimiter=',', column_data_type_mappings=None):
    column_data_types = {}
    if column_data_type_mappings:
        type_mapping = Constant.COLUMN_DATA_TYPE_MAPPINGS
        for column, data_type in column_data_type_mappings.items():
            if data_type in type_mapping:
                column_data_types[column] = type_mapping[data_type]
    if file_type == 'xlsx' or file_type == 'xls':
        return pd.read_excel(file, skiprows=skip_rows, dtype=column_data_types)
    elif file_type == 'csv':
        return pd.read_csv(file, skiprows=skip_rows, dtype=column_data_types, delimiter=delimiter)


def map_template_headers_to_file_headers(template_headers, file_headers):
    mapped_header = {}
    for i in range(len(template_headers)):
        mapped_header[template_headers[i]] = file_headers[i]
    return mapped_header


def readRequestParams(req):
    retailer_id = req.headers.get('retailerId')
    distributor_id = req.headers.get('distributorId')
    entity_type = req.headers.get('role')
    if entity_type is None or entity_type.strip() == '':
        raise RestError('Missing entityType in the req')
    if entity_type == 'distributor' and (distributor_id is None or distributor_id.strip() == ''):
        raise RestError('Missing distributorId in the req')
    elif entity_type == 'retailer' and (retailer_id is None or retailer_id.strip() == ''):
        raise RestError('Missing retailerId in the req')
    elif entity_type == 'admin':
        return 1
    elif retailer_id:
        return retailer_id
    elif distributor_id:
        return distributor_id


def allowed_file(file_type):
    return file_type in ALLOWED_EXTENSIONS


def validateFile(file):
    return allowed_file(getFileType(file.filename))


def getFileType(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower()
