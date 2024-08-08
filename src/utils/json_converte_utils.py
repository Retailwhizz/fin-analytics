import json

import numpy as np
import pandas as pd
import datetime
from src.utils.common_util import camel_case
from src.utils.logger import logger


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if pd.isnull(obj):
            return None
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(obj, pd.Timestamp):
            return obj.to_pydatetime().strftime('%Y-%m-%d %H:%M:%S')
        return super(NpEncoder, self).default(obj)


def convert_data_chunks_to_json(file, chunk_size, mapping, file_type, delimiter=','):
    skip_rows = 0
    row_id = int(0)
    chunk_number = 0
    chunk_map = {}
    if file_type == 'csv':
        df = pd.read_csv(file, chunksize=chunk_size, delimiter=delimiter)
        for df_chunk in df:
            converted_df_chunk = df_chunk.replace(np.nan, '', regex=True)
            logger.info("Converted df chunk: {} ".format(converted_df_chunk))
            map_chunks_to_headers(converted_df_chunk, mapping, chunk_map, chunk_number, row_id)
            chunk_number += 1
            row_id += chunk_size
    else:
        df = pd.read_excel(file)
        total_row_count = df.shape[0]
        headers = df.columns.to_list()
        
        # while row_id < total_row_count:
        #     df_chunk = pd.read_excel(file, nrows=chunk_size, skiprows=skip_rows, names=headers)
        #     converted_df_chunk = df_chunk.replace(np.nan, '', regex=True)
        #     map_chunks_to_headers(converted_df_chunk, mapping, chunk_map, chunk_number, row_id)
        #     chunk_number += 1
        #     print("Row Id is", row_id)
        #     skip_rows += chunk_size
        #     row_id += chunk_size


        now = datetime.datetime.now()
        while row_id < total_row_count:
            df_chunk = df.iloc[row_id:row_id + chunk_size].fillna('')
            map_chunks_to_headers(df_chunk, mapping, chunk_map, chunk_number, row_id)
            chunk_number += 1
            row_id = row_id + chunk_size
        later = datetime.datetime.now()
        difference = (later - now).total_seconds()
        logger.info("Total row count %s and number of chunks %s", total_row_count, chunk_number)          
        logger.info("Total time taken by code in convert chunk to json is %s" , difference) 

    return chunk_map


def map_chunks_to_headers(converted_df_chunk, mapping, chunk_map, chunk_number, row_id):
    if not converted_df_chunk.shape[0]:
        return
    else:
        json_converted_list = []
        for idx in converted_df_chunk.index:
            obj = {}
            for key, value in mapping.items():
                obj["rowId"] = row_id
                logger.info("key, value : %s, %s",camel_case(key),value)
                try:
                    logger.info("map_chunks_to_headers idx : {}".format(idx))
                    obj[camel_case(key)] = converted_df_chunk[str(value).rstrip()][idx]
                    logger.info("map_chunks_to_headers key : %s, %s",camel_case(key),str(value).rstrip())
                except Exception as e:
                    logger.error("Something went wrong in map_chunks_to_headers {} ".format(e.__cause__))
                    logger.error(e)
                    continue

            row_id = row_id + 1
            json_converted_list.append(obj)
    chunk_map[chunk_number] = json_converted_list
