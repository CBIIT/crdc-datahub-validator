
import sys
import csv
import os
import shutil
import json
import requests
import yaml
import boto3 
import pandas as pd
import numpy as np
from bento.common.utils import get_stream_md5
from datetime import datetime
import uuid
from common.constants import DATA_COMMON, VERSION, LAST_MODIFIED

""" 
clean_up_key_value(dict)
Removes leading and trailing spaces from keys and values in a dictionary
:param: dict as dictionary
:return: cleaned dict
"""   
def clean_up_key_value(dict):
        
    return {key if not key else key.strip() if isinstance(key, str) else key : 
            value if not value else value.strip() if isinstance(value, str) else value for key, value in dict.items()}

"""
Removes leading and trailing spaces from header names
:param: str_arr as str array
:return: cleaned str array
"""
def clean_up_strs(str_arr):
       
    return [item.strip() for item in str_arr]

"""
Extract exception type name and message
:return: str
"""
def get_exception_msg():
    ex_type, ex_value, exc_traceback = sys.exc_info()
    return f'{ex_type.__name__}: {ex_value}'


"""
Dump list of dictionary to TSV file, caller needs handle exception.
:param: dict_list as list of dictionary
:param: file_path as str
:return: boolean
"""
def dump_dict_to_tsv(dict_list, file_path):
    if not dict_list or len(dict_list) == 0:
        return False 
    keys = dict_list[0].keys()
    with open(file_path, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=keys, delimiter='\t')
        dict_writer.writeheader()
        dict_writer.writerows(dict_list) 
    return True

"""
Dump list of dictionary to json file, caller needs handle exception.
:param: dict_list as list of dictionary
:param: file_path as str
:return: boolean
"""
def dump_dict_to_json(dict, file_path):
    if not dict or len(dict.items()) == 0:
        return False 
    for k, v in dict.items():
        path = file_path.replace("data", f'{k}')
        output_file = open(path, 'w', encoding='utf-8')
        json.dump(v, output_file, default=set_default) 
    return True

def set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError

def cleanup_s3_download_dir(dir):
    if os.path.exists(dir):
        for filename in os.listdir(dir):
            file_path = os.path.join(dir, filename)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
    else:
        os.makedirs(dir)

"""
Dump compare dict key ignore case.
:param: a dict
:param: k string 
:return: value
"""
def case_insensitive_get(a, k, default_value):
    k = k.lower()
    result = [a[key] for key in a if key.lower() == k]
    return result[0] if result and len(result) > 0 else default_value

"""
download file from url and load into dict 
:param: url string
:return: value dict
"""
def download_file_to_dict(url):
    # NOTE the stream=True parameter below
    file_ext = url.split('.')[-1]
    with requests.get(url) as r:
        if r.status_code > 400: 
            raise Exception(f"Can't find model file at {url}, {r.content}!")
        if file_ext == "json":
            return r.json()
        elif file_ext in ["yml", "yaml"]: 
            return yaml.safe_load(r.content)
        else:
            raise Exception(f'File type is not supported: {file_ext}!')
"""
get current datetime string in iso format
"""
def current_datetime_str():
    return datetime.now(tz = datetime.now().astimezone().tzinfo).isoformat(timespec='milliseconds')

"""
get current datetime
"""
def current_datetime():
    return datetime.now()
"""

get uuid v4
"""
def get_uuid_str():
    return str(uuid.uuid4())

    
"""
get MD5 and object size by object stream 
"""
def get_s3_file_info(bucket_name, key):
    s3 = None
    try:
        s3 = boto3.client('s3') 
        res = s3.head_object(Bucket=bucket_name, Key=key)
        size = res['ContentLength']
        last_updated = res[LAST_MODIFIED]
        return size, last_updated, 
    except Exception as e:
        raise e
    finally:
        s3 = None

"""
get MD5 and object size by object stream 
"""
def get_s3_file_md5(bucket_name, key):
    s3 = None
    try:
        s3 = boto3.client('s3') 
        response = s3.get_object(Bucket=bucket_name, Key=key) 
        object_data = response['Body'] 
        md5 = get_stream_md5(object_data)
        return md5
    except Exception as e:
        raise e
    finally:
        s3 = None

"""
create error dict
"""
def create_error(title, msg):
    return {"title": title, "description": msg}

"""
dataframe util to remove tailing empty rows and columns
"""
def removeTailingEmptyColumnsAndRows(df):
     # remove empty column from last 
    columns = df.columns.tolist()
    col_length = len(columns)
    index = col_length - 1
    while not columns[index] or  "Unnamed:" in columns[index]:
        if df["Unnamed: " + str(index)].notna().sum() == 0: 
            df = df.drop(df.columns[index], axis=1)
        else:
            break
        index -= 1
    # remove tailing empty row
    while  df.iloc[[-1]].isnull().all(1).values[0]:
        df = df.iloc[:-1]
    return df



