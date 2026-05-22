
import sys
import csv
import os
import re
import shutil
import json
import requests
import yaml
import boto3 
import numpy as np
from bento.common.utils import get_stream_md5
from datetime import datetime
import uuid
from common.constants import QC_SEVERITY, LAST_MODIFIED, PROPERTY_PERMISSIBLE_VALUES

VALIDATION_MESSAGE_CONFIG_FILE = "configs/messages_configuration.yml"
VALIDATION_MESSAGES = "Messages"
MESSAGE = "message"
MESSAGE_TITLE = "title"
MESSAGE_CODE = "code"

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
    file_ext = url.split('.')[-1] if url and '.' in url else None
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

def get_date_time(format = "%Y-%m-%dT%H%M%S"):
    """
    get current time in format
    """  
    return datetime.strftime(datetime.now(), format)

def convert_date_time(date, format = "%Y-%m-%dT%H%M%S"):
    """
    convert date to format
    """
    return datetime.strftime(date, format)

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
get s3 file size and last modified
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
def create_error(code, msg, property_name, property_value):
    message_config = load_message_config()
    if message_config:
        error_config = message_config.get(code)
        if error_config:
            title = error_config.get(MESSAGE_TITLE)
            severity = error_config.get(QC_SEVERITY)
            # if msg is list of args, use configured template, else use the message passed in.
            msg = error_config[MESSAGE].format(*msg) if isinstance(msg, list) and error_config.get(MESSAGE) else msg
    return {MESSAGE_CODE: code, QC_SEVERITY: severity, MESSAGE_TITLE: title, "offendingProperty": property_name, "offendingValue": property_value, "description": msg, }
"""
Load message config from yaml file
"""
def load_message_config():
    if not hasattr(load_message_config, 'message_config'):
        messages = load_yaml_to_dict(VALIDATION_MESSAGE_CONFIG_FILE)
        if messages:
            load_message_config.message_config = messages.get(VALIDATION_MESSAGES)
    return load_message_config.message_config

"""
load yaml file to dict
"""
def load_yaml_to_dict(file_path):
    if not os.path.isfile(file_path):
        return None
    with open(file_path, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            return None
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

def dict_exists_in_list(dict_list, target_dict, keys=None):
    """
    Check if a dictionary exists in a list of dictionaries
    :param dict_list: List of dictionaries
    :param target_dict: Dictionary to search for
    :param keys: List of keys to compare (optional)
    :return: Boolean indicating if the dictionary exists in the list
    """
    for d in dict_list:
        if keys:
            # Only compare specific keys
            if all(d.get(key) == target_dict.get(key) for key in keys):
                return True
        else:
            # Compare the whole dictionary
            if d == target_dict:
                return True
    return False

def is_valid_uuid(uuid_to_test, version=5):
    """
    Check if uuid_to_test is a valid UUID.
    
     Parameters
    ----------
    uuid_to_test : str
    version : {1, 2, 3, 4}
    
     Returns
    -------
    `True` if uuid_to_test is a valid UUID, otherwise `False`.
    
     Examples
    --------
    >>> is_valid_uuid('c9bf9e57-1685-4c89-bafb-ff5af830be8a')
    True
    >>> is_valid_uuid('c9bf9e58')
    False
    """
    
    try:
        uuid_obj = uuid.UUID(uuid_to_test, version=version)
        uuid_str = str(uuid_obj)
        if uuid_str == uuid_to_test:
            return True
        else:
            return is_valid_uuid(uuid_to_test, int(version)-1)    
    except ValueError:
        return False
    
def validate_uuid_by_rex(value):
    """
    Validate UUID by regular expression
    :param value: value to be validated
    :return: boolean
    """
    
    if not value:
        return False
    rex = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    return validate_by_rex(value, rex)

def validate_by_rex(value, rex):
    """
    Validate value by regular expression
    :param value: value to be validated
    :param rex: regular expression
    :return: boolean
    """
    if not value or not rex:
        return False
    return re.match(rex, value) is not None

def convert_file_size(size_bytes):
    """
    Convert file size to human-readable format
    :param size_bytes: size in bytes
    :return: string
    """
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(np.floor(np.log(size_bytes) / np.log(1024)))
    p = np.power(1024, i)
    s = round(size_bytes / p, 2)
    return "%s %s" % (s, size_name[i])

def has_permissive_value(prop):
    """
    check if the property has permissive values
    """
    if prop.get(PROPERTY_PERMISSIBLE_VALUES) is not None:
        if len(prop[PROPERTY_PERMISSIBLE_VALUES]) > 0: 
            return True, prop[PROPERTY_PERMISSIBLE_VALUES]
    return False, None

