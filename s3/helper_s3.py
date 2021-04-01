import json
import csv
import pandas as pd 
import boto3
import os
import time
from datetime import datetime

cwd = os.getcwd()

def save_to_s3(s3_client,filepath,bucket_name,filename_in_s3):
    if os.path.isdir(filepath):
        dir_name = os.path.basename(filepath)
        for filename in os.listdir(filepath):
            if filename.endswith(".csv"):
                name = filename.split(".")[0]
                if name.isnumeric():
                    unix_timestamp = int(name)
                    local_time = time.localtime(unix_timestamp/1000)
                    timestamp = time.strftime("%Y-%m-%d_%H:%M:%S", local_time)
                    filename_in_s3 = dir_name + "/" + timestamp + ".csv"
                else:
                    filename_in_s3 = dir_name + "/" + filename
                path = os.path.join(filepath,filename)
                print(filename_in_s3)
                s3_client.upload_file(path,bucket_name,filename_in_s3)
            
    else:
        dir_name = os.path.basename(os.path.dirname(filepath))
        filename_in_s3 = dir_name + "/" + os.path.basename(filepath)
        print(filename_in_s3)
        s3_client.upload_file(filepath,bucket_name,filename_in_s3)

def load_from_s3(s3_client,filename_in_s3,local_file_path,bucket_name):
    s3_client.download_file(bucket_name,filename_in_s3,local_file_path)
    return(pd.read_csv(local_file_path))

def start_s3_client(config):
    s3_client = boto3.client('s3' ,config['aws_access']["region_name"],
    aws_access_key_id=config['aws_access']["aws_access_key_id"],
    aws_secret_access_key=config['aws_access']["aws_secret_access_key"])
    return s3_client

def read_config(file_path):
    with open(file_path, 'r') as f:
        return(json.load(f))

if __name__ == "__main__":
    conf = read_config("s3_config.json")
    bucket_name = conf['aws_access']['bucket_name']
    s3_client = start_s3_client(conf)
    
    PREFIX = 'metrics/'

    get_last_modified = lambda obj: int(obj['LastModified'].timestamp())
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=PREFIX)['Contents']
   
    for object in response:
        print(object['Key'])
       