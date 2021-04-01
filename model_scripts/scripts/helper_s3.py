import json
import csv
import pandas as pd 
import boto3
import os
import time

def save_to_s3(s3_client,filepath,bucket_name):
    if os.path.isdir(filepath):
        dir_name = os.path.basename(filepath)
        for filename in os.listdir(filepath):
            path = os.path.join(filepath,filename)
            print("Uploading ", path)
            s3_client.upload_file(path,bucket_name,path)

    else:
        filename_in_s3 = filepath
        print("Uploading ", filename_in_s3)
        s3_client.upload_file(filepath,bucket_name,filename_in_s3)

def load_from_s3(s3_client,prefix,local_folder_path,bucket_name):
    PREFIX = prefix

    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=PREFIX)

    for object in response['Contents']:
        print('Downloading ', object['Key'])
        filename_in_s3 = object['Key']
        path = os.path.join(local_folder_path, filename_in_s3.split("/")[-1])
        s3_client.download_file(bucket_name,filename_in_s3,path)

def start_s3_client(config):
    s3_client = boto3.client('s3' ,config['aws_access']["region_name"],
    aws_access_key_id=config['aws_access']["aws_access_key_id"],
    aws_secret_access_key=config['aws_access']["aws_secret_access_key"])
    return s3_client

def read_config(file_path):
    with open(file_path, 'r') as f:
        return(json.load(f))
