import json
import os
import time
import boto3
import pandas as pd
import sqlalchemy


def read_configs(config_path):
    '''
    Read configs from JSON
    '''

    dbConf = None
    kafkaConf = None
    with open(config_path) as cfg:
        configs = json.load(cfg)
        dbConf = configs['db']
        trainConf = configs['train_data']

    return dbConf, trainConf


def connect_db(dbConf):
    '''
    Setup DB connection
    '''

    # Build connection URL
    dburl = 'postgresql://{}:{}@{}:{}/{}'.format(
        dbConf['username'],
        dbConf['password'],
        dbConf['host'],
        dbConf['port'],
        dbConf['db_name']
    )

    # Create DB connection
    con = sqlalchemy.create_engine(dburl, client_encoding='utf8')

    # Build metadata
    meta = sqlalchemy.MetaData(bind=con, reflect=True)

    return con, meta


def saveNDelete(cleanDB=False):
    """
    save rating_event table to csv file with timestamp bigger than the previous saved timestamp
    params:
        cleanDB: whether remove all the rows in rating_event
    returns:
        None
    """
    curr_timestamp = int(time.time()*1000)
    timestamp = str(curr_timestamp)
    print('current timestamp: ', timestamp)
    # read timestamp a
    conf = read_config("s3_config.json")
    bucket_name = conf['aws_access']['bucket_name']
    # local_file_path= cwd + "temp_test.csv"
    s3_client = start_s3_client(conf)
    # use the former script here, maybe it's called con
    config_path = "config/config.json"
    dbConf, trainConf = read_configs(config_path)
    con, meta = connect_db(dbConf)
    rating_event_table = meta.tables['rating_event']
    training_data_timestamp_path = trainConf['save_training_data_timestamp']
    training_data_folder_path = trainConf['training_data_folder']

    if os.path.exists(training_data_timestamp_path):
        with open(training_data_timestamp_path, 'r') as f:
            prev_timestamp = f.readline()
    else:
        prev_timestamp = '0'
    print('previous timestamp: ', prev_timestamp)

    # read sql using pandas
    df = pd.read_sql_query(
        'SELECT * FROM rating_event WHERE kafka_ts>%s' % prev_timestamp, con=con)
    df.drop(["event_id", "kafka_ts"], axis=1, inplace=True)
    print('rating_event table data feteched to pandas dataframe')

    # overwrite with new timestamp
    with open(training_data_timestamp_path, 'w') as f:
        f.writelines(timestamp)
    print('replace with new timestamp')

    # create the training data dir if not exists
    if not os.path.exists(training_data_folder_path):
        os.mkdir(training_data_folder_path)
    # export df to csv
    try:
        ## To-do: Add S3 code
        path = os.path.join(training_data_folder_path,
                               timestamp+'.csv')
        df.to_csv(path, index=False, header=False)
        print('saved to' + os.path.join(training_data_folder_path, timestamp+'.csv'))  
        save_to_s3(s3_client,path,bucket_name, curr_timestamp)
    except Exception:
        return

    # cleanup table
    # warning! this will delete all the rows
    if cleanDB:
        rating_event_table.delete()

    con.dispose()

def save_to_s3(s3_client,filepath,bucket_name,timestamp):
    dir_name = os.path.basename(os.path.dirname(filepath))
    local_time = time.localtime(timestamp/1000)
    timestamp = time.strftime("%Y-%m-%d_%H:%M:%S", local_time)
    filename_in_s3 = dir_name + "/" + timestamp + ".csv"        
    print("Uploading", filename_in_s3)
    s3_client.upload_file(filepath,bucket_name,filename_in_s3)

def start_s3_client(config):
    s3_client = boto3.client('s3' ,config['aws_access']["region_name"],
    aws_access_key_id=config['aws_access']["aws_access_key_id"],
    aws_secret_access_key=config['aws_access']["aws_secret_access_key"])
    return s3_client

def read_config(file_path):
    with open(file_path, 'r') as f:
        return(json.load(f))

if __name__ == "__main__":
    saveNDelete()

