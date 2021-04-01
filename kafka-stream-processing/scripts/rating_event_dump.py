import json
import signal
import re
import sqlalchemy
import csv
import pandas as pd
import time
from kafka import KafkaConsumer
from sqlalchemy.orm import sessionmaker
from scanf import scanf
from scripts.database import Database
from scripts.helper_s3 import *


def get_killswitch():
    return killswitch

def read_configs(config_path):
    '''
    Read configs from JSON
    '''
    dbConf = None
    kafkaConf = None
    with open(config_path) as cfg:
        configs = json.load(cfg)
        dbConf = configs['rds_db']
        kafkaConf = configs['kafka']
        s3Conf = configs['aws_access']
        metricsConf =configs['metrics']

    return dbConf, kafkaConf,s3Conf,metricsConf


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


def connect_kafka(kafkaConf):
    '''
    Setup Kafka broker connection
    (NOTE: SSH Tunnel to brokers needs to be running at the time of execution)
    '''
    consumer = KafkaConsumer(
        kafkaConf['topic_name'],
        bootstrap_servers=kafkaConf['bootstrap_servers'],
        auto_offset_reset='earliest',
        group_id=kafkaConf['group_id'],
        enable_auto_commit=True,
        auto_commit_interval_ms=1000
    )

    return consumer

def get_model_id(s3_client,bucket_name):
        result=(get_latest_model_id(s3_client,bucket_name,"training_output/"))
        return result


def dump_rating_event(table, consumer, db_con,model_id):
    count=0
    valid_count=0
    # Start consuming from Kafka topic
    for message in consumer:
        count +=1
        # Get the timestamp in BIGINT
        ts = message.timestamp

        # Read message byetes into comma-separated string
        event = message.value.decode('utf-8')

        # Parse values from event
        values = event.split(',')
        if(len(values) == 3 and '/rate/' in values[2]):
            user_id = (values[1])
            movie_id, rating = scanf('GET /rate/%s=%s', values[2])
            year = movie_id.split('+')[-1]
            check_valid=validate(ts, user_id, movie_id, rating, year)
            if validate(ts, user_id, movie_id, rating, year):
                values = {
                'kafka_ts':ts, 
                'user_id': int(user_id),
                'movie_id': movie_id,
                'rating': int(rating),
                'model_id': model_id
                }

                try:
                    db_con.insert('rating_event',values)
                except:
                    pass
            
            if killswitch:
                break

def match_pattern(input_str, regex_pattern):
    pattern = re.compile(regex_pattern)
    if(pattern.match(input_str) is None):
        return False
    else:
        return True

def validate_timestamp(timestamp):
    if (timestamp > 0 and type(timestamp)==int):
        return True
    
    return False

def validate_year(year):
    try:
        year=int(year)
        if(int(year) <=2020 and int(year) >1800):
            return True
        
        return False
    except:
        return False
  

def validate_rating(rating):
    try:
        rating = int(rating)
        if(type(rating) == int and  rating >=1 and rating <=5):
            return True
        
        return False
    except:
        return False

def validate_userid(user_id):
    try:
        user_id=int(user_id)
        if(type(user_id)==int and user_id > 0):
            return True
        
        return False
    except:
        return False

    

def validate(timestamp, user_id, movie_id, rating,year):
    '''
    TODO - Write data cleanup logic
    # Set data type validations 
    # Range of ratings from 1 to 5
    # Check if the movie_id is valid
    # Check if the user id is valid
    '''
    try:
        if(validate_year(year) and validate_timestamp(timestamp) and validate_rating(rating) and validate_userid(user_id) and  match_pattern(movie_id,r"(.*\+\d{4}$)")):
            return True
    
        return False

    except:
        return False


def cleanup(consumer, db_con):
    consumer.close()
    db_con.teardown()

def toggle_killswitch(signalNumber, frame):
    global killswitch
    killswitch = True

if __name__ == "__main__":
    # Setup killswitch for clean exit
    killswitch = False
    signal.signal(signal.SIGTERM, toggle_killswitch)

    # Read configs
    dbConf, kafkaConf,s3Conf,_ = read_configs('config/config.json')

    #Connect to s3
    s3_client = start_s3_client(s3Conf)

    # Create DB instance
    db = Database(dbConf)

    # Get table from database
    rating_event_table = db.get_table('rating_event')

    # Connect to Kafka
    consumer = connect_kafka(kafkaConf)

    # get model id
    bucket_name = s3Conf['bucket_name']
    model_id= get_model_id(s3_client,bucket_name)

    # Begin dump process
    dump_rating_event(rating_event_table, consumer, db,model_id)

    cleanup(consumer, db)
