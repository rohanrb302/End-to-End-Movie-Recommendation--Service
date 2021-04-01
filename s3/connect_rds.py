import sys
import os
import json
import signal

import sqlalchemy
from kafka import KafkaConsumer
from scanf import scanf

from online_eval import *


def read_config(config_path):
    with open(config_path,"r") as f:
        configs = json.load(f)

    rdsConf=configs["aws_rds"]
    kafkaConf=configs["kafka"]
    return(rdsConf,kafkaConf)



if __name__ == "__main__":
    # Setup killswitch for clean exit
    killswitch = False
    signal.signal(signal.SIGTERM, toggle_killswitch)

    # Read configs
    dbConf, kafkaConf = read_config('/home/teamjurassicpark/rohan/s3/s3_config.json')

    # Connect to DB
    con, meta = connect_db(dbConf)

    recommendations_table = meta.tables['recommendations']
    rating_event_table = meta.tables['rating_event']

    # Connect to Kafka
    consumer = connect_kafka(kafkaConf)

    # Set pointer to end of partition
    consumer.poll()
    consumer.seek_to_end()

    # Begin online evaluation
    evaluate_ratings(recommendations_table, rating_event_table, consumer, con, killswitch)

    # Cleanup after breaking loop
    cleanup(consumer, con)


                