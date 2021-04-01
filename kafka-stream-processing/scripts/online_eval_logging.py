import json
from ratings_event_dump import *
import time
import os
import functools 
import schedule 

eps =0.005

def cal_false_discovery(true_pos,false_pos):
    return(false_pos/(false_pos+true_pos+eps))

def cal_precision(true_pos,false_pos):
    return(true_pos/(false_pos+true_pos+eps))

def query_model_ids(table,db_con):
    q= table.select().distinct(table.c.model_id)
    temp_res = db_con.select_query(q)
    res = temp_res.fetchall()
    models=[x[3] for x in res]
    return(models)

def query_database(table,db_con,model_id):
    q = table.select().where((table.c.rating != 0) &
                            (table.c.model_id == model_id))
    temp_res = db_con.select_query(q)
    res = temp_res.fetchall()
    tp = sum(1 for row in res if row[2] == 1)
    fp = sum(1 for row in res if row[2] == -1)
    return(tp,fp)


def update_record(precision,false_discovery_rate,metricConf,s3_client,bucket_name, model_id):
    time_str=time.strftime("%Y_%m_%d-%H%M%S")
    # model_id = get_latest_model_id(s3_client,bucket_name)
    save_path = f"{metricConf['online_evalutaion_local']}"+f"online_results_{time_str}_{model_id}.json"
    data = {"Precision":precision,"false_discovery_rate":false_discovery_rate}
    with open(save_path, 'w+') as fp:
        json.dump(data, fp)
    
    filename_in_s3 = metricConf['online_evaluation_s3_path']+f"online_results_{time_str}_{model_id}.json"
    s3_client.upload_file(save_path,bucket_name,filename_in_s3)


def job_main():
    dbConf, kafkaConf,s3Conf,metricConf= read_configs('config/config.json')

    #Connect to s3
    s3_client = start_s3_client(s3Conf)
    bucket = s3Conf['bucket_name']

    # Create DB instance
    db = Database(dbConf)


    # Get table from database
    recommendations_table = db.get_table('recommendations')

    models =query_model_ids(recommendations_table,db)
    for model in models:
        #Query the database
        true_pos,false_pos= query_database(recommendations_table,db,model)

        # Make calculations 
        false_discovery_rate=cal_false_discovery(true_pos,false_pos)
        precision=cal_precision(true_pos,false_pos)

        #Update the output
        update_record(precision,false_discovery_rate,metricConf,s3_client,bucket,model)

    #Close DB connection
    db.teardown()


schedule.every().day.at("11:00").do(job_main)

while True:
    schedule.run_pending()
    time.sleep(20) 

