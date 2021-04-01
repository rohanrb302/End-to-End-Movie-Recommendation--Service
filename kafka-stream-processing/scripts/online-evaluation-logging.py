import json
from rating_event_dump import connect_db,read_configs
import time
import os
import functools 

def cal_false_discovery(true_pos,false_pos):
    return(false_pos/(false_pos+true_pos))

def cal_precision(true_pos,false_pos):
    return(true_pos/(false_pos+true_pos))

def query_database(table,con):
    q = table.select().where(table.c.rating != 0)
    temp_res = con.execute(q)
    res = temp_res.fetchall()
    tp = sum(1 for row in res if row[2] == 1)
    fp = sum(1 for row in res if row[2] == -1)
    return(tp,fp)

def update_record(precision,false_discovery_rate,dataConf):
    time_str=time.strftime("%Y_%m_%d-%H:%M:%S")
    save_path = dataConf['online_eval_results']

    if os.path.exists(save_path):
        with open(save_path, 'a') as f:
            f.write('{},{:.5f},{:.4f}\n'.format(time_str,precision,false_discovery_rate))
    else:
        with open(save_path, 'w') as f:
            f.write('timestamp,Precision,False Discovery rate\n')
            f.write('{},{:.5f},{:.4f}\n'.format(time_str,precision,false_discovery_rate))

if __name__ == "__main__":
    # Read configs
    config_path = "/home/harshjai/movie-prediction-group-project-jurassicpark/confs/config.json"
    dbConf, _,dataConf = read_configs(config_path)
    # Connect to DB
    con, meta = connect_db(dbConf)

    # Get table from database
    recommendations_table = meta.tables['recommendations']
    rating_event_table = meta.tables['rating_event']

    #Query the database
    true_pos,false_pos= query_database(recommendations_table,con)

    # Make calculations 
    false_discovery_rate=cal_false_discovery(true_pos,false_pos)
    precision=cal_precision(true_pos,false_pos)

    #Update the output
    update_record(precision,false_discovery_rate,dataConf)

    #Close DB connection
    con.dispose()


