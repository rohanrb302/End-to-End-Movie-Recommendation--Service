from model import Model
# from pipeline import Pipeline
# To store the data
import pandas as pd
import pickle
import os

# To do linear algebra
import numpy as np
import os
import glob
import json
import time

from sklearn.model_selection import train_test_split
from helper_s3 import start_s3_client, read_config, save_to_s3, load_from_s3

class TestSubpopulation:
    def __init__(self, configs, timestamp=None):
        self.configs = configs
        self.training_folder_path = configs['train_data']['training_data_folder']
        self.training_output_folder = configs['model']['save_folder']
        self.metrics_folder = configs['metrics']['metrics_folder']
        if not os.path.exists(self.training_output_folder):
            os.mkdir(self.training_output_folder)
        if not os.path.exists(self.metrics_folder):
            os.mkdir(self.metrics_folder)

        if timestamp:
            self.timestamp_start = timestamp
        else:
            self.timestamp_start = self.get_timestamp()

        self.model_path = os.path.join(self.training_output_folder, "model_" + self.timestamp_start)
        self.mappings_path = os.path.join(self.training_output_folder, "mappings_" + self.timestamp_start + ".pkl")
        
        self.population_metric_path = os.path.join(self.metrics_folder, "subpopulation_metric.csv")

    def prepare_subpopulation(self, df):
        misc_folder_path = self.configs['misc']['misc_folder_path']
        users_path = os.path.join(self.configs['misc']['misc_folder_path'], "users.csv")
        if not os.path.exists(users_path):
            os.mkdir(self.configs['misc']['misc_folder_path'])

            conf = read_config("s3_config.json")
            bucket_name = conf['aws_access']['bucket_name']
            s3_client = start_s3_client(conf)
            prefix = "user_info/"
            load_from_s3(s3_client, prefix, self.configs['misc']['misc_folder_path'], bucket_name)
        users = pd.read_csv(users_path)
        users.drop(['age','occupation'], axis=1, inplace=True)
        users.rename(columns={'user_id':'userid'}, inplace=True)

        df = pd.merge(df,users,on='userid',how='right').dropna()
        df = df.drop_duplicates(['userid','movieid'],keep='last')

        subpopulation_1 = df[df.gender == "M"]
        subpopulation_2 = df[df.gender == "F"]

        return subpopulation_1, subpopulation_2

    def get_timestamp(self):
        unix_timestamp = int(time.time())
        local_time = time.localtime(unix_timestamp)
        timestamp = time.strftime("%Y-%m-%d_%H:%M:%S", local_time)

        return timestamp

    def record_offline_metric(self, subpopulations, columns, rmses):
        if os.path.exists(self.population_metric_path):
            with open(self.population_metric_path, 'a') as f:
                for subpopulation, column, rmse in zip(subpopulations, columns, rmses):
                    f.write('{},{},{},{:.4f}\n'.format(self.timestamp_start, column, subpopulation.shape[0], rmse))
        else:
            with open(self.population_metric_path, 'w') as f:
                f.write('timestamp,Subpopultion Type,Number of ratings,RMSE\n')
                for subpopulation, column, rmse in zip(subpopulations, columns, rmses):
                    f.write('{},{},{},{:.4f}\n'.format(self.timestamp_start, column, subpopulation.shape[0], rmse))

    def test_direct(self, df, model, m):
        subpopulation_1, subpopulation_2 = self.prepare_subpopulation(df)

        test_config = {
            'test_movie_data': subpopulation_1['movieid'],
            'test_user_data': subpopulation_1['userid'],
            'test_ratings': subpopulation_1['rating'],
            'batch_size': 4096
        }
        subpopulation_1_rmse = m.test_model(model, test_config)

        test_config = {
            'test_movie_data': subpopulation_2['movieid'],
            'test_user_data': subpopulation_2['userid'],
            'test_ratings': subpopulation_2['rating'],
            'batch_size': 4096
        }
        subpopulation_2_rmse = m.test_model(model, test_config)

        self.record_offline_metric([subpopulation_1,subpopulation_2], ["Male","Female"], [subpopulation_1_rmse,subpopulation_2_rmse])



    def test_pipeline(self):
        pipeline = Pipeline(self.configs)
        df = pipeline.prepare_data()
        subpopulation_1, subpopulation_2 = self.prepare_subpopulation(df)

        subpopulation_1, mappings = pipeline.map_saved_df(subpopulation_1, self.mappings_path)
        subpopulation_2, _ = pipeline.map_saved_df(subpopulation_2, self.mappings_path)

        # _, subpopulation_1_test = pipeline.create_train_test(subpopulation_1)
        # _, subpopulation_2_test = pipeline.create_train_test(subpopulation_2)

        m = Model(self.model_path)
        model = m.build_model({}, True)

        test_config = {
            'test_movie_data': subpopulation_1['movieid'],
            'test_user_data': subpopulation_1['userid'],
            'test_ratings': subpopulation_1['rating'],
            'batch_size': 4096
        }
        subpopulation_1_rmse = m.test_model(model, test_config)
        test_config = {
            'test_movie_data': subpopulation_2['movieid'],
            'test_user_data': subpopulation_2['userid'],
            'test_ratings': subpopulation_2['rating'],
            'batch_size': 4096
        }
        subpopulation_2_rmse = m.test_model(model, test_config)

        self.record_offline_metric([subpopulation_1,subpopulation_2], ["Male","Female"], [subpopulation_1_rmse,subpopulation_2_rmse])
    
if __name__ == "__main__":
    config_path = "/home/harshjai/movie-prediction-group-project-jurassicpark/confs/config.json"

    with open(config_path) as cfg:
        configs = json.load(cfg)
    
    test = Test(configs)
    test.test_pipeline()