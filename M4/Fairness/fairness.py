from model import Model
# from subpopulation_test import TestSubpopulation
from subpopulation_test import *
from helper_s3 import start_s3_client, read_config, save_to_s3, load_from_s3
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

class Pipeline:
    def __init__(self, configs):
        self.configs = configs
        self.training_folder_path = configs['train_data']['training_data_folder']
        
        self.training_output_folder = configs['model']['save_folder']
        self.metrics_folder = configs['metrics']['metrics_folder']
        
        if not os.path.exists(self.metrics_folder):
            os.mkdir(self.metrics_folder)

        self.timestamp_start = self.get_timestamp()
        self.training_output_folder = os.path.join(self.training_output_folder, self.timestamp_start)
        if not os.path.exists(self.training_output_folder):
            os.makedirs(self.training_output_folder,)

        self.recommendations_path = configs['model']['recommendations_path']
        self.model_path = configs['model']['model_path']
        self.mappings_path = configs['model']['mappings_path']

        self.offline_metric_path = os.path.join(self.metrics_folder, "offline_metric.csv")
        self.population_metric_path = os.path.join(self.metrics_folder, "subpopulation_metric.csv")
        # print(os.path.relpath(self.recommendations_path))

    def test_direct(self, df, model, m):
        subpopulation_1, subpopulation_2 = self.prepare_subpopulation(df)

        test_config = {
            'test_movie_data': subpopulation_1['movieid'],
            'test_user_data': subpopulation_1['userid'],
            'test_ratings': subpopulation_1['rating'],
            'batch_size': 4096
        }
        subpopulation_1_rmse, conf_1 = m.test_model(model, test_config)

        test_config = {
            'test_movie_data': subpopulation_2['movieid'],
            'test_user_data': subpopulation_2['userid'],
            'test_ratings': subpopulation_2['rating'],
            'batch_size': 4096
        }
        subpopulation_2_rmse, conf_2 = m.test_model(model, test_config)

    def prepare_data(self):
        if not os.path.exists(self.training_folder_path):
            conf = read_config("/home/teamjurassicpark/cora/movie-prediction-group-project-jurassicpark/model_scripts/s3_config.json")
            bucket_name = conf['aws_access']['bucket_name']
            s3_client = start_s3_client(conf)
            prefix = "training_data/"
            os.mkdir(self.training_folder_path)
            load_from_s3(s3_client, prefix, self.training_folder_path, bucket_name)

        all_files = glob.glob(self.training_folder_path + "/*.csv")
    
        ratings = []

        for filename in all_files:
            df = pd.read_csv(filename, index_col=None, header=0, names=['userid','movieid','rating'])
            ratings.append(df)

        df = pd.concat(ratings, axis=0, ignore_index=True)

        df = df.drop_duplicates(['userid','movieid'], keep='last')
        return df
    
    def map_df(self, df):
        # Create user- & movie-id mapping
        user_id_mapping = {id:i for i, id in enumerate(df['userid'].unique())}
        movie_id_mapping = {id:i for i, id in enumerate(df['movieid'].unique())}

        id_user_mapping = {id:user for user, id in user_id_mapping.items()}
        id_movie_mapping = {id:movie for movie, id in movie_id_mapping.items()}

        # Create correctly mapped train- & testset
        df['userid'] = df['userid'].map(user_id_mapping)
        df['movieid'] = df['movieid'].map(movie_id_mapping)

        mappings = {
            'user_id_mapping': user_id_mapping,
            'movie_id_mapping': movie_id_mapping,
            'id_user_mapping': id_user_mapping,
            'id_movie_mapping': id_movie_mapping
        }

        return df, mappings

    def get_timestamp(self):
        unix_timestamp = int(time.time())
        local_time = time.localtime(unix_timestamp)
        timestamp = time.strftime("%Y-%m-%d_%H:%M:%S", local_time)

        return timestamp

    def test_subPopulation(self, load_saved_model=False, save_trained_model=False):
        df = self.prepare_data()

        if load_saved_model and os.path.exists(self.mappings_path) and os.path.exists(self.model_path):
            df, mappings = self.map_saved_df(df, self.mappings_path)
        else:
            df, mappings = self.map_df(df)

        m = Model(self.model_path)

        model = m.build_model({}, True)

        test_config = {
            'test_movie_data': df['movieid'],
            'test_user_data': df['userid'],
            'test_ratings': df['rating'],
            'batch_size': 4096
        }
        
        offline_rmse = m.test_model(model, test_config)

        test_direct(df, model, m)


if __name__ == "__main__":
    config_path = "/home/teamjurassicpark/cora/movie-prediction-group-project-jurassicpark/model_scripts/config.json"

    with open(config_path) as cfg:
        configs = json.load(cfg)
    pipeline = Pipeline(configs)
    pipeline.test_subPopulation(load_saved_model=False, save_trained_model=True)