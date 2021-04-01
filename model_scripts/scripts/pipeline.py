from model import Model
# from subpopulation_test import TestSubpopulation
from subpopulation_test import TestSubpopulation
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

        self.recommendations_path = os.path.join(self.training_output_folder, "recommendations" + ".csv")
        self.model_path = os.path.join(self.training_output_folder, "model")
        self.mappings_path = os.path.join(self.training_output_folder, "mappings" + ".pkl")

        self.offline_metric_path = os.path.join(self.metrics_folder, "offline_metric.csv")
        self.population_metric_path = os.path.join(self.metrics_folder, "subpopulation_metric.csv")
        # print(os.path.relpath(self.recommendations_path))
        if not os.path.exists(self.offline_metric_path):
            self.check_metrics_exist()

    def check_metrics_exist(self):
        conf = read_config("s3_config.json")
        bucket_name = conf['aws_access']['bucket_name']
        s3_client = start_s3_client(conf)
        prefix = "metrics/"

        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            load_from_s3(s3_client, prefix, self.metrics_folder, bucket_name)

    def check_model_exist(self):
        conf = read_config("s3_config.json")
        bucket_name = conf['aws_access']['bucket_name']
        s3_client = start_s3_client(conf)
        prefix = "training_output/"

        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' in response:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=PREFIX)['Contents']
            last_added = [obj['Key'] for obj in sorted(response, key=lambda x: x['LastModified'].timestamp(), reverse=True)][0]
            timestamp_last = last_added.split("/")[1]
            print(last_added)
            prefix += timestamp_last
            load_from_s3(s3_client, prefix, self.training_output_folder, bucket_name)

    def prepare_data(self):
        if not os.path.exists(self.training_folder_path):
            conf = read_config("s3_config.json")
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

        df = df = df.drop_duplicates(['userid','movieid'], keep='last')
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

    def map_saved_df(self, df, mappings_path):
        with open(mappings_path, 'rb') as handle:
            mappings = pickle.load(handle)

        user_id_mapping = mappings['user_id_mapping']
        movie_id_mapping = mappings['movie_id_mapping']
        id_user_mapping = mappings['id_user_mapping']
        id_movie_mapping = mappings['id_movie_mapping']
        # Create user- & movie-id mapping
        last_user_id = len(user_id_mapping)
        last_movie_id = len(movie_id_mapping)

        for user in df['userid'].unique():
            if user not in user_id_mapping:
                user_id_mapping[user] = last_user_id
                id_user_mapping[last_user_id] = user
                last_user_id += 1

        for movie in df['movieid'].unique():
            if movie not in movie_id_mapping:
                movie_id_mapping[movie] = last_movie_id
                id_movie_mapping[last_movie_id] = movie
                last_movie_id += 1

        # Create correctly mapped train- & testset
        df['userid'] = df['userid'].map(user_id_mapping)
        df['movieid'] = df['movieid'].map(movie_id_mapping)

        return df, mappings

    def dump_mappings(self, mappings_path, mappings):
        mappings = {
            'user_id_mapping':mappings['user_id_mapping'],
            'movie_id_mapping':mappings['movie_id_mapping'],
            'id_user_mapping':mappings['id_user_mapping'],
            'id_movie_mapping':mappings['id_movie_mapping']
        }
        with open(mappings_path, 'wb') as handle:
            pickle.dump(mappings, handle, protocol=pickle.HIGHEST_PROTOCOL)

    def create_train_test(self, df):
        df_train, df_test = train_test_split(df, test_size=0.2)

        return df_train, df_test

    def get_seen_user_movie(self, df):
        seen = set()
        for user, movie in df[['userid','movieid']].values:
            seen.add((user,movie))

        return seen

    def get_timestamp(self):
        unix_timestamp = int(time.time())
        local_time = time.localtime(unix_timestamp)
        timestamp = time.strftime("%Y-%m-%d_%H:%M:%S", local_time)

        return timestamp

    def record_offline_metric(self, test_rmse):
        
        if os.path.exists(self.offline_metric_path):
            with open(self.offline_metric_path, 'a') as f:
                f.write('{},{:.4f}\n'.format(self.timestamp_start, test_rmse))
        else:
            with open(self.offline_metric_path, 'w') as f:
                f.write('timestamp,test_rmse\n')
                f.write('{},{:.4f}\n'.format(self.timestamp_start, test_rmse))

    def upload_to_s3(self, folder_path):
        conf = read_config("s3_config.json")
        bucket_name = conf['aws_access']['bucket_name']
        s3_client = start_s3_client(conf)
        folder_path = os.path.relpath(folder_path)
        save_to_s3(s3_client, folder_path, bucket_name)


    def model_train_pipeline(self, save_trained_model=False ,load_saved_model=False):
        df = self.prepare_data()

        if load_saved_model and os.path.exists(self.mappings_path) and os.path.exists(self.model_path):
            df, mappings = self.map_saved_df(df, self.mappings_path)
        else:
            df, mappings = self.map_df(df)

        if save_trained_model:
            self.dump_mappings(self.mappings_path, mappings)

        df_train, df_test = self.create_train_test(df)
        seen = self.get_seen_user_movie(df)

        build_config = {
            'num_users': len(mappings['user_id_mapping']),
            'num_movies': len(mappings['movie_id_mapping']),
            'user_embedding_size': 20,
            'movie_embedding_size': 10,
            'max_rating': df.rating.max(),
            'min_rating': df.rating.min()
        }
        m = Model(self.model_path)
        model = m.build_model(build_config, load_saved_model)

        train_config = {
            'train_movie_data': df_train['movieid'],
            'train_user_data': df_train['userid'],
            'train_ratings': df_train['rating'],
            'epochs': 1,
            'batch_size': 8192
        }
        model = m.train_model(model, train_config, save_trained_model)
        test_config = {
            'test_movie_data': df_test['movieid'],
            'test_user_data': df_test['userid'],
            'test_ratings': df_test['rating'],
            'batch_size': 4096
        }
        offline_rmse = m.test_model(model, test_config)
        self.record_offline_metric(offline_rmse)

        test = TestSubpopulation(self.configs, self.timestamp_start)
        test.test_direct(df_test, model, m)

        m.create_recommendations(model, seen, mappings['user_id_mapping'], mappings['movie_id_mapping'],\
                                                                             self.recommendations_path, 100)
        self.upload_to_s3(self.training_output_folder)
        self.upload_to_s3(self.metrics_folder)

        

if __name__ == "__main__":
    config_path = "config.json"

    with open(config_path) as cfg:
        configs = json.load(cfg)
    pipeline = Pipeline(configs)
    pipeline.model_train_pipeline(save_trained_model=True, load_saved_model=False)