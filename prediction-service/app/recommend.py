import pickle
import random

import numpy as np
import pandas as pd
from app.helper_s3 import *
from app.database import Database


class Recommend:

    def __init__(self, recommendations_path, mappings_path, path_to_popular_movie, db=None,s3_access=None):
        self.recommendations_path = recommendations_path
        self.mappings_path = mappings_path
        self.s3_client = start_s3_client(s3_access)
        self.s3_access = s3_access

        self.recommendations = pd.read_csv(self.recommendations_path, header=None, names=[
                                           'user', 'movies'], converters={'movies': eval})

        with open(self.mappings_path, 'rb') as handle:
            self.mappings = pickle.load(handle)

        self.recommendations['user'] = self.recommendations['user'].map(
            self.mappings['id_user_mapping'])
        self.recommendations.set_index('user', inplace=True)

        # for cold recommendation
        self.popular_movie_list = pd.read_csv(
            path_to_popular_movie, low_memory=True)['id'].tolist()

        self.__db = db

    def get_model_id(self):
        bucket_name= self.s3_access['bucket_name']
        result=(get_latest_model_id(self.s3_client,bucket_name,"training_output/"))
        return result

    def recommend(self, userid):
        movies = None
        if userid in self.recommendations.index:
            movies = random.sample(self.recommendations.loc[userid].movies, 20)
            movies = list(
                map(lambda x: self.mappings['id_movie_mapping'].get(x), movies))
        else:
            movies = self.popular_movie_list

        self.__record_recommendation(userid, movies)

        return ','.join(movies)


    def __record_recommendation(self, user_id, movies):
        values = [{
            'user_id': user_id,
            'movie_id': movie_id,
            'rating': 0,
            'model_id':self.get_model_id()
        } for movie_id in movies]

        try:
            self.__db.insert('recommendations', values)
        except:
            pass
