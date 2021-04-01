# To store the data
import pandas as pd

# To do linear algebra
import numpy as np
import os

import keras

# # To create deep learning models
from keras.layers import Input, Embedding, Flatten, Dot, Add, Lambda, Activation, Reshape, Concatenate, Dense
from keras.models import load_model, save_model
from keras.regularizers import l2

from sklearn.metrics import mean_squared_error

class Model:
    def __init__(self, model_path):
        self.model_path = model_path
    
    def build_model(self, config, load_saved_model):
        if load_saved_model and os.path.exists(self.model_path):
            model = self.load_saved_model(self.model_path)
            # model.compile(loss='mse', optimizer='adam')

            return model
        # Setup variables
        users = config['num_users']
        movies = config['num_movies']
        user_embedding_size = config['user_embedding_size']
        movie_embedding_size = config['movie_embedding_size']
        max_rating = config['max_rating']
        min_rating = config['min_rating']


        ##### Create model
        # Set input layers
        user_id_input = Input(shape=[1], name='user')
        movie_id_input = Input(shape=[1], name='movie')

        # Create embedding layers for users and movies
        user_embedding = Embedding(output_dim=user_embedding_size, 
                                input_dim=users,
                                input_length=1, 
                                name='user_embedding')(user_id_input)
        movie_embedding = Embedding(output_dim=movie_embedding_size, 
                                    input_dim=movies,
                                    input_length=1, 
                                    name='item_embedding')(movie_id_input)

        # Reshape the embedding layers
        user_vector = Reshape([user_embedding_size])(user_embedding)
        movie_vector = Reshape([movie_embedding_size])(movie_embedding)

        # Concatenate the reshaped embedding layers
        concat = Concatenate()([user_vector, movie_vector])

        # Combine with dense layers
        dense = Dense(512)(concat)
        pred = Dense(1)(dense)

        # Item Bias
        item_bias = Embedding(movies, 1, embeddings_regularizer=l2(1e-4), name='ItemBias')(movie_id_input)
        item_bias_vec = Flatten(name='FlattenItemBiasE')(item_bias)

        # User Bias
        user_bias = Embedding(users, 1, embeddings_regularizer=l2(1e-4), name='UserBias')(user_id_input)
        user_bias_vec = Flatten(name='FlattenUserBiasE')(user_bias)

        # Pred with bias added
        PredAddBias = Add(name="AddBias")([pred, item_bias_vec, user_bias_vec])


        # Scaling for each user
        y = Activation('sigmoid')(PredAddBias)
        rating_output = Lambda(lambda x: x * (max_rating - min_rating) + min_rating)(y)


        # Setup model
        model = keras.models.Model(inputs=[user_id_input, movie_id_input], outputs=rating_output)
        model.compile(loss='mse', optimizer='adam')

        return model

    def load_saved_model(self, path):
        model = load_model(path)

        return model

    def save_the_model(self, model, path):
        save_model(model, path)

        return

    def train_model(self, model, config, save_trained_model):
        train_movie_data = config['train_movie_data']
        train_user_data = config['train_user_data']
        train_ratings = config['train_ratings']
        epochs = config['epochs']
        batch_size = config['batch_size']
        # Fit model
        model.fit([train_user_data, train_movie_data],
                train_ratings,
                batch_size=batch_size, 
                epochs=epochs,
                validation_split=0.1,
                shuffle=True)

        if save_trained_model:
            self.save_the_model(model, self.model_path)

        return model

    def test_model(self, model, config):
        test_movie_data = config['test_movie_data']
        test_user_data = config['test_user_data']
        test_ratings = config['test_ratings']
        # Test model
        y_pred = model.predict([test_user_data, test_movie_data], batch_size=config['batch_size'])
        y_true = test_ratings

        #  Compute RMSE
        rmse = np.sqrt(mean_squared_error(y_pred=y_pred, y_true=y_true))
        # print('\n\nTesting Result With Keras Deep Learning: {:.4f} RMSE'.format(rmse))

        return rmse


    def create_recommendations(self, model, seen, user_id_mapping, movie_id_mapping, recommendations_path, top_N=500):
        recommendation_tuples = []
        total_users = len(user_id_mapping)
        curr_user = 1
        with open(recommendations_path, 'w') as f:
            for user in user_id_mapping.values():
                for item in movie_id_mapping.values():
                    if (user, item) not in seen:
                        recommendation_tuples.append([user, item])

                if curr_user % 5000 == 0:
                    user_movie = pd.DataFrame(recommendation_tuples,columns=['user','movie'])
                    user_movie['rating'] = model.predict([user_movie.user, user_movie.movie], batch_size=16384)
                    user_movie['user'] = pd.to_numeric(user_movie['user'], downcast='integer')
                    user_movie['movie'] = pd.to_numeric(user_movie['movie'], downcast='integer')
                    user_movie = user_movie.sort_values(by='rating', ascending=False).groupby('user').head(top_N)
                    # user_movie['rating'] = user_movie['rating'].map(int)
                    user_movie = user_movie.groupby('user').agg(lambda x: tuple(x)).applymap(list).reset_index()
                    user_movie.to_csv(f,mode='a',columns=['user','movie'],header=False,index=False)
                    recommendation_tuples.clear()
                    del user_movie

                if curr_user % 25000 == 0:
                    print(f'Recommendations built for {curr_user} users')
                curr_user += 1

            if recommendation_tuples:
                user_movie = pd.DataFrame(recommendation_tuples,columns=['user','movie'])
                user_movie['rating'] = model.predict([user_movie.user, user_movie.movie], batch_size=16384)
                user_movie['user'] = pd.to_numeric(user_movie['user'], downcast='integer')
                user_movie['movie'] = pd.to_numeric(user_movie['movie'], downcast='integer')
                user_movie = user_movie.sort_values(by='rating', ascending=False).groupby('user').head(top_N)
                # user_movie['rating'] = user_movie['rating'].map(int)
                user_movie = user_movie.groupby('user').agg(lambda x: tuple(x)).applymap(list).reset_index()
                user_movie.to_csv(f,mode='a',columns=['user','movie'],header=False,index=False)
                recommendation_tuples.clear()
                del user_movie