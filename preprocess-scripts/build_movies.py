import pandas as pd
import requests

ratings = pd.read_csv("processed_ratings.csv")

# fetch movie details of all the unique movieids from the movie API
movie_api_url = "http://128.2.204.215:8080/movie/"
movies = [requests.get(movie_api_url + movie).json() for movie in ratings['movieid'].unique()]

# filter out records for which movie details does not exist
movies = list(filter(lambda x: x.get('message','None') == 'None', movies))
# convert JSON data to dataframe
movies = list(map(pd.io.json.json_normalize, movies))
movies_data = pd.concat(movies).reset_index(drop=True)
movies_data = movies_data.drop_duplicates(subset=['id'])
# keep only important columns
movies_data = movies_data[['id','imdb_id','title','adult','budget','genres','original_language','release_date','vote_count','vote_average','popularity','overview']]
# preprocess the genres column to make it readable
movies_data['genres'] = movies_data['genres'].apply(lambda x: ",".join([y['name'] for y in x]))

movies_data.to_csv("movies.csv", index=False)