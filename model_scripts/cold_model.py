import pandas as pd
import numpy as np
import dataconfig as cfg

df=pd.read_csv(cfg.data['input_rating_file'])


def get_userids(user_data):
    #get users
    try:
        users=set(user_data['userid'].tolist())
    except:
        pass

    return users

    
def weighted_rating(x):
    #IMDB weight ratings metric
    v = x['vote_count']
    R = x['vote_average']
    return (v/(v+m) * R) + (m/(m+v) * C)

def recommend_movie():
    df=pd.read_csv(cfg.data['input_movies_file'])
    vote_counts = df[df['vote_count'].notnull()]['vote_count'].astype('int')
    vote_averages = df[df['vote_average'].notnull()]['vote_average'].astype('int')
    threshold = vote_counts.quantile(0.95)
    global C
    C = vote_averages.mean()
    global m 
    m = vote_counts.quantile(0.95)
    selected_movies = df[(df['vote_count'] >= threshold) & (df['vote_count'].notnull())]
    selected_movies['weighted rating'] = selected_movies.apply(weighted_rating, axis=1)
    selected_movies = selected_movies.sort_values('weighted rating', ascending=False)
    return(selected_movies.head(20))

#Wrapper function
def filter_and_recommend(user_id,users_set):
    #Check to see if the user is present
    if(user_id not in users_set):
        movies=recommend_movie()
        return movies

    return None


def main():
    user_df=pd.read_csv(cfg.data['input_rating_file'])
    valid_users=get_userids(user_df)
    #Test case
    movies = filter_and_recommend(-50,valid_users)
    ###
    movies.to_csv(cfg.data['output_dir']+"cold_recommendations.csv")

if __name__ == "__main__":
    main()
    pass