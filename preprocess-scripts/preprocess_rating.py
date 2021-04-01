import pandas as pd

def get_rating(x):
    rating = x.split("=")[-1]
    return int(rating)

def get_name(x):
    name = x.split("/")[2]
    name = name.split("+")[:-1]
    return " ".join(name)

def get_year(x):
    year = x.split("+")[-1]
    year = year.split("=")[0]
    return int(year)

def get_movieid(x):
    movieid = x.split("/")[2]
    movieid = movieid.split("=")[0]
    return movieid


ratings = pd.read_csv("rating_event.csv", header=None, names=['time', 'userid', 'request'])

# clean up records which dont conform to the GET rating request standard
ratings = ratings[ratings.request.str.contains("GET /rate/.*\+\d{4}=[1-5]")]
# clean up records where the userid is non-numeric and transform the userid column to int
ratings =  ratings[ratings.userid.apply(lambda x: str(x).isnumeric())]
ratings['userid'] = pd.to_numeric(ratings['userid'])

#create column for ratings
ratings['rating'] = ratings['request'].apply(get_rating)
# create column for movie title
# ratings['movie'] = ratings['request'].apply(get_name)
# create column for movie year
# ratings['year'] = ratings['request'].apply(get_year)
# create column for movieid
ratings['movieid'] = ratings['request'].apply(get_movieid)
# drop columns request and time
ratings.drop(['request','time'], axis=1, inplace=True)

ratings.to_csv("processed_ratings.csv", index=False)