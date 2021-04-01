import pandas as pd
import requests

ratings = pd.read_csv("processed_ratings.csv")

# fetch user details of all the unique userids from the user API
user_api_url = "http://128.2.204.215:8080/user/"
users = [requests.get(user_api_url + str(user)).json() for user in ratings['userid'].unique()]

# filter out records for which user details does not exist
users = list(filter(lambda x: x.get('message','valid userID') != 'invalid userID', users))
# convert JSON data to dataframe
users = list(map(pd.io.json.json_normalize, users))
users_data = pd.concat(users).reset_index(drop=True)
users_data = users_data.drop_duplicates(subset=['user_id'])

users_data.to_csv("users.csv", index=False)