from ast import keyword
import datetime
import tweepy
import time
import pandas as pd
import os
from dotenv import load_dotenv
import boto3
import re
import geocoder

load_dotenv()
client = boto3.client('sqs', aws_access_key_id=os.getenv("ACCESS_KEY"), aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"), region_name=os.getenv("REGION"))


consumer_key = os.getenv("CONSUMER_KEY")
consumer_secret = os.getenv("CONSUMER_SECRET")
access_token = os.getenv("ACCESS_TOKEN")
access_token_secret = os.getenv("ACCESS_TOKEN_SECRET")
bearer_token = os.getenv("BEARER_TOKEN")

auth = tweepy.OAuthHandler(consumer_key,consumer_secret)
auth.set_access_token(access_token,access_token_secret)
api = tweepy.API(auth)
tweepy.Cursor(api.user_timeline)
list1 = ["#BharatJodoWithSoniaGandhi","Adipurush"]
# g = geocoder.osm('Delhi')
# closest_loc = api.closest_trends(g.lat, g.lng)
# trends = api.get_place_trends(closest_loc[0]["woeid"])
# trendArr = trends[0]["trends"]
# for x in range(len(trendArr)):
#     if(x<6):
#         print(trendArr[x]['name'])
#         list1.append(trendArr[x]['name'])
data = []
for cat in list1:
    keys = cat 
    limit = 20
    tweets = tweepy.Cursor(api.search_tweets, q="%s -filter:retweets -filter:links -filter:mentions"%keys, count = 100, tweet_mode='extended', lang='en').items(limit)
    columns = ['Category', 'Tweet']
    for tweet in tweets:     
        data.append([keys,tweet.full_text])
df = pd.DataFrame(data,columns=columns)
df.to_csv('tweets.csv')
df1= pd.DataFrame(columns=['Tweet'])
df1['Tweet'] = df['Tweet']

for x in range(len(df)):
    df["Tweet"].iloc[x] = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|RT"," ", df["Tweet"].iloc[x]).split())

df.to_csv('tweets.csv', mode = 'a', header=False)
print(df)