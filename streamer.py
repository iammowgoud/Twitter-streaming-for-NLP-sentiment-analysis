'''
Credentials

O5i6SVAb46BwIQ8CPzOkqGITE (API key)

evGo0nSIeatFkAbsVIyXiVwVv30vA8MuMrMPocvMmsDWlqcoPb (API secret key)

Access token & access token secret
64418226-lxFYNBHB1mhmjzPFUG8k5fdJCfKMhM4LwOHnA2si0 (Access token)

u4uweKrH0QYTFFMhXfn2b5rT02yjFN7umjQFfL41deo5f (Access token secret)

'''

import socket
import sys
import requests
import requests_oauthlib
import json
import pandas as pd
import numpy as np
import csv
import re
import time
from threading import Thread

CONSUMER_KEY = 'O5i6SVAb46BwIQ8CPzOkqGITE'
CONSUMER_SECRET = 'evGo0nSIeatFkAbsVIyXiVwVv30vA8MuMrMPocvMmsDWlqcoPb'

ACCESS_TOKEN = '64418226-lxFYNBHB1mhmjzPFUG8k5fdJCfKMhM4LwOHnA2si0'
ACCESS_SECRET = 'u4uweKrH0QYTFFMhXfn2b5rT02yjFN7umjQFfL41deo5f'

TCP_IP = "localhost"
TCP_PORT = 1003

threads = []

# API init
my_auth = requests_oauthlib.OAuth1(
    CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)


# query prep
queryCats = pd.read_csv("./weather_word_list.csv")

# subset to only two categories -- RATE LIMIT FOR TWITTER STREMING API
# rain, sun, clouds, wind, hot, cold, humid
queryCats = queryCats[["humid"]]

def start_Stream(category, searchQuery):
    print(f'========= Stream for {category} STARTED')
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [
        ('language', 'en'),
        ("track", searchQuery),
        ("locations", "-74,40,-73,41"),
        ('result_type', 'recent'),
        ('include_entities', 'false')
    ]
    query_url = url + '?' + \
        '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(f'~~response status: {response.status_code}')
    # send_tweets_to_spark(response, category)
    process = Thread(target=send_tweets_to_spark, args=[response, category])
    process.start()
    threads.append(process)


def start_ALL_streams():
    for category in queryCats.columns:
        searchQueryList = [x for x in queryCats[category].tolist() if x == x]
        start_Stream(category, " , ".join(searchQueryList))
    for process in threads:
        process.join()


def send_tweets_to_spark(http_resp, category):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            if "extended_tweet" in full_tweet:
                tweet_text = full_tweet['extended_tweet']['full_text']
            else:
                tweet_text = full_tweet['text']
            tweet_data = bytes(category + "--"+tweet_text + "\n", 'utf-8')
            conn.send(tweet_data)
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)
            pass


##### MAIN
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print('========= Waiting for TCP connection..')
conn, addr = s.accept()
print("('========= Connected... Starting streams")
start_ALL_streams()
