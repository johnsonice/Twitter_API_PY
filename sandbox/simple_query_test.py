# -*- coding: utf-8 -*-
"""
Created on Wed Oct 12 17:05:30 2022

@author: CHuang
"""

import os,sys,time
sys.path.insert(0,'../libs')
import datetime,dateutil.parser,unicodedata,time
from util import read_json,read_keywords,chunk_list,create_time_ranges
from twitter_api import Twitter_API
import pandas as pd
import requests

def create_url(keyword, start_date, end_date, max_results = 10):
    
    search_url = "https://api.twitter.com/2/tweets/search/all" #Change to the endpoint you want to collect data from

    #change params based on the endpoint you are using
    query_params = {'query': keyword,
                    'start_time': start_date,
                    'end_time': end_date,
                    'max_results': max_results,
                    #'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    'next_token': {}}
    return (search_url, query_params)

#%%
if __name__ == "__main__":
    key_path = '../keys/twitter_api_key.json'
    keys = read_json(key_path)
    
    ## try search in archieve 
    bearer_token = keys['IMFAPI']['Bearer_Token']
    search_url = "https://api.twitter.com/2/tweets/search/all"
    
    ## initiate T object 
    #search_url = "https://api.twitter.com/2/tweets/search/recent"
    T = Twitter_API(bearer_token,search_url)
    #%%
    
    keyword = "xbox lang:en"
    start_time = "2021-03-01T00:00:00.000Z"
    end_time = "2021-03-31T00:00:00.000Z"
    max_results = 15
    url,query_params = create_url(keyword, start_time, end_time, max_results = 10)
    #%%
    json_response = T.connect_to_endpoint(query_params,to_df=False)
    #%%
    
    response = requests.request("GET", url, headers = T.headers, params = query_params)