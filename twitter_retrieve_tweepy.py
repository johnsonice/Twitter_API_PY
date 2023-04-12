# -*- coding: utf-8 -*-
"""
Created on Tue Aug 16 09:37:41 2022

@author: CHuang
"""

## use tweepy for requests

import tweepy
import os,sys,time
sys.path.insert(0,'./libs')
from util import read_json,read_keywords,chunk_list
import pandas as pd
import datetime

if __name__ == "__main__":
    
    ## read key 
    key_path = 'keys/twitter_api_key.json'
    kye_word_path = 'search_keywords/remapping.csv'
    res_out_path = 'Data/{}_{}_{}.csv'
    country_path = 'search_keywords/countries.csv'
    keys = read_json(key_path)
    bearer_token = keys['IMFAPI']['Bearer_Token']
    
    ## initiate tweepy client 
    client = tweepy.Client(bearer_token)
    
    #%%
    ## seasrch from archieve
    # Replace with time period of your choice
    start_time = '2020-01-01T00:00:00Z'
    
    # Replace with time period of your choice
    end_time = '2020-08-01T00:00:00Z'
    
    query = '("climate change" OR "global warming" OR "#climatechange" OR "#globalwarming") place_country:{} -is:retweet'.format('CN')
    res = client.search_all_tweets(query,
                                   start_time=start_time,
                                   end_time=end_time, 
                                   max_results=100,
                                   tweet_fields=['context_annotations', 'created_at'],
                                     )
    #%%
    
    res = client.search_recent_tweets(query)
            