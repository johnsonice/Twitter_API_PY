# -*- coding: utf-8 -*-
"""
Created on Wed Mar  8 16:49:35 2023

@author: CHuang
"""

import os,sys,time,argparse
sys.path.insert(0,'./libs')
#import datetime,dateutil.parser,unicodedata,time
from util import read_json,read_keywords,chunk_list,create_time_ranges
from twitter_api import Twitter_API
import tqdm
import pandas as pd
import urllib3
import requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) ## surpress request warning

#%%
def bearer_oauth(r):
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FullArchiveSearchPython"
    return r

def get_parameters_reply(tweet_id):
    params = {
        "query": f"conversation_id:{tweet_id}",
        "tweet.fields": "id,text,in_reply_to_user_id,author_id,conversation_id,public_metrics,entities",
        "max_results": "500",
    }
    # if args.start_time:
    #     params.update(start_time=args.start_time)
    # if args.end_time:
    #     params.update(end_time=args.end_time)

    return (params, tweet_id)

def get_reply_search_param(convsersion_id):
    #query = '("Cambio Climático" OR "Calentamiento Global") from:{}'.format(uid)
    query = "conversation_id:{}".format(convsersion_id)
    query_params = {'query': query,
                    'max_results': 100,
                    'next_token': {},  ## if results > max, use next token to keep requesting
                    'place.fields': 'country,country_code',
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    }
    return query_params,convsersion_id

def get_parameters_quote():
    params = {
        "tweet.fields": "id,text,in_reply_to_user_id,author_id,conversation_id,public_metrics,entities",
        "max_results": "500",
    }
    # if args.start_time:
    #     params.update(start_time=args.start_time)
    # if args.end_time:
    #     params.update(end_time=args.end_time)

    return params

#%%
if __name__ == "__main__":
    
    key_path = 'keys/twitter_api_key.json'
    keys = read_json(key_path)
    bearer_token = keys['IMFAPI']['Bearer_Token']
    #bearer_token = keys['Shiying']['Bearer_Token']
#%%
    # Specify the tweet ID you want to retrieve comments for
    #conversation_id = '1633822454165680128'
    conversation_id = '1402277474982055949'
    search_url = "https://api.twitter.com/2/tweets/search/all"    
    #%%
    #parameters, original_tweet_id = get_parameters_reply(conversation_id)    
    parameters, original_tweet_id = get_reply_search_param(conversation_id) 
    response = requests.request(
        "GET", search_url, auth=bearer_oauth, params=parameters
    )
    res = response.json()
    print(res)
    #%%
    search_url = "https://api.twitter.com/2/tweets/{}/quote_tweets".format(conversation_id) 
    parameters, original_tweet_id = get_parameters_quote()  
    response = requests.request(
        "GET", search_url, auth=bearer_oauth, params=parameters
    )
    res = response.json()
    print(res)
    
    
    #%%
    #url = f"https://api.twitter.com/2/tweets/{tweet_id}?expansions=referenced_tweets.id&tweet.fields=public_metrics&media.fields=duration_ms,height,media_key,preview_image_url,type,url,width&expansion.fields=attachments.media_keys"

    