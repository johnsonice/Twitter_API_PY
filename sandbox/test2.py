# -*- coding: utf-8 -*-
"""
Created on Thu Dec  1 16:20:54 2022

@author: CHuang
"""

import requests
import json
import os,sys,time
sys.path.insert(0,'../libs')
from util import read_json,read_keywords,chunk_list,create_time_ranges
from twitter_api import Twitter_API


# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
search_url = "https://api.twitter.com/2/tweets/search/recent"

key_path = '../keys/twitter_api_key.json'
keys = read_json(key_path)

## try search in archieve 
bearer_token = keys['IMFAPI']['Bearer_Token']
search_url = "https://api.twitter.com/2/tweets/search/recent"

# Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
# expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields
query_params = {'query': 'fuel subsidies (Climate OR Carbon OR CO2 OR emission OR emissions OR renewable OR renewables OR green OR energy OR "Greenhouse Gas" OR "Greenhouse Gases" OR GHG) -is:retweet',
                'tweet.fields': 'author_id'}


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2RecentSearchPython"
    return r

def connect_to_endpoint(url, params):
    response = requests.get(url, auth=bearer_oauth, params=params,verify=False)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def main():
    json_response = connect_to_endpoint(search_url, query_params)
    print(json.dumps(json_response, indent=4, sort_keys=True))

#%%

if __name__ == "__main__":
    main()