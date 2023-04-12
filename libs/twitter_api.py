# -*- coding: utf-8 -*-
"""
Created on Thu Aug 11 10:49:18 2022

@author: chuang

reference
https://github.com/twitterdev/Twitter-API-v2-sample-code/blob/main/Recent-Search/recent_search.py
https://towardsdatascience.com/an-extensive-guide-to-collecting-tweets-from-twitter-api-v2-for-academic-research-using-python-3-518fcb71df2a

twitter api request generator
https://developer.twitter.com/apitools/api?endpoint=%2F2%2Fusers%2Fby&method=get
"""

import requests
import os
import json
import pandas as pd
import csv,datetime,dateutil.parser,unicodedata,time
from util import read_json

class Twitter_API(object):
    def __init__(self, bearer_token,url=None):
        self.bearer_token = bearer_token
        self.headers = self.create_headers(bearer_token)
        if url:
            self.url = url
        
    
    @staticmethod
    def create_headers(bearer_token):
        headers = {
            "Authorization": "Bearer {}".format(bearer_token),
            #"User-Agent": "v2SpacesLookupPython"
            }
        return headers
    
    def update_endpoint(self,url):
        """
        update search end point 
        """
        self.url = url
    
    def connect_to_endpoint(self,params,url=None,to_df=True,verbose=True,next_token=None,next_token_type='next_token',n_try=5,wait_n=10):
        if url:
            self.update_endpoint(url)
        ## update next token     
        params[next_token_type] = next_token 
        
        STOP = False
        for i in range(n_try):
            try:
                response = requests.get(self.url, headers=self.headers, params=params,verify=False)
                
                if verbose:
                    print(response.status_code)
                if response.status_code != 200:
                    raise Exception(response.status_code, response.text)
                json_response = response.json()
                STOP = True
            except Exception as e:
                if verbose:
                    print(e)
                print('trail {}; wait for {} seconds'.format(i,wait_n))
                time.sleep(wait_n)
                STOP = False
                error_msg = e 
            
            if STOP:
                break
            
        if STOP == False:
            raise Exception(error_msg)
        
        if to_df:
            json_response['data'] = pd.DataFrame(json_response['data'])
            return json_response
        else:
            return json_response
        
    def get_all_twits(self,params,url=None,to_df=True,max_page=10000,verbose=True,next_token_type='next_token',n_try=5,wait_n=20):
        page_count = 0
        data_list = []
        total_count = 0 
        next_token = None ## initial next token = None
        
        flag = True
        
        while flag:
            json_response = self.connect_to_endpoint(params,url,to_df=False,verbose=verbose,next_token=next_token,
                                                     next_token_type=next_token_type,n_try=n_try,wait_n=wait_n)
            result_count = json_response['meta']['result_count']
            
            if result_count is not None and result_count > 0:
                total_count += result_count
                data_list.extend(json_response['data'])
                print("#### Twitter API Retrieve Info: Total # of Tweets added: ", total_count)
                time.sleep(5) 
            
            if 'next_token' in json_response['meta']:
                next_token = json_response['meta']['next_token']  ## update next token 
                if verbose:
                    print("Next Token: ", next_token)
                flag = True
            else:
                flag = False
            
            page_count +=1
            if page_count >= max_page:
                flag = False
                print('max iteration reached, stop sending request....')
        
        if to_df:
            ## convert to pd df
            df = pd.DataFrame(data_list)
            return total_count,df
        
        return total_count,data_list

                
#%%
if __name__ == "__main__":
    
    ## read bear_token 
    key_path = '../keys/twitter_api_key.json'
    keys = read_json(key_path)
    bearer_token = keys['Chengyu']['Bearer_Token']
    
    ## initiate T object 
    search_url = "https://api.twitter.com/2/tweets/search/recent"
    T = Twitter_API(bearer_token,search_url)
    
    ## set query 
    query_params = {'query':'(from:twitterdev -is:retweet) OR #twitterdev', 
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    'max_results':100}
    #%%
    ## get response 
    results = T.connect_to_endpoint(query_params,to_df=True,verbose=False)
    print('meta: {}'.format(results['meta']))
    print('data:',results['data'])

    ## get all pages of twits 
    total_count,data = T.get_all_twits(query_params,max_page=10)