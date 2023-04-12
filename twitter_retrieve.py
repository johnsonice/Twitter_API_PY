# -*- coding: utf-8 -*-
"""
Created on Mon Aug  8 11:23:28 2022

@author: chuang
https://github.com/twitterdev/Twitter-API-v2-sample-code/blob/main/Recent-Search/recent_search.py
https://towardsdatascience.com/an-extensive-guide-to-collecting-tweets-from-twitter-api-v2-for-academic-research-using-python-3-518fcb71df2a

## twitter search all, endpoing doc:
https://developer.twitter.com/en/docs/twitter-api/tweets/search/api-reference/get-tweets-search-all
https://developer.twitter.com/en/docs/twitter-api/tweets/search/api-reference/get-tweets-search-recent

## how to build a query
https://developer.twitter.com/en/docs/twitter-api/tweets/counts/integrate/build-a-query

"""

import os,sys,time
sys.path.insert(0,'./libs')
#import datetime,dateutil.parser,unicodedata,time
from util import read_json,read_keywords,chunk_list,create_time_ranges
from twitter_api import Twitter_API
import pandas as pd

#%%
def try_n_request(query_params,n:int,wait_n:int):
    STOP = False
    for i in range(n):
        try:
            total_count,data = T.get_all_twits(query_params,max_page=float('inf'),to_df=True,verbose=False)
            STOP = True
        except Exception as e:
            print(e)
            print('trail {}; wait for {} seconds'.format(i,wait_n))
            time.sleep(wait_n)
            STOP = False
            error_msg = e 
        
        if STOP:
            break
    
    if STOP == False:
        raise Exception(error_msg)
        
    return total_count,data
    
def get_general_search_res(country_code):
    """
    General Climate Word Search
    passin in two digit ISO code for country
    """
    query = '("climate change" OR "global warming" OR "#climatechange" OR "#globalwarming") place_country:{} -is:retweet'.format(country_code)
    query_params = {'query': query,
                    'start_time': '2020-08-01T00:00:00.000Z',
                    'end_time' : '2021-08-02T00:00:00.000Z',
                    'max_results': 100,
                    'next_token': {},  ## if results > max, use next token to keep requesting
                    'tweet.fields': 'id,text,created_at,lang',
                    'place.fields': 'country,country_code',
#                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
#                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
#                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
#                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    }
    
    total_count,data = T.get_all_twits(query_params,max_page=float('inf'),to_df=True,verbose=False)
    
    return total_count,data
    
def get_by_key_group(key_groups,ISO=None,start_time='2007-01-01T00:00:00Z',end_time='2022-01-01T00:00:00Z'):
    res_dict = {}
    for dict_key,key_words in key_groups.items():
        all_data = []
        for ks in chunk_list(key_words,1):
            #ks = key_words[10:20]
            ks = [k.replace('and','').replace('&','') for k in ks]
            if len(ks) == 1:
                if ISO:
                    query = '{} place_country:{} -is:retweet'.format(' OR '.join(ks),ISO)
                else:
                    query = '{} -is:retweet'.format(' OR '.join(ks))
            else:
                if ISO:
                    query = '({}) place_country:{} -is:retweet'.format(' OR '.join(ks),ISO)
                else:
                    query = '({}) -is:retweet'.format(' OR '.join(ks))
            print('query: {}'.format(query))
            query_params = {'query': query,
#                        'start_time': '2007-01-01T00:00.0:00.000Z',
#                        'end_time' : '2022-09-01T00:00:00.000Z',
                        'start_time': start_time,
                        'end_time' : end_time,
                        'max_results': 200,
                        'next_token': {},  ## if results > max, use next token to keep requesting
                        'tweet.fields': 'id,text,created_at,lang',
                        'place.fields': 'country,country_code',
                        }
            
            total_count,data = try_n_request(query_params,n=20,wait_n=60*2)
            time.sleep(10)
            all_data.append(data)    
        country_df = pd.concat(all_data, ignore_index=True)
        res_dict[dict_key] = country_df
    return res_dict
        
#%%
if __name__ == "__main__":
    
    ## read key 
    key_path = 'keys/twitter_api_key.json'
    kye_word_path = 'search_keywords/step1_search_terms_v2.csv' ##revamped
    res_out_path = 'Data/{}_{}_{}.csv'
    res_out_path_by_time = 'Data/Market_Non-Market_by_year/{}_{}.csv'
    country_path = 'search_keywords/countries.csv'
    keys = read_json(key_path)
    
    ## try search in archieve 
    #bearer_token = keys['IMFAPI']['Bearer_Token']
    bearer_token = keys['Shiying']['Bearer_Token']
    search_url = "https://api.twitter.com/2/tweets/search/all"
    
    ## set up some flags 
    BY_COUNTRY = False
    BY_YEAR = True
    AD_HOC = False
    
    ## initiate twitter api retrieve object 
    T = Twitter_API(bearer_token,search_url)
    
    ## get general term, and recent period 
    #total_count,data = get_general_search_res('CN')
    
    ## get policy keywords 
    key_groups,all_keys = read_keywords(kye_word_path)
    countries = pd.read_csv(country_path).set_index('2_Code')['Country_Name'].to_dict()
    #%%
    ############################
    ### retrieve by country ####
    ############################
    if BY_COUNTRY:
        for country_iso,country_name in countries.items(): 
    #        country_iso = 'CN'
    #        country_name= 'China'
            print('.......... Work on {} ........'.format(country_name))
            res_dict = get_by_key_group(key_groups,ISO=country_iso,start_time='2007-01-01T00:00:00Z',end_time='2022-01-01T00:00:00Z')
            for k,v in res_dict.items():
                v.to_csv(res_out_path.format(country_name,country_iso,k),encoding='utf-8')
    
    #%%
    ############################
    ### retrieve by year    ####
    ############################
    if BY_YEAR:
        time_ranges = create_time_ranges(2022,2023)  #2022   
        for tr in time_ranges:
            s,e = tr
            print('.......... Work on {} - {} ........'.format(s[:4],e[:4]))
            res_dict = get_by_key_group(key_groups,start_time=s,end_time=e)
            for k,v in res_dict.items():
                v.to_csv(res_out_path_by_time.format(k,s[:4]),encoding='utf-8')
            
    #%%
    ############################
    ### adhoc retrieve    ####
    ############################ 
    if AD_HOC:
        res_dict = get_by_key_group(key_groups,start_time='2022-01-01T00:00:00Z',end_time='2022-12-01T00:00:00Z')
        for k,v in res_dict.items():
            v.to_csv(res_out_path_by_time.format(k,'2022_01-11'),encoding='utf-8')
        
    #%%
#    #%%
    # query = '(research \\"and\\" development climate) place_country:CN -is:retweet'
    # query = '(commitments climate) -is:retweet'
    # query_params = {'query': query,
    #                 'start_time': '2010-08-01T00:00:00.000Z',
    #                 'end_time' : '2011-09-01T00:00:00.000Z',
    #                 'max_results': 200,
    #                 'next_token': {},  ## if results > max, use next token to keep requesting
    #                 'tweet.fields': 'id,text,lang',
    #                 'place.fields': 'country,country_code',
    #                 }
    # #%%
    # total_count,data = T.get_all_twits(query_params,max_page=float('inf'),to_df=True,verbose=False)
#    #%%
#    data.to_csv('Data/test3.csv')
#    #%%
#    
    
    
    