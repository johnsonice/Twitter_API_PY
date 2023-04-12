# -*- coding: utf-8 -*-
"""
Created on Mon Feb  6 15:21:14 2023

@author: CHuang
"""

import os,sys,time
sys.path.insert(0,'./libs')
#import datetime,dateutil.parser,unicodedata,time
from util import read_json#,read_keywords,chunk_list,create_time_ranges
from twitter_api import Twitter_API
import pandas as pd
import requests
import json,tqdm
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) ## surpress request warning

#%%
## for user tweets, we can only get most recent 3200 tweets
##

def create_url(user_id=None,endpoint_type='tweets'):
    # Replace with user ID below
    #user_id = 2244994945
    return "https://api.twitter.com/2/users/{}/{}".format(user_id,endpoint_type)

def get_user_param(user_names_list,field_list):
    user_names = ','.join(user_names_list) if len(user_names_list)>1 else user_names_list[0]
    field_list = ','.join(field_list) if len(field_list)>1 else field_list[0]
    
    params = {'usernames':user_names,
              'user.fields': field_list
              }
    return params 

def get_params():
    # Tweet fields are adjustable.
    # Options include:
    # attachments, author_id, context_annotations,
    # conversation_id, created_at, entities, geo, id,
    # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
    # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
    # source, text, and withheld

    params = {
                # 'tweet.fields': 'id,text,created_at,lang',
                # 'place.fields': 'country,country_code',
                #'start_time':'2019-09-05T16:39:23.000Z',
                'end_time': '2023-02-01T00:00:00.000Z',
                'max_results': 100,
                'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                }
    return params

def get_user_names(user_file_path):
    df = pd.read_excel(user_file_path,sheet_name = "accounts")
    res = df.Name.to_list()
    return res


#%%

if __name__ == "__main__":
    
    ## read key 
    user_file_path = 'search_keywords/Paraguay_Twitter_Search.xlsx'
    user_info_out = 'Data/Tweet_by_user//Paraguay_Twitter_user_info.xlsx'
    user_tweets_out = 'Data/Tweet_by_user/user_tweets{}'
    user_mentions_out = 'Data/Tweet_by_user/user_mentions{}'
    
    
    key_path = 'keys/twitter_api_key.json'
    keys = read_json(key_path)
    bearer_token = keys['IMFAPI']['Bearer_Token']
    #%%
    ###########
    ## get user id by handle 
    ## for user tweets, we can only get most recent 3200 tweets
    ##############
    
    user_info_url = "https://api.twitter.com/2/users/by"
    T_user = Twitter_API(bearer_token,user_info_url)
    user_names = get_user_names(user_file_path)
    user_field_list = ['description','created_at','location','public_metrics']
    user_params = get_user_param(user_names,user_field_list)
    user_res = T_user.connect_to_endpoint(user_params,url=user_info_url,to_df=True,verbose=True)
    user_df = user_res['data']
    user_df = user_df.join(pd.json_normalize(user_df.pop('public_metrics')))
    user_df.to_excel(user_info_out,index=False)
    #%%
    ###########
    ## get user tweets by handle 
    ##############
    res_df_list = []
    user_df.sort_values(by=['tweet_count'],inplace=True,ascending=False)
    for uid in tqdm.tqdm(user_df['id']):
        #row = user_df.iloc[0]
        search_url = create_url(uid,endpoint_type='tweets')
        query_params = get_params()
        T = Twitter_API(bearer_token,search_url)
        t_counts,res_df = T.get_all_twits(query_params,to_df=True,verbose=False,next_token_type='pagination_token')
        if t_counts>0:
            res_df_list.append(res_df)
    ## export to file 
    read_df_all = pd.concat(res_df_list,axis=0)
    read_df_all.reset_index(drop=True,inplace=True)
    read_df_all.to_excel(user_tweets_out.format('.xlsx'),index=False)
    read_df_all.to_pickle(user_tweets_out.format('.pkl'))
    
    #%%
    #######
    # get mensions by user 
    # Using pagination, up to the most recent 800 Tweets can be retrieved.
    #######
    
    res_mention_df_list = []
    #user_df.sort_values(by=['tweet_count'],inplace=True,ascending=False)
    for uid in tqdm.tqdm(user_df['id']):
        #row = user_df.iloc[0]
        search_url = create_url(uid,endpoint_type='mentions')
        query_params = get_params()
        T = Twitter_API(bearer_token,search_url)
        t_counts,res_df = T.get_all_twits(query_params,to_df=True,verbose=False,next_token_type='pagination_token')
        if t_counts>0:
            res_df['mentioned_user_id'] = uid
            res_mention_df_list.append(res_df)
    
    read_df_all = pd.concat(res_mention_df_list,axis=0)
    read_df_all.reset_index(drop=True,inplace=True)
    read_df_all.to_excel(user_mentions_out.format('.xlsx'),index=False)
    read_df_all.to_pickle(user_mentions_out.format('.pkl'))
    
