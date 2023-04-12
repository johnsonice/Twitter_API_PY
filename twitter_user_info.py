# -*- coding: utf-8 -*-
"""
Created on Wed Oct 12 10:48:09 2022

@author: CHuang
"""
import os,sys,time
sys.path.insert(0,'./libs')
#import datetime,dateutil.parser,unicodedata,time
from util import read_json#,read_keywords,chunk_list,create_time_ranges
from twitter_api import Twitter_API
import pandas as pd
import requests
#%%

def get_param(user_names_list,field_list):
    user_names = ','.join(user_names_list) if len(user_names_list)>1 else user_names_list[0]
    field_list = ','.join(field_list) if len(field_list)>1 else field_list[0]
    
    params = {'usernames':user_names,
              'user.fields': field_list
              }
    # params = {'usernames':'pankajtiwari2,elonmusk',
    #           'user.fields':'description,created_at,location'}
    return params 

#%%
if __name__ == "__main__":
    
    ## read key 
    key_path = 'keys/twitter_api_key.json'
    keys = read_json(key_path)
    bearer_token = keys['IMFAPI']['Bearer_Token']

    ## try search in user end point 
    search_url = "https://api.twitter.com/2/users/by"
    T = Twitter_API(bearer_token,search_url)
    field_list = ['description','created_at','location','public_metrics']
    user_names_list = ['elonmusk','pankajtiwari2']
    #user_names_list=['aduanapy','BCP_PY']

    params = get_param(user_names_list,field_list)

    #%%
    res = T.connect_to_endpoint(params,url=search_url,to_df=True,verbose=True)


    
    
    
    
    
    
    
    