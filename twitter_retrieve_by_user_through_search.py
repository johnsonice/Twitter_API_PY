# -*- coding: utf-8 -*-
"""
Created on Mon Aug  8 11:23:28 2022
@author: chuang
## how to build a query
https://developer.twitter.com/en/docs/twitter-api/tweets/counts/integrate/build-a-query

"""
#%%
import os,sys,time,argparse
sys.path.insert(0,'./libs')
#import datetime,dateutil.parser,unicodedata,time
from util import read_json,read_keywords,chunk_list,create_time_ranges,maybe_create
from twitter_api import Twitter_API
import tqdm
import pandas as pd
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) ## surpress request warning

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
    
def get_user_search_param(uid):
    #query = '("Cambio Climático" OR "Calentamiento Global") from:{}'.format(uid)
    query = 'from:{}'.format(uid)
    query_params = {'query': query,
                    'start_time': '2007-01-01T00:00:00.000Z',
                    'end_time' : '2023-01-01T00:00:00.000Z',
                    'max_results': 100,
                    'next_token': {},  ## if results > max, use next token to keep requesting
                    'place.fields': 'country,country_code',
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
#                    'tweet.fields': 'id,text,created_at,lang',
#                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
#                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    }
    return query_params

def get_user_mention_search_param(uid):
    #query = '("Cambio Climático" OR "Calentamiento Global") from:{}'.format(uid)
    query = '@{}'.format(uid)
    query_params = {'query': query,
                    'start_time': '2007-01-01T00:00:00.000Z',
                    'end_time' : '2023-01-01T00:00:00.000Z',
                    'max_results': 100,
                    'next_token': {},  ## if results > max, use next token to keep requesting
                    'place.fields': 'country,country_code',
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
#                    'tweet.fields': 'id,text,created_at,lang',
#                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
#                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    }
    return query_params

def get_user_names(user_file_path):
    df = pd.read_excel(user_file_path,sheet_name = "accounts")
    res = df.Name.to_list()
    return res

def get_user_param(user_names_list,field_list):
    user_names = ','.join(user_names_list) if len(user_names_list)>1 else user_names_list[0]
    field_list = ','.join(field_list) if len(field_list)>1 else field_list[0]
    
    params = {'usernames':user_names,
              'user.fields': field_list
              }
    return params 

def get_downloaded_users(folder_path,remove='user_tweets_v2'):
    #chunk_folder = os.path.dirname(folder_path)
    f_names = os.listdir(folder_path)
    existing_users = list(set([f.replace(remove,'').split('.')[0] for f in f_names]))
    return existing_users

def list_differences(list1, list2):
    diff1 = list(set(list1) - set(list2))
    diff2 = list(set(list2) - set(list1))
    return diff1 + diff2

def t_args(args_list=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('-name', '--name', action='store', dest='name',
                    default='Argentina',type=str)
    parser.add_argument('--user_info', action='store_true', dest='user_info')
    parser.add_argument('--tweet', action='store_true', dest='tweet') 
    parser.add_argument('--mention', action='store_true', dest='mention') 
    
    if args_list is not None:
        args = parser.parse_args(args_list) 
    else:
        args = parser.parse_args()    
        
    return args

#%%
if __name__ == "__main__":
    args = t_args()
    print(args)
    ## read key 
    # user_file_path = 'search_keywords/Paraguay_Twitter_Search.xlsx'
    # user_info = 'Data/Tweet_by_user/Paraguay_Twitter_user_info.xlsx'
    user_file_path = 'search_keywords/user_tweet_by_country/{}_Twitter_Search.xlsx'.format(args.name)
    user_info = 'Data/Tweet_by_user/user_info/{}_Twitter_user_info.xlsx'.format(args.name)
    user_tweets_out = 'Data/Tweet_by_user/'+args.name+'_user_tweets_v2{}'
    
    chunks_folder = 'Data/Tweet_by_user/chunks/'+args.name
    maybe_create(chunks_folder)
    user_tweets_out_chunks = 'Data/Tweet_by_user/chunks/'+args.name+'/user_tweets_v2{}'
    user_mentions_out = 'Data/Tweet_by_user/'+args.name+'_user_mentions_v2{}'
    user_mentions_out_chunks = 'Data/Tweet_by_user/chunks/'+args.name+'/user_mentions_v2{}'
    
    key_path = 'keys/twitter_api_key.json'
    keys = read_json(key_path)
    bearer_token = keys['IMFAPI']['Bearer_Token']
    #bearer_token = keys['Shiying']['Bearer_Token']

    TASK_user_info = args.user_info
    TASK_tweet= args.tweet
    TASK_mention = args.mention
#%%
    ## get user meta info
    ###########
    ## get user id by handle 
    ## for user tweets, we can only get most recent 3200 tweets
    ##############
    if TASK_user_info:
        user_info_url = "https://api.twitter.com/2/users/by"
        T_user = Twitter_API(bearer_token,user_info_url)
        user_names = get_user_names(user_file_path)
        user_field_list = ['description','created_at','location','public_metrics']
        user_params = get_user_param(user_names,user_field_list)
        user_res = T_user.connect_to_endpoint(user_params,url=user_info_url,to_df=True,verbose=True)
        user_df = user_res['data']
        user_df = user_df.join(pd.json_normalize(user_df.pop('public_metrics')))
        user_df.to_excel(user_info,index=False)

#%%

    search_url = "https://api.twitter.com/2/tweets/search/all"
    user_df = pd.read_excel(user_info)
    
    ###########
    ## get user tweets by handle 
    ##############
    if TASK_tweet:
        print('Task : Get user tweets data ............')
        
        chunk_folder = os.path.dirname(user_tweets_out_chunks)
        
        res_df_list = []
        user_df.sort_values(by=['tweet_count'],inplace=True,ascending=True)
        ## only download the differences 
        existing_users= get_downloaded_users(chunk_folder)
        users_to_download = list_differences(existing_users,user_df['username'].tolist())
        
        for uname in tqdm.tqdm(users_to_download):
            #row = user_df.iloc[0]
            print('working on {}'.format(uname))
            if uname in existing_users:
                continue
            query_params = get_user_search_param(uname)   #### define from query here 
            T = Twitter_API(bearer_token,search_url)
            total_count,data = T.get_all_twits(query_params,max_page=float('inf'),
                                            to_df=True,verbose=False,next_token_type='next_token')
            if total_count>0:
                res_df_list.append(data)
                data.to_excel(user_tweets_out_chunks.format(uname + '.xlsx'),index=False)
                data.to_pickle(user_tweets_out_chunks.format(uname + '.pkl'))
        ## export to file 
        read_df_all = pd.concat(res_df_list,axis=0)
        read_df_all.reset_index(drop=True,inplace=True)
        read_df_all.to_excel(user_tweets_out.format('.xlsx'),index=False)
        read_df_all.to_pickle(user_tweets_out.format('.pkl'))

#%%
    ###########
    ## get user mentions 
    ##############
    
    if TASK_mention:
        print('Task : Get user mentions data ............')
        res_df_list = []
        user_df.sort_values(by=['tweet_count'],inplace=True,ascending=True)
        
        #downloaded_users
        d_users = os.listdir('Data/Tweet_by_user/chunks/')
        d_users = [f.replace('user_mentions_v2','').replace('.pkl','') for f in d_users if '.pkl' in f]
        d_users = [u for u in user_df['username'].tolist() if u not in d_users ]
       
        for uname in tqdm.tqdm(d_users):
            #row = user_df.iloc[0]
            print('working on {}'.format(uname))
            query_params = get_user_mention_search_param(uname)          #### define @ query ehre 
            T = Twitter_API(bearer_token,search_url)
            total_count,data = T.get_all_twits(query_params,max_page=float('inf'),
                                            to_df=True,verbose=False,next_token_type='next_token')
            if total_count>0:
                data['mentioned_user_name'] = uname
                res_df_list.append(data)
                try:
                    data.to_excel(user_mentions_out_chunks.format(uname + '.xlsx'),index=False)
                except:
                    print('failed to save to excel')
                data.to_pickle(user_mentions_out_chunks.format(uname + '.pkl'))
        # ## export to file 
        # read_df_all = pd.concat(res_df_list,axis=0)
        # read_df_all.reset_index(drop=True,inplace=True)
        # read_df_all.to_excel(user_mentions_out.format('.xlsx'),index=False)
        # read_df_all.to_pickle(user_mentions_out.format('.pkl'))





#data.to_excel('test.xlsx')
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
    
    
    
# %%
