# -*- coding: utf-8 -*-
"""
Created on Mon Aug  8 11:23:28 2022
@author: chuang
## how to build a query
https://developer.twitter.com/en/docs/twitter-api/tweets/counts/integrate/build-a-query

"""
#%%
import os,sys,time,argparse,ast
sys.path.insert(0,'./libs')
#import datetime,dateutil.parser,unicodedata,time
from util import read_json,get_all_files,maybe_create,chunk_list
from twitter_api import Twitter_API
import tqdm
import pandas as pd
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) ## surpress request warning

#%%    
def try_n_request(try_func,conv_id,n:int,wait_n:int):
    STOP = False
    for i in range(n):
        try:
            res = try_func(conv_id)
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
        
    return res

def get_reply_search_param(conversation_id):
    #query = '("Cambio Climático" OR "Calentamiento Global") from:{}'.format(uid)
    if isinstance(conversation_id,list):
        query = " OR ".join([f"conversation_id:{cid}" for cid in conversation_id])
    else:
        query = "conversation_id:{}".format(conversation_id)
        
    query_params = {'query': query,
                    'start_time': '2007-01-01T00:00:00.000Z',
                    'end_time' : '2023-03-09T00:00:00.000Z',
                    'max_results': 100,
                    'next_token': {},  ## if results > max, use next token to keep requesting
                    'place.fields': 'country,country_code',
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    }
    return query_params

def get_quote_param():
    query_params = {
                    'max_results': 100,
                    'next_token': {},  ## if results > max, use next token to keep requesting
                    'place.fields': 'country,country_code',
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    }
    return query_params

def get_tweet_ids(tweet_file,author_tweets_only=True,threshold=1):
    try:
        df = pd.read_excel(tweet_file)
    except:
        df = pd.read_csv(tweet_file)
        
    if author_tweets_only:
        df = df[df['id'] == df['conversation_id']]
    if threshold>0:
        df = df[df['public_metrics'].apply(comments_filter,args=(threshold,))]
    
    return df

def comments_filter(dict_str,threshod=1):
    info_dict = ast.literal_eval(dict_str)
    n = info_dict['reply_count'] + info_dict['quote_count']
    return n >= threshod
    

def get_reply_and_quote(conversation_id,max_page=5):
    agg_results = []
    ###################
    ## get all replys #
    ###################
    search_url = "https://api.twitter.com/2/tweets/search/all"    
    reply_param = get_reply_search_param(conversation_id)    
    T_reply = Twitter_API(bearer_token,search_url)
    reply_count,reply_tweets = T_reply.get_all_twits(reply_param,max_page=max_page, ## we set to retrieve only 1000 replies per tweet ; should be large engough
                                            to_df=False,verbose=False,next_token_type='next_token',
                                            n_try=3,wait_n=60)
    if reply_count>0:
        reply_tweets = [{**item, 
                         'twitter_type': 'reply',
                         'referenced_tweet_id':str(item.get('conversation_id'))} 
                        for item in reply_tweets]
        agg_results.extend(reply_tweets)
    
    time.sleep(1)
    ###################
    ## get quotes  #
    ###################
    if not isinstance(conversation_id,list):
        conversation_id = [conversation_id]
    all_quotes = []
    for cid in conversation_id:
        #print(cid)
        search_url = "https://api.twitter.com/2/tweets/{}/quote_tweets".format(cid)    
        quote_param = get_quote_param()    
        T_quotes = Twitter_API(bearer_token,search_url)
        quotes_count,quotes_tweets = T_quotes.get_all_twits(quote_param,max_page=max_page, ## we set to retrieve only 1000 quotes per tweet ; should be large engough
                                                to_df=False,verbose=False,next_token_type='pagination_token',
                                                n_try=3,wait_n=60)
        all_quotes.extend(quotes_tweets)
    ## quotes data, the reference id is not always, qotes can be in a reply to another tweet. So referenced id can be differt from quoted 
    ## but that is fine 
    if quotes_count>0:
        quotes_tweets = [{**item, 'twitter_type': 'quote','referenced_tweet_id':str(cid)} for item in all_quotes]
        agg_results.extend(quotes_tweets)
    
    return agg_results

def batch_get_comments_by_conv_ids(conv_ids,batch=True,batch_size=6):
    
    all_results=[]
    error_ids=[]
    
    if batch:
        conv_ids = chunk_list(conv_ids,size=batch_size)
        
    for idx,conversation_id in enumerate(tqdm.tqdm(conv_ids)): ## we use tweet id as conv id here as we only want conv under that tweet
        #print(conversation_id) 
        
        if idx//50 == 0 and idx!=0:
            time.sleep(120)
        try:
            id_res = try_n_request(get_reply_and_quote,conversation_id,n=1,wait_n=60)
            if len(id_res)>0:
                all_results.extend(id_res)
        except Exception as e:
            print(f'error in retrieve conv_id: {conversation_id}')
            if isinstance(conversation_id,list):
                error_ids.extend(conversation_id)
            else:
                error_ids.append(conversation_id)
            #raise Exception(e)
            #break
        ## add delay to aviod server hang up 
        time.sleep(2)
    
    ## return results and errors 
    return all_results,error_ids

def list_differences(list1, list2):
    diff1 = list(set(list1) - set(list2))
    diff2 = list(set(list2) - set(list1))
    return diff1 + diff2

def t_args(args_list=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('-name', '--name', action='store', dest='name',
                    default='ArgentinaBrazilUruguayParaguay',type=str)
    parser.add_argument('-key', '--key', action='store', dest='key',
                    default='IMFAPI',type=str)  
    parser.add_argument('-t', '--threshold', action='store', dest='threshold',
                    default=1,type=int)
    if args_list is not None:
        args = parser.parse_args(args_list) 
    else:
        args = parser.parse_args()    
        
    return args

#%%
if __name__ == "__main__":
    args = t_args()
    print(args)
    user_tweets_folder = 'Data/Tweet_by_user/chunks/{}/user_tweets'.format(args.name)
    user_tweets_files = get_all_files(user_tweets_folder,end_with='.csv')
    output_folder = 'Data/Tweet_by_user/chunks/{}/comments_by_user_tweets'.format(args.name)
    maybe_create(output_folder)
    error_folder = 'Data/Tweet_by_user/chunks/{}/error'.format(args.name)
    maybe_create(error_folder)
    
    #%%
    key_path = 'keys/twitter_api_key.json'
    keys = read_json(key_path)
    bearer_token = keys[args.key]['Bearer_Token']
    #bearer_token = keys['Shiying']['Bearer_Token']
    #max_page = 5    ## for testing purpose , in real case, can set it to 1000 ; for here, 
                    ## get 5*100 is more than enough for sentiment agg

    #%%
    for utf in tqdm.tqdm(user_tweets_files):
    ## go through all user tweet files 
        print('working on {}'.format(utf))
        u_name = utf.split('user_tweets_v2')[1].split('.')[0]
        df_tid = get_tweet_ids(utf,threshold=args.threshold) ## only get those tweets that starts a conversion
        
        ##############################
        #### go through all tweets  ## 
        ##############################
        all_results,error_ids = batch_get_comments_by_conv_ids(df_tid.id.tolist(),batch=True)
            
        ############################
        #### try all errors again ##
        ############################
        time.sleep(120) ## wait for 1 seconds 
        new_results,final_error_ids = batch_get_comments_by_conv_ids(error_ids,batch=False)
        all_results.extend(new_results)
            
        #####################################
        #### put everything in a dataframe ##
        #####################################
        try:
            df_results = pd.DataFrame(all_results)
        except Exception as e:
            print('failed to convert to df: {}'.format(e))
        
        try:
            df_results.to_excel(os.path.join(output_folder,'{}__comments.xlsx'.format(u_name)),index=False)
        except:
            print('failed to save to excel')
        try:
            df_results.to_pickle(os.path.join(output_folder,'{}__comments.pkl'.format(u_name)))
        except:
            print('failed to save to pkl')
            
        if len(error_ids)>0:
            pd.DataFrame(error_ids).to_excel(os.path.join(error_folder,'{}__comments_error.xlsx'.format(u_name)))

#     #%%
#     utf = user_tweets_files[0]
#     df_tid = get_tweet_ids(utf,threshold=1)
#     conv_ids = df_tid.id.tolist()
#     #%%
#     res = get_reply_and_quote(conv_ids[:5]) 
#     #%%
#     #res,errors= batch_get_comments_by_conv_ids(conv_ids)   
#     max_page=5
#     cid = '1602352521435021318'
#     search_url = "https://api.twitter.com/2/tweets/{}/quote_tweets".format(cid)    
#     quote_param = get_quote_param()    
#     T_quotes = Twitter_API(bearer_token,search_url)
#     res = T_quotes.connect_to_endpoint(quote_param,url=search_url,to_df=False,verbose=True,next_token=None,next_token_type='pagination_token',n_try=1,wait_n=10)

# #%%
#     quotes_count,quotes_tweets = T_quotes.get_all_twits(quote_param,max_page=max_page, ## we set to retrieve only 1000 quotes per tweet ; should be large engough
#                                             to_df=False,verbose=False,next_token_type='pagination_token',
#                                             n_try=3,wait_n=60)