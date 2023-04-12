
"""
Twitter impression score 
https://www.tweetbinder.com/blog/twitter-impressions/#:~:text=An%20impression%20is%20generated%20when,the%20hashtag%20got%2010%20impressions.

"""

#%%
import pandas as pd
import os,sys
sys.path.insert(0,'../libs')
from search_util import construct_rex, construct_rex_group,find_exact_keywords,get_keywords_groups
from util import get_all_files

#%%
if __name__ == "__main__":
    
    ## read key 
    user_file_path = '../search_keywords/Paraguay_Twitter_Search.xlsx'
    user_info_out = '../Data/Tweet_by_user//Paraguay_Twitter_user_info.xlsx'
    user_tweets_out = '../Data/Tweet_by_user/v2/user_tweets_v2.pkl'
    #user_mentions_out = '../Data/Tweet_by_user/user_mentions.pkl'
    final_out = '../Data/Tweet_by_user/user_data_all_v2{}'
    
    #search_keys = ["Cambio Climático", "Calentamiento Global"]
    search_key_groups = get_keywords_groups(user_file_path,sheet_name='keywords')
#%%
    ## read results 
    df_user_info = pd.read_excel(user_info_out) 
    df_tweets = pd.read_pickle(user_tweets_out)
    #df_mentions = pd.read_pickle(user_mentions_out)    

    ### format some columns 
    df_tweets['message_type'] = 'tweet' 
    df_tweets['Reference_Agency_ID'] = df_tweets['author_id']
    # df_mentions['message_type'] = 'mention' 
    # df_mentions.rename(columns={'mentioned_user_id':'Reference_Agency_ID'},inplace=True)
    
    ## merge 
    df_merge = df_tweets
    # df_merge = df_tweets.append(df_mentions,ignore_index=True)
    ## prepare for merge user info
    df_user_info['id']=df_user_info['id'].astype(str)
    df_merge['Reference_Agency_ID']=df_merge['Reference_Agency_ID'].astype(str)
    
    keep_user_info = ['username','created_at','name','followers_count','id']
    
    df_all = df_merge.merge(df_user_info[keep_user_info],how='left',
                            left_on='Reference_Agency_ID', right_on='id',
                            suffixes=('_left', '_right'))
#%%
    ## post merge process 
    ## search for keywors 
    df_all.rename(columns={'created_at_left':'created_at'},inplace=True)
    df_all['time'] = df_all['created_at'].str[:10]
    df_all['time'] = pd.to_datetime(df_all['time'],infer_datetime_format=True)
    df_all['time_month'] = df_all['time'].apply(lambda t: t.to_period('M'))
    df_all = df_all.join(pd.json_normalize(df_all.pop('public_metrics')))
#%%
    ## keep only useful fields for clean look 
    keep_simple = ['time','time_month','name','Reference_Agency_ID','followers_count',
                   'lang','message_type','retweet_count',
                    'reply_count', 'like_count', 'quote_count', 
                    'impression_count','text']
    df_simple = df_all[keep_simple]
#%%
    for k in search_key_groups.keys():
        search_keys = search_key_groups[k]
        rex = construct_rex(search_keys,plural=False,casing=False)
        df_simple['{}_count'.format(k)] = df_simple['text'].map(lambda c:find_exact_keywords(c,content_clean=True,rex=rex)[1])
        df_simple['{}_dummy'.format(k)] = (df_simple['{}_count'.format(k)]>0).astype(int)
        print('{} related ratio: {}'.format(k,df_simple['{}_dummy'.format(k)].sum()/len(df_simple)))
    
    #%%
    #df_simple['weighted_count'] = df_simple['climate_related']*df_simple['impression_count']
    sum_cols = ['{}_dummy'.format(k) for k in search_key_groups.keys()]
    df_simple['all_dummy'] = (df_simple[sum_cols].sum(axis=1)>1).astype(int)
#%%
    ## export results 
    df_simple.to_csv(final_out.format('.csv'),encoding='utf8',index=False)
    df_simple.to_pickle(final_out.format('.pkl'))
# %%
