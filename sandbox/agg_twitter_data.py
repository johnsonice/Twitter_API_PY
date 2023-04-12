# -*- coding: utf-8 -*-
"""
Created on Mon Oct 17 16:24:19 2022

@author: CHuang
"""

### Merge geo coded and non geo coded twitter data 
### need to be reorganized latter  once underling files are updated 

import os,sys
sys.path.insert(0, 'libs')
import pandas as pd 
from util import literal_return

#%%

def read_augmented_data(twitter_data_path,ctag2imf_map):
    
    ## read augumented twitter data 
    t_df = pd.read_csv(twitter_data_path)
    keep_columns = ['created_at','text_processed','id','label','Market','Country',]
    t_df = t_df[keep_columns]
    t_df = t_df[t_df['Country'] != '[]']                                                ## remove no country assigned data
    t_df['Country'] = t_df['Country'].apply(literal_return)
    t_df = t_df.explode('Country')
    t_df['Country'].replace(ctag2imf_map,inplace=True)
    t_df = t_df[t_df['Country'].isin(ctag2imf_map.values())] 
    t_df['label'] = t_df['label'].apply(lambda x: 1 if x==0 else 0)                      ## code binary for negative
    t_df.columns = ['created_at','text_processed','id','negative','category','country']  ## rename to be consistent 
    
    return t_df

def read_geo_data(geo_twitter_data_path,code2imf_map):
    ## read geocoded twitter data 
    geo_df = pd.read_csv(geo_twitter_data_path)
    keep_columns = ['created_at','new_text','id','negative','category','country']
    geo_df = geo_df[keep_columns]
    geo_df['country'].replace(code2imf_map,inplace=True)
    geo_df['negative'] = geo_df['negative'].apply(lambda x: 1 if x>0 else 0) ## > 0 recode as negateve, else positive
    geo_df.columns = ['created_at','text_processed','id','negative','category','country']
    
    return geo_df


if __name__ == "__main__":
    
    results_folder = 'Data/final_result'
    cm_path = 'libs/country_map.csv'
    twitter_data_path = os.path.join(results_folder,'twitter_withCountry.csv')
    geo_twitter_data_path = os.path.join(results_folder,'aggtweets_senti.csv')
    raw_merged_data_out_path = os.path.join(results_folder,'twitter_merged_raw.csv')
    result_file_path = os.path.join(results_folder,'twitter_processed_for_dashboard.csv')
    
    #%%
    ###############
    ## read country mapping
    ###############
    country_df = pd.read_csv(cm_path)
    ctag2imf_map = dict(zip(country_df['Identified_Country_Name'], country_df['IMF_Country_Name']))
    code2imf_map = dict(zip(country_df['2_Code'], country_df['IMF_Country_Name']))
    
    #%%
    ###############
    ## read data from two sources 
    ###############
    t_df = read_augmented_data(twitter_data_path,ctag2imf_map)
    geo_df = read_geo_data(geo_twitter_data_path,code2imf_map)
    
    #%%
    ##########################
    ## merge/process  data ###
    ##########################
    t_df.append(geo_df,ignore_index=True)
    ## processed merged data 
    t_df['time'] = t_df['created_at'].str[:10]
    t_df['time'] = pd.to_datetime(t_df['time'],infer_datetime_format=True)
    t_df['time_month'] = t_df['time'].apply(lambda t: t.to_period('M'))
    
    ## export raw merged data for other downstream tasks 
    t_df.to_csv(raw_merged_data_out_path,index=False,encoding='utf8')
    
    #%% 
    ##########################
    ## aggregate data  ###
    ##########################
    agg_df = t_df.groupby(['time_month','country','category','negative']).agg({'id':['count']})
    agg_df.columns = agg_df.columns.map('_'.join)
    agg_df.reset_index(inplace=True)
    agg_df.sort_values(by=['country','category','negative','time_month'],inplace=True)
    
    del t_df, geo_df ## clear memory 
    #%%
    agg_df_wide=pd.pivot(agg_df,index=['country','category','time_month'],columns='negative',values='id_count')
    agg_df_wide.rename(columns={0: 'neutral-positive', 1: 'negative'}, inplace=True)
    agg_df_wide[['neutral-positive','negative']] = agg_df_wide[['neutral-positive','negative']].fillna(0)
    agg_df_wide.reset_index(inplace=True)
    
    #%%
    ##############
    ## calculate rolling average and std
    ##############
    agg_df_wide['negative_share'] = agg_df_wide['negative'] / (agg_df_wide['negative'] + agg_df_wide['neutral-positive'])
    agg_df_wide['12m_ma'] = agg_df_wide.groupby(['country','category'])['negative_share'].transform(lambda x: x.rolling(12, 1).mean())
    agg_df_wide['12m_sd'] = agg_df_wide.groupby(['country','category'])['negative_share'].transform(lambda x: x.rolling(12, 1).std())
    agg_df_wide['up_over_1sd'] = (agg_df_wide['negative_share'] - agg_df_wide['12m_ma'])/agg_df_wide['12m_sd'] >1
    #%% export to file 
    agg_df_wide.to_csv(result_file_path,index=False)
    
