# -*- coding: utf-8 -*-
"""
Created on Mon Nov  7 16:42:25 2022

@author: CHuang
"""

import pandas as pd
import glob
import os

def process_geo_data(geo_df,code2imf_map):
    ## read geocoded twitter data 
    #geo_df = pd.read_csv(geo_twitter_data_path)
    keep_columns = ['created_at','text_processed','id','negative','country']
    geo_df = geo_df[keep_columns]
    geo_df['country'].replace(code2imf_map,inplace=True)
    geo_df['negative'] = geo_df['negative'].apply(lambda x: 1 if x>0 else 0) ## > 0 recode as negateve, else positive
    #geo_df.columns = ['created_at','text_processed','id','negative','country']
    
    return geo_df

#%%
if __name__ == "__main__":
    
    AGG_RAW = False
    AGG_SENTI = True
    
    ######################
    ## aggregate raw data 
    ######################
    if AGG_RAW:
        data_folder = 'Data/climate_awareness'
        # setting the path for joining multiple files
        files = os.path.join(data_folder,'RAW', "*.csv")
        # list of merged files returned
        files = glob.glob(files)
        print("Resultant CSV after joining all CSV files at a particular location...");
        # joining files with concat and read_csv
        df = pd.concat(map(pd.read_csv, files), ignore_index=True)
        print(len(df))
        #print(df.head())
        df.to_csv(os.path.join(data_folder,"climate_awareness_agg.csv"), index=False, encoding='utf-8-sig')
    
    #%%
    ############################
    ## aggregate sentiment data 
    ############################
    if AGG_SENTI:
        data_folder = 'Data/climate_awareness'
        file = os.path.join(data_folder,'climate_awareness_agg_senti.csv')
        out_file = os.path.join(data_folder,'climate_awareness_dashboard.csv')
        cm_path = 'libs/country_map.csv'
        
        ## read country mapping
        country_df = pd.read_csv(cm_path)
        ctag2imf_map = dict(zip(country_df['Identified_Country_Name'], country_df['IMF_Country_Name']))
        code2imf_map = dict(zip(country_df['2_Code'], country_df['IMF_Country_Name']))
        #%%
        ## read raw data 
        t_df = pd.read_csv(file,engine='python')
        t_df = process_geo_data(t_df,ctag2imf_map)
        ## processed merged data 
        t_df['time'] = t_df['created_at'].str[:10]
        t_df['time'] = pd.to_datetime(t_df['time'],infer_datetime_format=True)
        t_df['time_month'] = t_df['time'].apply(lambda t: t.to_period('M'))
        
        ##########################
        ## aggregate data  ###
        ##########################
        agg_df = t_df.groupby(['time_month','country','category','negative']).agg({'id':['count']})
        agg_df.columns = agg_df.columns.map('_'.join)
        agg_df.reset_index(inplace=True)
        agg_df.sort_values(by=['country','category','negative','time_month'],inplace=True)
        
        del t_df
        
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
        agg_df_wide.to_csv(out_file)