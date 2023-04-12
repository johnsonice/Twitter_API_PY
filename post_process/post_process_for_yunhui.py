# -*- coding: utf-8 -*-
"""
Created on Fri Jan 13 13:56:14 2023

@author: CHuang
"""

import pandas as pd


#%%
if __name__ =="__main__":
    in_data_path = 'Data/final_result_v3/All_agg_stats.csv'
    cm_path = 'libs/country_map.csv'
    out_data_path = 'Data/final_result_v3/All_agg_stats_for_yunhui.csv'
    
    ###############
    ## read country mapping
    ###############
    country_df = pd.read_csv(cm_path)
    ctag2imf_map = dict(zip(country_df['Identified_Country_Name'], country_df['IMF_Country_Name']))
    code2imf_map = dict(zip(country_df['2_Code'], country_df['IMF_Country_Name']))
    
    ###############
    ## read data from sources, and recode country name 
    ###############
    df = pd.read_csv(in_data_path)
    df['country'].replace(ctag2imf_map,inplace=True)
    df = df[df['country'].isin(ctag2imf_map.values())] 
    
    ###############
    ## create global quantiles  
    ###############
    df['percentile_25'] = df.groupby(['category', 'time_month'])['negative_share'].transform(lambda x: x.quantile(0.25))#.rename('colA_sum').reset_index()
    df['percentile_50'] = df.groupby(['category', 'time_month'])['negative_share'].transform(lambda x: x.quantile(0.5))
    df['percentile_75'] = df.groupby(['category', 'time_month'])['negative_share'].transform(lambda x: x.quantile(0.75))
    
    df.to_csv(out_data_path,index=False)