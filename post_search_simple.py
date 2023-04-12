# -*- coding: utf-8 -*-
"""
Created on Wed Mar 15 16:22:22 2023

@author: CHuang
"""

import pandas as pd 
import numpy as np
import os , sys, tqdm,argparse
sys.path.insert(0,'./libs')
from util import get_all_files
from search_util import get_keywords_groups,construct_rex,find_exact_keywords

#%%

def process_keywords_with_logic(seasrch_key_files,return_merged_keys=True,
                                return_logic=True,and_key='\+',key_lower=True,
                                search_sheet_name=None):
    """
    Parameters
    ----------
    seasrch_key_files : file path; string
    Returns
    -------
    kg : Dict
        a dict of keywords groups 
    all_keys : List
        all search key merged together
    """
    kg = get_keywords_groups(seasrch_key_files,lower=key_lower,sheet_name=search_sheet_name)
    keys_nested = list(kg.values())
    
    ## remove duplicates 
    all_keys = list(set([item for sublist in keys_nested for item in sublist]))
    
    ## break keys with logics 
    filtered_list = [item for item in all_keys if and_key not in item]
    logical_keys = [item for item in all_keys if item not in filtered_list]
    
    ## process logical keys and merge them back 
    if len(logical_keys)>0:
        logical_keys_split = [item.split(and_key) for item in logical_keys]
        logical_keys_split = list(set([item.strip() for sublist in logical_keys_split for item in sublist]))
        
        filtered_list.extend(logical_keys_split) ## merge logical terms back togeher 
        filtered_list = list(set(filtered_list)) ## remove duplicates again 
        
    return kg,filtered_list, logical_keys


def t_args(args_list=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('-country_name', '--country_name', action='store', dest='country_name',
                    default='Argentina',type=str)
    parser.add_argument('-search_sheet_name', '--search_sheet_name', action='store', dest='search_sheet_name',
                    default='Keywords_Portuguese_Climate',type=str)  
    if args_list is not None:
        args = parser.parse_args(args_list) 
    else:
        args = parser.parse_args()    
        
    return args

#%%

if __name__ == "__main__":
    
    args = t_args()
    country_name =args.country_name #'Brazil' #Argentina Uruguay Paraguay
    search_sheet_name = args.search_sheet_name #'Keywords_Portuguese_Climate'
    
    inputs_folder = 'Data/Tweet_by_user/chunks/{}/user_tweets'.format(country_name)
    raw_files = get_all_files(inputs_folder,end_with='.xlsx')
    result_file = 'Data/Tweet_by_user/chunks/{}/user_tweets_v2{}.csv'.format(country_name,country_name)
    seasrch_key_files = './search_keywords/twitter_search_terms_v4.xlsx'
    and_key = '\+'
    #normas+mercado
    keywords_dict , all_search_keywords, logical_keys = process_keywords_with_logic(seasrch_key_files,
                                                                                    search_sheet_name=search_sheet_name)
    
    search_rex = construct_rex(all_search_keywords,casing=False,plural=False)  ## here we are using case insensitive
    #%%
    #raw_merged_data_out_path = os.path.join(results_folder,'user_tweets_v2agenciabrasil.xlsx')
    all_result_df = []
    for raw_file in tqdm.tqdm(raw_files):
        df = pd.read_excel(raw_file)
        ## some simple data cleaning and filtering 
        df['text'] = df['text'].astype(str)
        df = df[df['text'].str.len() > 5]
        ## search keywords 
        df['search_res'] = df['text'].str.lower().apply(find_exact_keywords,rex=search_rex,return_count=False)   
        ## expode matched results 
        df = df.join(pd.json_normalize(df.pop('search_res'))).fillna(0)
        matched_cols = [k for k in df.columns if k in all_search_keywords] ## all are lower case, so we are fine here  
        df[matched_cols] = df[matched_cols].applymap(lambda x: 1 if x >= 1 else 0) ## turn all match to dummies 
        ## apply logicals based on keywords patterns 
        for lk in logical_keys:
            ks = [k.strip() for k in lk.split(and_key)]
            if all([k in matched_cols for k in ks]):
                df[lk] = df[ks].sum(axis=1)
                df[lk] = np.where(df[lk] == len(ks), 1, 0)
            else:
                df[lk] = 0 
        ## drop original columns 
        for k in ks:
            if k in matched_cols:
                df.drop([k],axis=1)
                
        ## aggregate statistics and filter 
        climate_columns = [k for k in keywords_dict['Climate_Keys'] if k in matched_cols + logical_keys]
        policy_columns = [k for k in keywords_dict['Policy_Keys'] if k in matched_cols + logical_keys]
        df['Climate_tag'] = df[climate_columns].sum(axis=1).clip(upper=1)
        df['Policy_tag'] = df[policy_columns].sum(axis=1).clip(upper=1)
        df['Keep_tag'] = (df['Climate_tag'] + df['Policy_tag']).apply(lambda x: 1 if x >= 2 else 0)
        keep_df = df[df['Keep_tag']>0]
        all_result_df.append(keep_df)

    final_agg_df = pd.concat(all_result_df).fillna(0)
    id_colums = ['id','conversation_id','author_id']
    df[id_colums] = df[id_colums].astype(str)
    final_agg_df.to_csv(result_file,index=False)
    #final_agg_df.to_excel(result_file,index=False)
    
    #%%
    
    # content = 'this is to test see if it can find '
    # matches = find_exact_keywords(content,rex=search_rex,return_count=False)
    # print(matches)
    