# -*- coding: utf-8 -*-
"""
Created on Thu Oct 20 10:33:26 2022

@author: CHuang
"""

##### twitter, create policy specific series 
import os,sys
sys.path.insert(0,'libs')
import pandas as pd 
from search_util import construct_rex, construct_rex_group,find_exact_keywords,get_keywords_groups
import re
#%%

def read_key_from_template(key_file,sheet_name,lower=True):
    df = pd.read_excel(key_file,sheet_name).fillna('').values ## read everything into row by row array
    keys_dict = {i[0].strip():{'include':i[1].split('|'),'exclude':i[2].split('|')} for i in df}
    
    ## clean spaces 
    for k,v in keys_dict.items():
        keys_dict[k]['include']=[i.strip() for i in keys_dict[k]['include'] if i.strip() !='']
        keys_dict[k]['exclude']=[i.strip() for i in keys_dict[k]['exclude'] if i.strip() !='']
    
    if lower:
        keys_dict_lower = {}
        for k,v in keys_dict.items():
            keys_dict_lower[k.lower()] = {}
            keys_dict_lower[k.lower()]['include']=[i.strip().lower() for i in keys_dict[k]['include'] if i.strip() !='']
            keys_dict_lower[k.lower()]['exclude']=[i.strip().lower() for i in keys_dict[k]['exclude'] if i.strip() !='']
        return keys_dict_lower
    
    return keys_dict

#%%
def turn_template_keys2pattern_dict(search_dict):
    ## construct fisrt level anchor search pattern 
    ## and second leve include, exclude pattern 
    ## for now, we can only match it as it is, not all the other variations because match results are used as keys for second level logic retrieval 
    
    anchors = list(search_dict.keys())
    anchor_pattern = construct_rex(anchors,plural=False,casing=False)  ## for now, do turn off plural and casing, for simplicity of the process
    
    search_dict_pattern = {}
    for k in anchors:
        search_dict_pattern[k] = {}
        search_dict_pattern[k]['include'] = construct_rex(search_dict[k]['include'],plural=False,casing=False) ## for now, do turn off plural and casing, for simplicity of the process
        search_dict_pattern[k]['exclude'] = construct_rex(search_dict[k]['exclude'],plural=False,casing=False) ## for now, do turn off plural and casing, for simplicity of the process
    
    return anchor_pattern,search_dict_pattern

def advanced_match(content,anchor_pattern,search_dict_pattern,search_dict):
    matches,match_count = find_exact_keywords(content,rex = anchor_pattern)
    if match_count == 0:
        return {'status':False, 'anchor_check':False,'include_check':None,'exclude_check':None}
    
    matched_anchors = [i for i in matches.keys() if i in search_dict.keys()]
        
    for k in matched_anchors:
        if len(search_dict[k]['include']) == 0 and len(search_dict[k]['exclude']) == 0:
            return {'status':True, 'anchor_check':[k],'include_check':None,'exclude_check':None} 
        
        if len(search_dict[k]['include'])>0 and len(search_dict[k]['exclude']) == 0:
            inc_m,inc_c = find_exact_keywords(content,rex = search_dict_pattern[k]['include'])
            if inc_c>0:
                return {'status':True, 'anchor_check':[k],'include_check':list(inc_m.keys()),'exclude_check':None}
        
        if len(search_dict[k]['include'])==0 and len(search_dict[k]['exclude']) > 0:
            ex_m,ex_c = find_exact_keywords(content,rex = search_dict_pattern[k]['exclude'])
            if ex_c==0:
                #print(inc_m,ex_m)
                return {'status':True, 'anchor_check':[k],'include_check':None,'exclude_check':None}
            
        if len(search_dict[k]['include'])>0 and len(search_dict[k]['exclude']) > 0:
            inc_m,inc_c = find_exact_keywords(content,rex = search_dict_pattern[k]['include'])
            ex_m,ex_c = find_exact_keywords(content,rex = search_dict_pattern[k]['exclude'])
            if inc_c> 0 and ex_c==0:
                #print(inc_m,ex_m)
                return {'status':True, 'anchor_check':[k],'include_check':list(inc_m.keys()),'exclude_check':None}
    
    return {'status':False, 'anchor_check':matched_anchors,'include_check':None,'exclude_check':None}

def agg_reshape(in_df,policy_column):
    ## aggregate and reshape 
    keep_columns = ['created_at', 'text_processed', 'id', 'negative', 'category', 'country',
       'time', 'time_month', policy_column]
    df_ps = in_df[in_df[search_group]==True][keep_columns]
    agg_df = df_ps.groupby(['time_month','country','negative']).agg({'id':['count']}) ## do not group by category here 
    agg_df.columns = agg_df.columns.map('_'.join)
    agg_df.reset_index(inplace=True)
    agg_df.sort_values(by=['country','negative','time_month'],inplace=True)
    ### Pivot to wide and fill NA
    agg_df_wide=pd.pivot(agg_df,index=['country','time_month'],columns='negative',values='id_count')
    agg_df_wide.rename(columns={0: 'neutral-positive', 1: 'negative'}, inplace=True)
    agg_df_wide[['neutral-positive','negative']] = agg_df_wide[['neutral-positive','negative']].fillna(0)
    agg_df_wide.reset_index(inplace=True)
    
    return agg_df_wide

def cal_rolling_sd(in_df,groupby=['country','category']):
    in_df['negative_share'] = in_df['negative'] / (in_df['negative'] + in_df['neutral-positive'])
    in_df['12m_ma'] = in_df.groupby(groupby)['negative_share'].transform(lambda x: x.rolling(12, 1).mean())
    in_df['12m_sd'] = in_df.groupby(groupby)['negative_share'].transform(lambda x: x.rolling(12, 1).std())
    in_df['up_over_1sd'] = (in_df['negative_share'] - in_df['12m_ma'])/in_df['12m_sd'] >1
    
    return in_df

#%%
if __name__ == "__main__":
    results_folder = 'Data/final_result'
    raw_merged_data_out_path = os.path.join(results_folder,'twitter_merged_raw.csv')
    search_key_file = os.path.join('search_keywords','twitter_search_terms.xlsx')
    policy_specific_out_path = os.path.join(results_folder,'policy_specific','policy_specific_raw.csv')
    policy_specific_agg_path = os.path.join(results_folder,'policy_specific','policy_specific_agg_{}.csv')
    #df = pd.read_csv(raw_merged_data_out_path)
    #%%
    sheet_names = pd.ExcelFile(search_key_file).sheet_names  
    sheet_names = [sn for sn in sheet_names if sn!='README']
    ## read all raw twitter data 
    df = pd.read_csv(raw_merged_data_out_path)
    
    #%%
    ################
    ## serach for each policy instrument
    ################
    #search_group = sheet_names[0]
    for search_group in sheet_names:
        search_dict = read_key_from_template(search_key_file,search_group)    
        anchor_pattern,search_dict_pattern = turn_template_keys2pattern_dict(search_dict)
        df[search_group] = df['text_processed'].apply(lambda i:advanced_match(i,anchor_pattern,search_dict_pattern,search_dict)['status'])
    
    ####################
    ## export data #####
    df.to_csv(policy_specific_out_path,encoding='utf8')
    ####################
    for search_group in sheet_names:
        agg_df_wide = agg_reshape(df,search_group)
        agg_rolling = cal_rolling_sd(agg_df_wide,groupby=['country'])
        agg_rolling.to_csv(policy_specific_agg_path.format(search_group),encoding='utf8')