# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 14:41:42 2023

@author: CHuang
"""

import pandas as pd 
import os 
import numpy as np

def sequential_aggregate(in_f_p):
    all_cts = 0
    ## read and process by chunks 
    chunks = pd.read_csv(in_f_p,chunksize = 50000,engine='python',error_bad_lines=False)
    for idx,chunk in enumerate(chunks):#,desc='process sub-chunks'
        
        # chunk['agg_count'] = chunk['climate tag'] +chunk['policy tag']
        # chunk['agg_count'] = np.where(chunk['agg_count']==2, 1, 0)
        cts = chunk['climate policy'].sum()
        
        all_cts = all_cts + cts
        print('{}; total counts :{}'.format(idx,all_cts))
    return all_cts

#%%
data_folder = 'C:/Users/chuang/OneDrive - International Monetary Fund (PRD)/Climate Change Challenge/FT news/Latest/News_Raw_Processed_Data'
raw_file = os.path.join(data_folder,'raw','newslist_7.18.csv')

#%%
chunks = pd.read_csv(raw_file,chunksize = 50000,engine='python',error_bad_lines=False)
#%%
counts = sequential_aggregate(raw_file)