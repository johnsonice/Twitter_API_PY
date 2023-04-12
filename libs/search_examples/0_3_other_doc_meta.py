# -*- coding: utf-8 -*-
"""
Created on Mon Dec 17 09:29:44 2018

@author: chuang
"""
import os 
os.chdir('U:/Dev/Tim_Fiscal_multiplier')
import pickle 
import pandas as pd
from langdetect import detect

#%%
## improt all staff documents 
doc_dict_xml = pickle.load(open('data/xlm_docs.p', "rb")) 
doc_dict_txt = pickle.load(open('data/txt_docs.p', "rb"))

doc_dict = {**doc_dict_xml, **doc_dict_txt}

doc_ids = list(doc_dict.keys())

#%%

def get_doc_length(text):
    """ get document word count"""
    try:
        tokens = text.split()
    except:
        return 0
    return len(tokens)

def detect_language(text):

    try:        
        lang = detect(text)
    except:
        return 'NA'
    return lang

#%%
if __name__ == "__main__":
    meta_list = list()
    for i in doc_ids:
        text = ' '.join(doc_dict[i].paras)
        word_count = get_doc_length(text)
        lang = detect_language(text)
        meta_list.append((str(i),lang,word_count))
    
    #%%
    org_meta_path = "./data/meta_all.xlsx"
    meta_df = pd.read_excel(org_meta_path,sheet_name="all")
    meta_df['doc_id'] = meta_df['doc_id'].astype(str)
    meta_df['cor'] = meta_df['doc_id'].apply(lambda x: 'cor.' in str(x).lower())
    meta_df['request'] = meta_df['title'].apply(lambda x: 'request' in str(x).lower())
    addi_meta_df = pd.DataFrame(meta_list,columns=['doc_id','lang','word_count'])
    meta_df_update = pd.merge(meta_df,addi_meta_df,on='doc_id',how='left')    
    #%%
    meta_df_update.to_excel("./data/meta_updated.xlsx")