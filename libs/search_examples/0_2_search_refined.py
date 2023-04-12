# -*- coding: utf-8 -*-
"""
Created on Thu Sep 28 11:58:01 2017

@author: chuang
"""
#####################
## search keywords ##
#####################
import os 
os.chdir('U:/Dev/Tim_Fiscal_multiplier')
from util import  find_exact_keywords,find_exact_keywords_with_pos, read_keywords,construct_rex,construct_rex2#, read_meta, get_ids
import pickle 
import pandas as pd
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.stem import WordNetLemmatizer
import pandas as pd
import re
#%%
class rep(object):
    def __init__(self,rep_dict):
        self.rep_dict = dict((re.escape(k), v) for k, v in rep_dict.items())
        self.pattern = re.compile("|".join(rep_dict.keys()))
    
    def replace(self,text):
        result = self.pattern.sub(lambda m: self.rep_dict[re.escape(m.group(0))], text)
        return result

def read_pattern(keys,rep):    
    key_list = [l.lower() for l in keys if ' ' in l]
    rep_dict = dict([(s,s.replace(' ','_')) for s in key_list])
    pattern = rep(rep_dict)
    return pattern

def clean_up_setances(docs,pattern):
    """
    passins a list of documents 
    """
    replace_num = re.compile(r'\(.*?\)')
    docs = [replace_num.sub('',p) for doc in docs for p in doc]
    docs = [pattern.replace(doc.lower()) for doc in docs]
    return docs 

def read_key_pattern(key_file,sheet_name):
    df = pd.read_excel(key_file,sheet_name).fillna('').values
    keys = [i[0].strip() for i in df]
    pattern = read_pattern(keys,rep)
    keys_dict = {i[0].strip().replace(' ','_'):{'include':i[1].split('|'),'exclude':i[2].split('|')} for i in df}
    keys = list(keys_dict.keys())
    
    ## clean spaces 
    for k,v in keys_dict.items():
        keys_dict[k]['include']=[i.strip() for i in keys_dict[k]['include']]
        keys_dict[k]['exclude']=[i.strip() for i in keys_dict[k]['exclude']]
    
    return keys_dict,keys,pattern

def process_text(doc_dict,doc_id,_keys,_pattern):
    ## get document text and replace key with _ in between if it is a phrase 
    content = ' '.join(doc_dict[doc_id].paras)
    content = _pattern.replace(content)
    ## get matched keys
    _rex = construct_rex(_keys)
    match = find_exact_keywords(content.lower(),_keys,_rex)
    matched_keys = list(match.keys())  ## get all matched keys 
    ## tokenize documents
    sentances = sent_tokenize(content)
    sentances = [word_tokenize(s) for s in sentances]
    sentances_dict = {i:[s,len(s)] for i,s in enumerate(sentances)}
    sent_pos = dict()
    for i in range(len(sentances_dict)):
        if i == 0:
            sent_pos[i] = sentances_dict[i][1]
        else:
            sent_pos[i]= sentances_dict[i][1] + sent_pos[i-1]
    tokens = [t for s in sentances for t in s]
    ## double check 
    sentance_len = sum(len(s) for s in sentances)
    toekn_len = len(tokens)

    assert sentance_len == toekn_len
    
    return sentances_dict,sent_pos,tokens,matched_keys


def get_sentance_content(key_index,sentances_dict,sent_pos):
    '''
    Get the sentance where search keywords appear
    '''
    for i in range(len(sentances_dict)):
        if key_index < sent_pos[i]:
            sent_id = i 
            break
    sentance_content = ' '.join(sentances_dict[sent_id][0])
    return sentance_content

def get_range_content(window_size,key_index,tokens):
    '''
    Get range for checking context keywords
    '''
    ws = window_size//2
    start = key_index - ws
    end = key_index + ws 
    if start < 0: start = 0 
    if end > len(tokens): end = len(tokens)
    range_content = ' '.join(tokens[start:end])
    
    return range_content

def get_neg_range_content(ng_window_size,key_index,tokens):
    '''
    Get the range for checking negation words
    '''
    ws = window_size//2
    start = key_index - ws
    end = key_index + ws 
    if start < 0: start = 0 
    if end > len(tokens): end = len(tokens)
    range_content = ' '.join(tokens[start:end])
    
    return range_content

def check_context_words(keys,content):
    rex = construct_rex2(keys)
    match = [m.group() for m in rex.finditer(content)]
    if len(match) > 0 :
        res = 1
    else:
        res = 0 
    return res

def search_key_and_context(doc_ids,doc_dict,target_keys,key_dict,_pattern,window_size=30,ng_window_size=10):
    ## first stage, get all matched keys and contents ready
    ## get all text contents
    print('.... stage 1 sreach key words ....')
    res_stage1 = list()
    for doc_idx,doc_id in enumerate(doc_ids):
        ## print progress
        if doc_idx%100 == 0 :
            print('{}/{}'.format(doc_idx,len(doc_ids)))
        ## get contents 
        sentances_dict,sent_pos,tokens,matched_keys = process_text(doc_dict,doc_id,target_keys,_pattern)
        for sk in matched_keys:
            key_indexes = [i for i, s in enumerate(tokens) if s == sk]
            for key_index in key_indexes:
                sentance_content = get_sentance_content(key_index,sentances_dict,sent_pos)
                range_content =  get_range_content(window_size,key_index,tokens)
                neg_range_content = get_neg_range_content(ng_window_size,key_index,tokens)
                res_stage1.append((doc_id,sk,sentance_content,range_content,neg_range_content))
    print(len(res_stage1))
    ## second stage, search context
    print('.... stage 2 sreach context words ....')
    res_stage2 = list()
    for idx,vals in enumerate(res_stage1):
        if idx%5000 == 0 :
            print('{}/{}'.format(idx,len(res_stage1)))
        doc_id,sk,sentance_content,range_content,ng_range_content = vals
        if sk in target_keys:
            search_keys = key_dict[sk]['include']
            ng_keys = key_dict[sk]['exclude']
        elif sk[:-1] in target_keys:
            search_keys = key_dict[sk[:-1]]['include']
            ng_keys = key_dict[sk[:-1]]['exclude']
        elif sk[:-2] in target_keys:
            search_keys = key_dict[sk[:-2]]['include']
            ng_keys = key_dict[sk[:-2]]['exclude']
        elif sk[:-3] in target_keys:
            search_keys = key_dict[sk[:-3]]['include']
            ng_keys = key_dict[sk[:-3]]['exclude']
        else:
            raise ValueError  (sk +' not in dictionary')
        
        ## just to make sure there is no spaces
        search_keys = [k.strip() for k in search_keys]
        ng_keys = [k.strip() for k in ng_keys]
        ## check context , if include is nothing, return 1, otherwise run check
        if search_keys == ['']:
            sentance_res = 1 
            range_res = 1 
        else:
            sentance_res = check_context_words(search_keys,sentance_content)
            range_res = check_context_words(search_keys,range_content)
        ## if no exclude, return 0, otherwise, check 
        if ng_keys == ['']:
            ng_res = 0 
        else:
            ng_res = check_context_words(ng_keys,ng_range_content)
        
        ## append results
        res_stage2.append((doc_id,sk,search_keys,ng_keys,sentance_content,range_content,sentance_res,range_res,ng_res))
    
    print('search finished')
    return res_stage2
#%%
if __name__ == "__main__":
    window_size=30
    ng_wineow_size = 10
    ## improt all staff documents 
    doc_dict_xml = pickle.load(open('data/xlm_docs.p', "rb")) 
    doc_dict_txt = pickle.load(open('data/txt_docs.p', "rb"))
    
    doc_dict = {**doc_dict_xml, **doc_dict_txt}
    
    doc_ids = list(doc_dict.keys())
    #%%
    key_file = 'fiscal_search_term_v2.xlsx'
    hawks_dict,hawks_keys,hawks_pattern = read_key_pattern(key_file,'keys_hawks')
    doves_dict,doves_keys,doves_pattern = read_key_pattern(key_file,'keys_doves')
    
    #%%
    res_hawks = search_key_and_context(doc_ids,doc_dict,hawks_keys,hawks_dict,hawks_pattern,window_size,ng_wineow_size)
    res_doves = search_key_and_context(doc_ids,doc_dict,doves_keys,doves_dict,doves_pattern,window_size,ng_wineow_size)
    
    #%%
    header_names = ['doc_id','searck_key','context_key','exclude_keys','sentance_content','range_content','sentance_match','range_match','exclude']
    df_hawks = pd.DataFrame(res_hawks,columns=header_names)
    df_doves = pd.DataFrame(res_doves,columns=header_names)
    df_hawks.to_csv('data/res_v2/res_hawks.csv',encoding='utf-8')
    df_doves.to_csv('data/res_v2/res_doves.csv',encoding='utf-8')
    
    #%%
    print('finished')

##%%
###### some tests
#sentance_content = 'multiplier should money fiscal policy be tightened '
#search_keys = ['']
#sentance_res = check_context_words(search_keys,sentance_content)
#print(sentance_res)
##%%
#_keys = ['multiplier','multipliers']
#_rex = construct_rex(_keys)
#match = find_exact_keywords(sentance_content.lower(),_keys,_rex)
#matched_keys = list(match.keys())  ## get all matched keys 
#print(matched_keys)

