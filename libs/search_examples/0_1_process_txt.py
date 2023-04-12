# -*- coding: utf-8 -*-
"""
Created on Thu Sep 28 17:36:10 2017

@author: chuang
"""

## process PDF txt file from Marc 

import os 
import pickle
#import pandas as pd
os.chdir('U:\Dev\Tim_Fiscal_multiplier')
from util import text_document, read_ids,read_meta #, find_exact_keywords, get_ids,read_keywords, 

#%%
data_path = 'data/pdf_text_documents/txt_documents'
txt_mata_path = 'data/pdf_text_documents/pdf_mata.csv'
txts = os.listdir(data_path)
txt_ids,mata = read_meta(txt_mata_path)
print(len(txt_ids))
## deleted those ones that are too short, usually corrections 
txts = [t for t in txts if t.split('.')[0] in txt_ids]

#%%
#file_name = txts[100]
#txt_path = os.path.join(data_path,file_name)
#file_id = file_name.split('.')[0]
#doc = text_document(file_id,txt_path)

#%%
dump = True
if dump:
    doc_list = list()
    total_length = len(txts)
    print('converting {} text into paragraph lists ......'.format(total_length))
    for idx,file_name in enumerate(txts):
        txt_path = os.path.join(data_path,file_name)
        try:
            file_id = file_name.split('.')[0]
        except:
            continue
        try:
            doc = text_document(file_id,txt_path)
            #docs_dict[doc.file_id] = doc
            doc_list.append(doc)
        except:
            print(file_name)
            
        if (idx+1)%50 == 0:
            print('{} / {} '.format(idx+1,total_length))
        
    doc_dict = {}
    for doc in doc_list:
        doc_dict[doc.file_id] = doc
        
    process_text = os.path.join('data','txt_docs.p')
    pickle.dump(doc_dict,open(process_text, "wb"))
    print('Finishing dumping to pickle')
    
#%%
#for x in doc_list:
#    
#    print(x.file_id,len(x.paras))