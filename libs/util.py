# -*- coding: utf-8 -*-
"""
Created on Thu Aug 11 10:26:39 2022

@author: chuang
"""

import json
import pandas as pd 
import ast
import pathlib
import os


def read_json(f_path:str):
    with open(f_path,'r') as f:
        data = json.load(f)
    
    return data 

def read_keywords(f_path:str):
    ## convert csv to keyword groups and all keys 
    df = pd.read_csv(f_path)
    key_groups = df.to_dict('list')
    key_groups = {k: [i.strip() for i in v if not pd.isna(i)] for k,v in key_groups.items()}
    # falttern the nested list 
    all_keys = [item for sublist in list(key_groups.values()) for item in sublist]
    
    return key_groups,all_keys

def chunk_list(my_list,size:int):
    """
    size : number of item per chunk
    """
    n=size
    # using list comprehension
    final = [my_list[i * n:(i + 1) * n] for i in range((len(my_list) + n - 1) // n )]
    return final 

def create_time_ranges(start=2007,end=2022,Freq='y'):
    time_ranges = []
    for p in range(start,end):
        time_ranges.append(('{}-01-01T00:00:00Z'.format(p),'{}-01-01T00:00:00Z'.format(p+1)))
    
    return time_ranges

def literal_return(val):
    try:
        return ast.literal_eval(val)
    except ValueError:
        return (val)

def get_all_files(dirName,end_with=None): # end_with=".json"
    # create a list of file and sub directories 
    # names in the given directory 
    listOfFile = os.listdir(dirName)
    allFiles = list()
    # Iterate over all the entries
    for entry in listOfFile:
        # Create full path
        fullPath = os.path.join(dirName, entry)
        # If entry is a directory then get the list of files in this directory 
        if os.path.isdir(fullPath):
            allFiles = allFiles + get_all_files(fullPath)
        else:
            allFiles.append(fullPath)
    
    if end_with:
        end_with = end_with.lower()
        allFiles = [f for f in allFiles if pathlib.Path(f).suffix.lower() == end_with ] 

    return allFiles  

def maybe_create(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    

#%%
                           