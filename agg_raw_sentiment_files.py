## agg raw sentiment files 
import os,sys
sys.path.insert(0,'libs')
import pandas as pd
from util import literal_return
from tqdm import tqdm

def post_process_df(t_df):
    keep_columns = ['created_at','text_processed','id','label','category','Country']
    t_df = t_df[keep_columns]
    t_df = t_df[t_df['Country'] != '[]']                                                ## remove no country assigned data
    t_df = t_df.drop_duplicates(subset='id', keep="first")
    t_df['Country'] = t_df['Country'].apply(literal_return)
    t_df = t_df.explode('Country')
    t_df['label'] = t_df['label'].apply(lambda x: 1 if x==0 else 0)                      ## code binary for negative
    t_df.columns = ['created_at','text_processed','id','negative','category','country']  ## rename to be consistent 
    t_df['time'] = t_df['created_at'].str[:10]
    t_df['time'] = pd.to_datetime(t_df['time'],infer_datetime_format=True)
    t_df['time_month'] = t_df['time'].apply(lambda t: t.to_period('M'))
    t_df = t_df[['created_at','time','time_month','text_processed','id','negative','category','country']]
    return t_df

def sequential_aggregate(in_f_p):
    ## get category lable from file name
    if 'nonmarket' in os.path.basename(in_f_p):
        category = 'nonmarket'
    else:
        category = 'market'
    
    ## read and process by chunks 
    chunks = pd.read_csv(in_f_p,chunksize = 50000,engine='python',error_bad_lines=False)
    sum_dfs = []
    for idx,chunk in enumerate(chunks):#,desc='process sub-chunks'
        chunk['category'] = category
        t_df = post_process_df(chunk)
        agg_df = t_df.groupby(['time_month','country','category','negative']).agg({'id':['count']})
        agg_df.columns = agg_df.columns.map('_'.join)
        agg_df.reset_index(inplace=True)
        sum_dfs.append(agg_df)
        #print(len(agg_df))
    
    final_agg_df = pd.concat(sum_dfs)
    #print(len(final_agg_df))
    final_agg_df = final_agg_df.groupby(['time_month','country','category','negative'])['id_count'].sum()
    final_agg_df = final_agg_df.reset_index()
    
    return final_agg_df

def post_process_agg(agg_df):
    
    ##############
    ## Pivot label to get negative seperately 
    ##############
    agg_df_wide=pd.pivot(agg_df,index=['country','category','time_month'],columns='negative',values='id_count')
    agg_df_wide.rename(columns={0: 'neutral-positive', 1: 'negative'}, inplace=True)
    agg_df_wide[['neutral-positive','negative']] = agg_df_wide[['neutral-positive','negative']].fillna(0)
    agg_df_wide.reset_index(inplace=True)
    
    ##############
    ## calculate rolling average and std
    ##############
    agg_df_wide['negative_share'] = agg_df_wide['negative'] / (agg_df_wide['negative'] + agg_df_wide['neutral-positive'])
    agg_df_wide['12m_ma'] = agg_df_wide.groupby(['country','category'])['negative_share'].transform(lambda x: x.rolling(12, 1).mean())
    agg_df_wide['12m_sd'] = agg_df_wide.groupby(['country','category'])['negative_share'].transform(lambda x: x.rolling(12, 1).std())
    agg_df_wide['up_over_1sd'] = (agg_df_wide['negative_share'] - agg_df_wide['12m_ma'])/agg_df_wide['12m_sd'] >1
    
    return agg_df_wide

#%%
if __name__ =="__main__":
    in_data_folder = 'Data/Market_Non-Market_by_year_withSentiment_country'
    out_data_folder = 'Data/Market_Non-Market_AGG'
    final_out_path = 'Data/All_agg.csv'
    
    ## set up in out fine pathes for multi process 
    in_files = os.listdir(in_data_folder)
    in_file_pathes = [os.path.join(in_data_folder,in_f) for in_f in in_files]
    out_file_pathes = [os.path.join(out_data_folder,in_f) for in_f in in_files]
    #%%
    All_agg_df_list = []
    for in_f_p, out_f_p in tqdm(list(zip(in_file_pathes,out_file_pathes))):
        y_agg_df = sequential_aggregate(in_f_p)
        y_agg_df.to_csv(out_f_p,index=False)
        All_agg_df_list.append(y_agg_df)
    
    print('Process final aggregation ....')
    all_final_agg_df = pd.concat(All_agg_df_list)
    all_final_agg_df = all_final_agg_df.groupby(['time_month','country','category','negative'])['id_count'].sum()
    all_final_agg_df = all_final_agg_df.reset_index()
    all_final_agg_df.to_csv(final_out_path)
    print('write to {}'.format(final_out_path))

    #%%
    print('calculate timeseries statistics ....')
    df_stats = post_process_agg(all_final_agg_df)
    df_stats.to_csv('Data/All_agg_stats.csv')
    
    