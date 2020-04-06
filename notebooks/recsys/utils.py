import pandas as pd
import numpy as np
import shutil
import os

try:
    from tqdm import tqdm_notebook
except:
    from tqdm.notebook import tqdm as tqdm_notebook

"""
Recsys Challenge 2020: Twitter
"""

def read_data(path, N=None):
    """
    It reads the data from a given path.
    
    :N: Number of rows to be read. 'None' to read all the file.
    """
    
    # Tweet columns
    columns=['bert','hashtags','tweet_id','media','links','domains','type','language',
                              'timestamp','EWUF_user_id','EWUF_follower_count','EWUF_following_count',
                              'EWUF_verified','EWUF_account_creation','EUF_user_id','EUF_follower_count',
                              'EUF_following_count','EUF_verified','EUF_account_creation',
                              'engagee_follows_engager','reply_timestamp','retweet_timestamp',
                               'retweet_comment_timestamp','like_timestamp']

    # Read csv without separators. This will create a DataFrame with a sigle column
    df = pd.read_csv(path, nrows=N)
    
    # Split by the wierd character and expand all the fields into new columns
    df = df.iloc[:,0].str.split('\x01',expand=True)
    
    # Rename columns. Validation doesn't have the last 4 timestamps
    df.columns = columns if len(df.columns)==len(columns) else columns[:-4]
    
    # Create an index
    df = df.reset_index(drop=True)
    
    # Set nans
    df = df.replace(r'^$', np.nan, regex=True)
    df = df.replace(r'^false$', False, regex=True)
    df = df.replace(r'^true$', True, regex=True)

    return df


def create_submission(df_val, submission=1, create_zip=False):
    """
    Given a DataFrame, it creates four csv with the requested format.
    
    :df_val: Validation dataframe
    :submission: Number or string to create a folder in the submissions directory.
    :create_zip: If the folder will be compressed. Default is False.
    """
    
    try:
        sub_name = "{:03d}".format(submission)
    except:
        sub_name = "{:s}".format(submission)
       
    dirName = 'submissions/'+sub_name
    
    assert set(['reply','retweet','retweet_comment','like']).issubset(df_val.columns), "Target columns are not present in the dataframe"
    assert not df_val[['reply','retweet','retweet_comment','like']].isnull().values.any(), "NaN values not allowed"
    
    try:
        os.makedirs(dirName)    
        print("Directory " , dirName ,  " Created ")
        
    except FileExistsError:
        print("Directory " , dirName ,  " already exists") 
    
    for new_col in tqdm_notebook(['reply','retweet',
                    'retweet_comment','like']):
        df_val.loc[:,['tweet_id','EUF_user_id',new_col]].to_csv(dirName+'/'+new_col+'.csv',
                                                            index=None, header=None) 
        
    if create_zip:
        print("Zipping... ", end="")
        shutil.make_archive(sub_name, 'zip', dirName)
        os.rename(sub_name+".zip", dirName+'.zip')
        print("DONE!")
    
    
def fill_df_with_value(df_val, val=0.0):
    """
    Test function to create the 4 requested columns.
    
    :val: Value that will be populated to the four requested columns.
    """ 
    df_val['reply'] = val
    df_val['retweet'] = val
    df_val['retweet_comment'] = val
    df_val['like'] = val
    
    return df_val