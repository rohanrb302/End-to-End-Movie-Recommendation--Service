import pandas as pd
import numpy as np
import re
from tqdm import tqdm
import dataconfig as cfg


def process_df(input_df,i):
    movieids_plain=input_df['movieid']
    movieids_clean=movieids_plain.str.replace(r"(\bGET\b|\/\bdata\b|\/m\/|\s+|\bmpg\b|\/[0-9]*\.)","",regex=True)
    input_df['movieid']=movieids_clean
    input_df=input_df.groupby(input_df.columns.tolist(),as_index=False).size()
    input_df.to_csv(cfg.data["watch_event_out_folder"]+f"watch_out_{i}.csv")
    return(input_df)
 
def main():
    chunksize = 10 ** 7
    index=0
    for chunk in tqdm(pd.read_csv(cfg.data['watch_event_in'], chunksize=chunksize,names=["datetime", "userid", "movieid"],usecols=["userid","movieid"])):
        process_df(chunk,index)
        index +=1
    
    return None


if __name__ == "__main__":
    main()
    pass


