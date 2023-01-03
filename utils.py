''' Libraries '''
import pandas as pd
import time
import random
import os

from datetime import datetime
import uuid
import hashlib


def get_ligas():
   
    #* Inner function to clean the teams names
    extract_after_lowercase = lambda s: s[s.index(next(c for c in s if c.islower()))-1:]
    #* Reading the dataframe with leagues and urls
    #df_ligas = pd.read_csv('./url_ligas.csv', usecols=['Ligas', 'urls'])
    df_ligas = pd.read_csv('url_ligas.csv', usecols=['Ligas', 'urls'])

    i=0
    #* Populating a new dataframe with all the leagues
    df_main = pd.DataFrame()
    for liga in df_ligas.Ligas:
        df = pd.read_html(df_ligas.urls[i])
        i+=1
        
        
        df = pd.concat((df[0], df[1]), axis=1)
        df['Liga'] = [liga]*len(df)
        df.rename(columns={df.columns[0]: 'Equipos'}, inplace=True)

        df_main = pd.concat([df_main, df], ignore_index=True)
        
        run_date = datetime.now()
        run_date = run_date.strftime("%Y-%m-%d")
        df_main['CREATED_AT'] = run_date
        
        


    df_main['Equipos'] = df_main.Equipos.apply(extract_after_lowercase)
    df_main['Id'] = df_main.Equipos.apply(generate_id)
    
    return df_main



def generate_id(text):
  token = str(uuid.uuid1().hex)
  
  return token[:8]





