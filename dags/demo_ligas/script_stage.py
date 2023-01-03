''' Libraries '''
import pandas as pd
import time
import random
import os

from datetime import datetime, timedelta
import uuid
import hashlib
import os
import logging
'''Airflow '''
import airflow
from airflow import models
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
import snowflake.connector as sf

''' Functions'''
from utils import get_ligas, generate_id




''' DAG'''

default_arguments = {   'owner': 'Ale Engelstajn',
                        'email' : 'aleengelstajn@gmail.com',
                        'retries' : 1,
                        'retry_delay': timedelta(minutes=5) }
                        

with DAG('FUTBOL_LEAGUES',
        default_args=default_arguments,
        description= 'Extracting data from futbol leagues',
        start_date = datetime(2022,12,30),
        schedule_interval=None, 
        tags = ['tabla espn'],
        catchup=False) as dag:


        params_info = Variable.get('feature_info', deserialize_json= True)

        def extract_info():
                df = get_ligas()
                df.rename(columns={'Equipos':'equipo', 'J':'jugados', 'G':'ganados', 'E':'empatados', 'P': 'perdidos', 'GF':'gf', 'GC':'gc', 'DIF':'diff', 'PTS':'puntos', 'Liga':'liga',
                'CREATED_AT':'created_at', 'Id':'id'}, inplace=True)
                df = df[['id', 'equipo', 'jugados', 'ganados', 'empatados', 'perdidos', 'gf', 'gc', 'diff', 'puntos', 'liga', 'created_at']]
                df.to_csv('posiciones.csv', index=False)
        
        

        extract_data = PythonOperator(task_id='Extract_futbol_data',
                                python_callable=extract_info)


        upload_stage = SnowflakeOperator(

                task_id='upload_data_stage',
                sql='./queries/upload_stage.sql',
                snowflake_conn_id='snowflake_conn',
                warehouse=params_info["DWH"],
                database=params_info["DB"],
                role=params_info["ROLE"],
                params=params_info
                )


        ingest_table = SnowflakeOperator(

                task_id='ingest_table',
                sql='./queries/upload_table.sql',
                snowflake_conn_id='snowflake_conn',
                warehouse=params_info["DWH"],
                database=params_info["DB"],
                role=params_info["ROLE"],
                params=params_info
                )

extract_data >> upload_stage >> ingest_table

            



