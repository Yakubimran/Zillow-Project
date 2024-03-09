from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowFailException, AirflowSkipException
from datetime import datetime
import requests
import snowflake.connector
import os
import pandas
import configparser

config = configparser.ConfigParser()
config_path = '/opt/airflow/config/zillow.ini'
config.read(config_path)

def fetch_zillow_data():
    try:
        # Fetch data from Zillow API
        url = "https://zillow56.p.rapidapi.com/search"
        querystring = {"location":"houston, tx"}
        headers = {
            "X-RapidAPI-Key": config["DEFAULT"]["zillow_api_key"],
            "X-RapidAPI-Host": "zillow56.p.rapidapi.com"
        }
        response = requests.get(url, headers=headers, params=querystring)

        print(response.json())
        data = response.json()["results"]
        print('Response data:', data)  # Print the entire response data for debugging

        df = pandas.json_normalize(data)
        # Store into a local file
        file_path="/opt/airflow/tmp/data.csv"

        # remove dots in column names created by unnesting some nested columns
        df.columns = [x.split('.')[-1] for x in df.columns]
        df.to_csv(file_path, index=False)

    except Exception as e:
        raise AirflowFailException(f'Error occurred: {e}')

def store_in_snowflake(**kwargs):
    try:    
        # consider using airflow connection set up in the UI instead
        snowflake_account = config["DEFAULT"]["snowflake_account"]
        snowflake_user = config["DEFAULT"]["snowflake_user"]
        snowflake_password = config["DEFAULT"]["snowflake_password"]
        snowflake_warehouse = config["DEFAULT"]["snowflake_warehouse"]
        snowflake_database = 'ZILLOW'
        snowflake_schema = 'PUBLIC'

        conn = snowflake.connector.connect(
            account=snowflake_account,
            user=snowflake_user,
            password=snowflake_password,
            warehouse=snowflake_warehouse,
            database=snowflake_database,
            schema=snowflake_schema
        )

        cursor = conn.cursor()

        #create file format
        create_file_format_sql = '''create or replace file format zillow_file_format
                            type = csv skip_header=1;'''

        cursor.execute(create_file_format_sql)

        #create stage
        create_stage_sql = '''create or replace stage zillow_stage
                        file_format = zillow_file_format; '''
        cursor.execute(create_stage_sql)        

        # copy file to stage
        file_path="/opt/airflow/tmp/data.csv"
        cursor.execute(f'put file://{file_path} @zillow_stage;')

        # creaet sample table that fits the sample file we are working with
        ## first get column names from csv file first row

        columns = pandas.read_csv(file_path, nrows=0).columns.to_list()
        columns_sql_definition = ',\n\t'.join(x.upper() + " varchar(250)" for x in columns)
        ###check it out :)
        print(columns_sql_definition)

        ## create the table using extracted column names  
        cursor.execute(f'''create or replace table zillow_work (
            {columns_sql_definition}
            );
            ''')

        #copy from stage to snowflake table
        cursor.execute("copy into zillow_work from @zillow_stage file_format = (format_name='zillow_file_format') ON_ERROR='continue';")

        conn.commit()
        cursor.close()
        conn.close()
        
        print("Data inserted successfully into Snowflake table.")
    except Exception as e:
        raise AirflowFailException("Error inserting data into Snowflake table:", str(e))

default_args = {
    'owner': 'Yakubimran',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'zillow_data_pipeline',
    default_args=default_args,
    description='A DAG to fetch data from Zillow API and store it in Snowflake',
    schedule_interval='0 19 * * *',  # Runs once a day at 7 PM
)

fetch_zillow_data_task = PythonOperator(
    task_id='fetch_zillow_data_task',
    python_callable=fetch_zillow_data,
    dag=dag,
)

store_in_snowflake_task = PythonOperator(
    task_id='store_in_snowflake_task',
    python_callable=store_in_snowflake,
    provide_context=True,
    dag=dag,
)

fetch_zillow_data_task >> store_in_snowflake_task
