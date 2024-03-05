from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime
import requests
import json
import snowflake.connector
import os
import pandas

def fetch_zillow_data():
        # Fetch data from Zillow API
        zillow_api_key = "0e7172fd8emsh2e0c3f11409c19ep140d4fjsn0494b236051a",
        url = "https://zillow56.p.rapidapi.com/search"
        querystring = {"location":"houston, tx"}
        headers = {
            "X-RapidAPI-Key": "0e7172fd8emsh2e0c3f11409c19ep140d4fjsn0494b236051a",
            "X-RapidAPI-Host": "zillow56.p.rapidapi.com"
        }
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()  # Raise an error for HTTP status codes other than 2xx
        data = response.text
        print('Response data:', data)  # Print the entire response data for debugging
        df = pandas.json_normalize(json.loads(data))
        # Store into a local file
        df.to_csv(file_path, index=False)
    except requests.exceptions.HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as e:
        print(f'Error occurred: {e}')

def store_in_snowflake(**kwargs):
        snowflake_account = 'kbmjdcf-uq74356'
        snowflake_user = 'Yakubimran'
        snowflake_password = 'Snowflakes12'
        snowflake_warehouse = 'ZILLOWPROJECT'
        snowflake_database = 'ZILLOW'
        snowflake_schema = 'PUBLIC'
        snowflake_table = 'ZILLOW_DATA'  

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
        file_location = "/opt/airflow/dags/data.csv"
        cursor.execute(f'put file://{file_location} @zillow_stage;')

        # creaet sample table that fits the sample file we are working with
        cursor.execute('create or replace table zillow_work (message varchar(255));')

        #copy from stage to snowflake table
        cursor.execute("copy into zillow_work from @zillow_stage file_format = (format_name='zillow_file_format');")

        conn.commit()
        cursor.close()
        conn.close()
        
        print("Data inserted successfully into Snowflake table.")
    except Exception as e:
        print("Error inserting data into Snowflake table:", str(e))
        raise e  # Raising the exception for Airflow to handle and mark the task as failed

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
