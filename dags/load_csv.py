from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import Table, Column, MetaData, Float, Integer, String
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
    'retries': 3,  # Number of retries
    'retry_delay': timedelta(minutes=5), 
    'email_on_success':False,
    'email_on_failure':True,
    'email_on_retry':True,
    'email': ["marous.edu@gmail.com"],
}

# Define the DAG
dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='A DAG to load, clean, transform, and analyze CSV data before loading it into a database',
    schedule_interval='@daily',
    catchup=False,
)

# Callback for failure
def task_failure_alert(context):
    logging.error(f"Task {context['task_instance'].task_id} failed!")
    return

# Callback for success
def task_success_alert(context):
    logging.info(f"Task {context['task_instance'].task_id} succeeded!")
    return

# Callback for retry
def task_retry_alert(context):
    logging.warning(f"Task {context['task_instance'].task_id} is being retried.")
    return

# Define the function to create the table
def create_table():
    metadata = MetaData()
    table = Table(
        'MVR', metadata,
        Column('Date', String),
        Column('Open_M', Float),
        Column('High_M', Float),
        Column('Low_M', Float),
        Column('Close_M', Float),
        Column('Adj_Close_M', Float),
        Column('Volume_M', Integer),
        Column('Open_V', Float),
        Column('High_V', Float),
        Column('Low_V', Float),
        Column('Close_V', Float),
        Column('Adj_Close_V', Float),
        Column('Volume_V', Integer),
        Column('Price_Change_M', Float),
        Column('Price_Change_V', Float),
        Column('Pct_Change_M', Float),
        Column('Pct_Change_V', Float),
        Column('Volatility_M', Float),
        Column('Volatility_V', Float),
        Column('MA7_Close_M', Float),
        Column('MA7_Close_V', Float),
        Column('MA30_Close_M', Float),
        Column('MA30_Close_V', Float),
        Column('Volume_MA7_M', Float),
        Column('Volume_MA7_V', Float),
        Column('Volume_Ratio_MV', Float),
        Column('Day_of_Week', Integer),
        Column('Month', Integer),
        Column('Year', Integer),
    )
    pg_hook = PostgresHook(postgres_conn_id='postgres_local')
    engine = pg_hook.get_sqlalchemy_engine()
    with engine.begin() as connection:
        metadata.create_all(connection, checkfirst=True)

# Define the transformation function with data cleaning and feature engineering
def transform_data():
    csv_file_path = '/opt/airflow/dags/data/MVR.csv'
    data = pd.read_csv(csv_file_path)
    data.columns = ['Date', 'Open_M', 'High_M', 'Low_M', 'Close_M', 'Adj_Close_M',
                    'Volume_M', 'Open_V', 'High_V', 'Low_V', 'Close_V', 'Adj_Close_V',
                    'Volume_V']
    
    # Data Cleaning
    data.drop_duplicates(inplace=True)
    data.dropna(inplace=True)  # Drop rows with any missing values
    data['Date'] = pd.to_datetime(data['Date'], errors='coerce')  # Convert date column
    data.dropna(subset=['Date'], inplace=True)  # Drop rows where Date couldn't be converted

    # Remove outliers based on business knowledge (e.g., filter unrealistic volumes)
    data = data[data['Volume_M'] > 0]
    data = data[data['Volume_V'] > 0]

    # Feature Engineering
    data['Price_Change_M'] = data['Close_M'] - data['Open_M']
    data['Price_Change_V'] = data['Close_V'] - data['Open_V']
    data['Pct_Change_M'] = (data['Close_M'] - data['Open_M']) / data['Open_M'] * 100
    data['Pct_Change_V'] = (data['Close_V'] - data['Open_V']) / data['Open_V'] * 100
    data['Volatility_M'] = data['High_M'] - data['Low_M']
    data['Volatility_V'] = data['High_V'] - data['Low_V']
    data['MA7_Close_M'] = data['Close_M'].rolling(window=7).mean()
    data['MA7_Close_V'] = data['Close_V'].rolling(window=7).mean()
    data['MA30_Close_M'] = data['Close_M'].rolling(window=30).mean()
    data['MA30_Close_V'] = data['Close_V'].rolling(window=30).mean()
    data['Volume_MA7_M'] = data['Volume_M'].rolling(window=7).mean()
    data['Volume_MA7_V'] = data['Volume_V'].rolling(window=7).mean()
    data['Volume_Ratio_MV'] = data['Volume_M'] / data['Volume_V']
    
    # Extract additional features from Date column for seasonal analysis
    data['Day_of_Week'] = data['Date'].dt.day_name() 
    data['Month'] = data['Date'].dt.month
    data['Year'] = data['Date'].dt.year
    
    # Save the transformed data to a temporary file
    data.to_csv('/opt/airflow/dags/data/MVR_transformed.csv', index=False)

# Define the function to load transformed data into the database
def load_data():
    csv_file_path = '/opt/airflow/dags/data/MVR_transformed.csv'
    data = pd.read_csv(csv_file_path)
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_local')
    engine = pg_hook.get_sqlalchemy_engine()
    with engine.begin() as connection:
        data.to_sql('MVR', connection, if_exists='replace', index=False)

# Define the Python operator tasks with callbacks
create_table_task = PythonOperator(
    task_id='create_table_task',
    python_callable=create_table,
    on_failure_callback=task_failure_alert,  
    on_success_callback=task_success_alert,  
    on_retry_callback=task_retry_alert, 
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data_task',
    python_callable=transform_data,
    on_failure_callback=task_failure_alert,  
    on_success_callback=task_success_alert, 
    on_retry_callback=task_retry_alert,  
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data_task',
    python_callable=load_data,
    on_failure_callback=task_failure_alert,  
    on_success_callback=task_success_alert, 
    on_retry_callback=task_retry_alert,  
    dag=dag,
)

# Set task dependencies
create_table_task >> transform_data_task >> load_data_task
