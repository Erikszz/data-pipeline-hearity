from etl_forecasting_daily import BigQueryDataPipeline, ForecastingImplementation
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import logging


# set default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    'daily_forecasting_pipeline',
    default_args=default_args,
    description='A pipeline to extract data from Big Query, generate forecasting, and load it into Big Query again.',
    schedule_interval='* 17 * * *',
    start_date=datetime(2024, 12, 11),
    catchup=False,
)

# initialize and configure logging
logging.basicConfig(filename='logs/ingestion.log',
                    filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

# initialize the pipeline 
pipeline = BigQueryDataPipeline(
    source_table= "tests",
    dest_table= "forecasting",
    dataset_id = "hearity"
)

forecasting = ForecastingImplementation(
    source_table= "tests",
    dest_table= "forecasting",
    dataset_id = "hearity"
)

# define the extract task
def extract_task(**kwargs):
    logging.info(f"Extracting the data from {pipeline.source_table}")
    df = pipeline.extract(source='source')
    kwargs['ti'].xcom_push(key='extracted_data', value=df)
    
# define the transform task
def transform_task(**kwargs):
    extracted_data = kwargs['ti'].xcom_pull(key='extracted_data')
    df = pd.DataFrame.from_dict(extracted_data)
    logging.info("Transforming")
    transformed_df = pipeline.transform(df)
    kwargs['ti'].xcom_push(key='transformed_data', value=transformed_df)

# define the resampling task
def resampling_data_task(**kwargs):
     transformed_data = kwargs['ti'].xcom_pull(key='transformed_data')
     df = pd.DataFrame.from_dict(transformed_data)
     logging.info('Resampling')
     resampling_df = forecasting.resampling_data(df)
     kwargs['ti'].xcom_push(key='resampling_data', value=resampling_df)
     
# define the forecasting task     
def generate_forecasting_task(**kwargs):
     resampling_data = kwargs['ti'].xcom_pull(key='resampling_data')
     df = pd.DataFrame.from_dict(resampling_data)
     logging.info('Generate Forecasting')
     forecasting_df = forecasting.generate_forecast(df)
     kwargs['ti'].xcom_push(key='forecasting_data', value=forecasting_df)
     
# define the load task
def load_task(**kwargs):
    forecasting_data = kwargs['ti'].xcom_pull(key='forecasting_data')
    forecasting_df = pd.DataFrame.from_dict(forecasting_data)
    logging.info(f"Load data to {pipeline.dest_table}")
    pipeline.load_to_gbq(forecasting_df, 'hearity-capstone', 'trial_pipeline', 'forecasting')

# define Airflow tasks
extract = PythonOperator(
    task_id='extract',
    python_callable=extract_task,
    provide_context=True,
    dag=dag,
)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform_task,
    provide_context=True,
    dag=dag,
)

resampling = PythonOperator(
    task_id='resampling',
    python_callable=resampling_data_task,
    provide_context=True,
    dag=dag,
)

generate_forecasting = PythonOperator(
    task_id='generate_forecasting',
    python_callable=generate_forecasting_task,
    provide_context=True,
    dag=dag,
)

load_to_gbq = PythonOperator(
    task_id='load_to_gbq',
    python_callable=load_task,
    provide_context=True,
    dag=dag,
)

# set the task dependencies
extract >> transform >> resampling >> generate_forecasting >> load_to_gbq