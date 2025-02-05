from pipeline import GenericPipelineInterface
from datetime import datetime, timedelta, timezone
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import secretmanager
import pandas as pd
from pandas_gbq import to_gbq
import logging
import requests
import numpy as np
import os
import json

logging.basicConfig(filename='logs/ingestion.log',
                    filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')

def secret_variable(SECRET_NAME: str, VERSION: str):
    """Get secret variables."""
    
    try:
        # initialize secret manager client
        client = secretmanager.SecretManagerServiceClient()
        
        project_id = 'hearity-capstone'
        secret_name = f"projects/{project_id}/secrets/{SECRET_NAME}/versions/{VERSION}"
        
        # access secret
        response = client.access_secret_version(name=secret_name)
        variable = response.payload.data.decode("UTF-8")
        return variable
    except Exception as e:
        print(f"Error accessing secret: {e}")


class BigQueryDataPipeline(GenericPipelineInterface):
    
    def __init__(self, source_table: str, dest_table: str, dataset_id: str):
        self.source_table = source_table
        self.dest_table = dest_table
        self.project_id_src = "hearity-capstone"
        self.dataset_id = dataset_id
        
        service_account_key = secret_variable(SECRET_NAME='service_account', VERSION='1')
        # convert service account key string into credentials object
        credentials = service_account.Credentials.from_service_account_info(
            json.loads(service_account_key)
        )
        
        self.credentials = credentials

    def extract(self, source: str) -> pd.DataFrame:
        client = bigquery.Client(credentials=self.credentials)
        if source == 'source':
            source_table = self.source_table
        elif source == 'dest':
            source_table = self.dest_table
        else:
            print('Invalid input')

        data_source = f"{self.project_id_src}.{self.dataset_id}.{source_table}"
        query = f"SELECT * FROM `{data_source}`"
        query_job = client.query(query)
        df = query_job.result().to_dataframe()
        logging.info('Extraction completed successfully.')
        return df
    
    def _get_users(self) -> pd.DataFrame:
        """Retrieve the currently users based on JWT information stored in GBQ."""
        
        client = bigquery.Client(credentials=self.credentials)
        table = 'users'
        data_source = f"{self.project_id_src}.{self.dataset_id}.{table}"
        query = f"SELECT user_id FROM `{data_source}`"
        query_job = client.query(query)
        df = query_job.result().to_dataframe()
        return df


    def transform(self, extracted_result: pd.DataFrame) -> pd.DataFrame:

       user_id = self._get_users() 
       df = extracted_result.copy()
          
       # filter df exclude patient with one data
       df = df.groupby('user_id').filter(lambda x: len(x) > 1)
          
       if 'user_id' in df.columns:
           df['user_id'] = df['user_id'].apply(lambda x: str(x))
           df[df['user_id'].isin(user_id['user_id'].tolist())]
           df.drop(columns=['id'], inplace=True)          
           new_order = [
                    'user_id', 
                    'date',
                    'left_freq_500_hz', 'left_freq_1000_hz', 
                    'left_freq_2000_hz','left_freq_4000_hz', 'right_freq_500_hz',
                    'right_freq_1000_hz','right_freq_2000_hz','right_freq_4000_hz'
                    ]
           df = df.reindex(columns=new_order)
           df['date'] = pd.to_datetime(df['date'])
           logging.info(f'Transformation completed successfully.')
           return df
       else:
           logging.warning(f'Column "user_id" not found in data.')
     
    def load_to_gbq(self, forecasting_results: pd.DataFrame, project_id: str, dataset_id: str, table_id: str):
        """Load the transformed result DataFrame into BigQuery."""
        
        try:
            # initialize the BigQuery client
            client = bigquery.Client(project=project_id)

            if not forecasting_results.empty:
                data_to_upload = []

                frequencies = ['500', '1000', '2000', '4000']
                sides = ['left', 'right']

                # get the current timestamp for ID generation
                timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')

                for _, row in forecasting_results.iterrows():
                    right_freq_values = []
                    left_freq_values = []

                    for freq in frequencies:
                        for side in sides:
                            column_name = f"{side}_freq_{freq}_hz"
                            if side == 'left':
                                left_freq_values.append(row[column_name])
                            elif side == 'right':
                                right_freq_values.append(row[column_name])

                    # calculate the average of each frequency values using numpy
                    AD_value = np.mean(right_freq_values)
                    AS_value = np.mean(left_freq_values)

                    # generate the unique ID based on the timestamp and user_id
                    unique_id = f"{timestamp}-{row['user_id']}"

                    # prepare the data row to upload
                    data = {
                        'id': unique_id,  
                        'user_id': row['user_id'],
                        'date': datetime.now(timezone.utc).strftime('%Y-%m-%d'),
                        'left_freq_500_hz': row['left_freq_500_hz'],
                        'left_freq_1000_hz': row['left_freq_1000_hz'],
                        'left_freq_2000_hz': row['left_freq_2000_hz'],
                        'left_freq_4000_hz': row['left_freq_4000_hz'],
                        'right_freq_500_hz': row['right_freq_500_hz'],
                        'right_freq_1000_hz': row['right_freq_1000_hz'],
                        'right_freq_2000_hz': row['right_freq_2000_hz'],
                        'right_freq_4000_hz': row['right_freq_4000_hz'],
                        'AD': np.round(AD_value, 2),
                        'AS': np.round(AS_value, 2)
                    }
                    data_to_upload.append(data)

                
                df_to_upload = pd.DataFrame(data_to_upload)
                to_gbq(df_to_upload, destination_table=f'{dataset_id}.{table_id}', project_id=project_id, if_exists='append')

                logging.info("Forecasting results successfully loaded into BigQuery.")

        except Exception as e:
            logging.error(f"Error loading data into BigQuery: {e}")

           
           
class ForecastingImplementation(BigQueryDataPipeline):
    
    def __init__(self, source_table: str, dest_table: str, dataset_id: str):
        super().__init__(source_table, dest_table, dataset_id)


    def _upsampling(self, df: pd.DataFrame) -> pd.DataFrame:
        """Upsampling hearing test results from each patient."""
          
        upsampling_data = []
        for user_id, group in df.groupby('user_id'):
            group = group.set_index('date')
            daily_group = group.resample('D').interpolate(method='linear')
            daily_group = daily_group.round(2)
                    
            daily_group.reset_index(inplace=True)
            daily_group['user_id'] = user_id
            upsampling_data.append(daily_group)
          
        df = pd.concat(upsampling_data, ignore_index=True)
        return df


    def resampling_data(self, transformed_result: pd.DataFrame) -> pd.DataFrame:
        """Resampling test results for each patient."""

        df = transformed_result
        today = datetime.now(timezone.utc).date()
        df = df.sort_values(by=['user_id', 'date'])
          
        # get the last test per user
        last_test = df.groupby(['user_id']).tail(1)
        last_test['date'] = last_test['date'].dt.date
          
        if today in last_test['date'].values:
            logging.info(f'Using {self.source_table} as data source because of new audiometry data.')
               
            patients_today = last_test[last_test['date'] == today]['user_id']
            df = df[df['user_id'].isin(patients_today)]
            df_last_2_test = df.groupby('user_id').tail(2)
            df_last_2_test = df_last_2_test.sort_values(by=['user_id', 'date'])
            df_last_2_test['date'] = pd.to_datetime(df_last_2_test['date'])
            upsampling_data = self._upsampling(df_last_2_test)
               
            logging.info(f'Resampling data completed successfully.')
               
            return upsampling_data
          
        else:
            logging.info(f'Using combination of {self.source_table} & {self.dest_table} as data sources.')
            
            # forecasting table
            source2 = self.extract(source='dest') 
            df2 = self.transform(source2)
            df2['date'] = pd.to_datetime(df2['date'])
            test_date = df2['date'].dt.date.values
            
            if today in test_date:
                patients_today = df2.loc[df2['date'].dt.date == today, 'user_id']
                df_filtered = df2[~df2['user_id'].isin(patients_today)]
            else:
                df_filtered = df2
            
            # hearing test table               
            df_last_2_test = df.groupby('user_id').tail(2)
            
            # concate data sources
            merged_df = pd.concat([df_last_2_test, df_filtered], ignore_index=True, axis=0) 
            merged_df = merged_df.sort_values(by=['user_id', 'date'])
            merged_df['date'] = pd.to_datetime(merged_df['date'])
            upsampling_data = self._upsampling(merged_df)
               
            logging.info(f'Resampling data completed successfully.')
               
            return upsampling_data

    def _feature_extraction(self, patient_data: pd.DataFrame):
        """Extract features for ML modeling."""
        
        features = patient_data.iloc[:, 2:].values   
        return features
    
    def _call_prediction_api(self, features: np.ndarray):
        """Call Flask API to get the forecast predictions."""
        
        try:
            url = secret_variable(SECRET_NAME='API_MODEL', VERSION='1')
            predict_url = str(url) + "/predict"
            payload = {"features": features.tolist()}
            response = requests.post(predict_url, json=payload)
            
            if response.status_code == 200:
                logging.info('Call Flask API completed successfully.')
                return np.round(response.json()['predictions'], 2)
            else:
                logging.warning(f'API request failed with status code {response.text}')

        except requests.exceptions.RequestException as e:
            logging.error(f'Request error occurred: {e}')
            
    
    def generate_forecast(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate new forecasting for each patient."""
        
        time_steps = 30  
        forecasting_results = []
        
        feature_names = [
            'left_freq_500_hz', 'left_freq_1000_hz', 'left_freq_2000_hz', 'left_freq_4000_hz',
            'right_freq_500_hz', 'right_freq_1000_hz', 'right_freq_2000_hz', 'right_freq_4000_hz'
        ]
        
        unique_users = df['user_id'].unique()

        for user_id in unique_users:

            user_data = df[df['user_id'] == user_id]
            features = self._feature_extraction(user_data)
            input_features = features[-time_steps:].reshape(1, time_steps, features.shape[1])
            patient_forecast = self._call_prediction_api(input_features)
            
            if patient_forecast is not None:
                forecast_dict = {'user_id': user_id}
            
                for idx, feature_name in enumerate(feature_names):
                    forecast_dict[feature_name] = patient_forecast[0][idx] 
            
                forecasting_results.append(forecast_dict)
            else:
                logging.warning(f"Forecasting failed for user {user_id}")
        
        forecasting_df = pd.DataFrame(forecasting_results)
        forecasting_df = forecasting_df.sort_values(by='user_id')
        logging.info('Generate forecasting completed successfully.')
        return forecasting_df
     

if __name__ == '__main__':
    """For testing purpose."""
    
    pipeline = BigQueryDataPipeline(
    source_table= "tests",
    dest_table= "forecasting",
    dataset_id = "trial_pipeline")

    extracted_test = pipeline.extract(source='source')
    print(f'EXTRACTED DATA: \n{extracted_test}')
    print()

    extracted_forecasting = pipeline.extract(source='dest')
    print(f'FORECASTING DATA: \n{extracted_forecasting}')
    print()

    users = pipeline._get_users()
    print(f'USERS: \n{users}')
    print()

    transformed_data = pipeline.transform(extracted_test)
    print(f'TRANSFORMED DATA: \n{transformed_data}')
    print()

    forecast = ForecastingImplementation(
        source_table= "tests",
        dest_table= "forecasting",
        dataset_id = "trial_pipeline")
    
    resampling = forecast.resampling_data(transformed_data)
    print(f'RESAMPLING DATA: \n{resampling}')
    print()
    
    forecasting = forecast.generate_forecast(resampling)
    print(f'FORECASTING DATA: \n {forecast}')
    print()
    
    load = pipeline.load_to_gbq(forecasting, 'hearity-capstone', 'trial_pipeline', 'forecasting')
    print('LOAD TO GBQ')