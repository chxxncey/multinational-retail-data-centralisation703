import pandas as pd
import requests
import boto3

class DataExtractor:
    def __init__(self):
        self.headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}

    def list_number_of_stores(self, number_stores_endpoint):
        response = requests.get(number_stores_endpoint, headers=self.headers)
        return response.json()['number_of_stores']

    def retrieve_stores_data(self, store_details_endpoint):
        num_stores = self.list_number_of_stores("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores")
        all_stores_data = []
        for store_number in range(1, num_stores + 1):
            response = requests.get(f"{store_details_endpoint}/{store_number}", headers=self.headers)
            all_stores_data.append(response.json())

        return pd.DataFrame(all_stores_data)

    def extract_from_s3(self, s3_url):
        bucket_name, key = s3_url.replace("s3://", "").split("/", 1)
        s3_client = boto3.client('s3')
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        return pd.read_csv(obj['Body'])

    def read_rds_table(self, db_connector, table_name):
        return pd.read_sql_table(table_name, db_connector.engine)
