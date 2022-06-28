from google.cloud import bigquery
import os

SERVICE_ACCOUNT_JSON = os.environ['GCP_SERV_ACC']

client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

dataset_id = 'bigquery-demo-354214.dataset_py'

dataset = bigquery.Dataset(dataset_id)
dataset.location = "US"
dataset.description = "Dataset created from Python"

dataset_ref = client.create_dataset(dataset, timeout = 30)

print(f"Created Dataset {client.project}.{dataset_ref.dataset_id} ")


