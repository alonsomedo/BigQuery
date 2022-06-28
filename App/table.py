from google.cloud import bigquery
import os

SERVICE_ACCOUNT_JSON = os.environ['GCP_SERV_ACC']

# Construct a BigQuery client object
client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

# Set table_id to the ID of the table to create
table_id = 'bigquery-demo-354214.dataset_py.table_py'

job_config = bigquery.LoadJobConfig(
    schema = [
        bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('gender', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('count', 'INTEGER', mode='NULLABLE')
    ],
    source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1
)

file_path = r'/home/alonsmd/Downloads/names/yob1880.txt'

source_file = open(file_path, "rb")

job = client.load_table_from_file(source_file, table_id, job_config=job_config)

job.result() # Waits for the job to complete

table = client.get_table(table_id)

print(f"Loaded {table.num_rows} rows in {table_id}. ")
