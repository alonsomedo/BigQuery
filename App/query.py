from google.cloud import bigquery
import os

SERVICE_ACCOUNT_JSON = os.environ['GCP_SERV_ACC']

client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_JSON)

query_to_run = "select * from bigquery-demo-354214.dataset_py.table_py"

query_job = client.query(query_to_run)

print(query_job)
print("Script run")

for row in query_job:
    print(str(row[0]) + "-" + str(row[1]) + "-" + str(row[2]))
