""" END TO END DATA PROCESSING PIPELINE FOR BIGQUERY USING APACHE-BEAM
 
Apache Beam is a unified API to process batch as well as streaming data.
 
Beam pipeline once created in any language can be run on any of the execution frameworks like Spark, Flink, Apex, Cloud dataflow etc.
 
Further, Beam pipelines can be created in any language, including Python.
 
while using Beam, we just need to focus on our use case logic and data without considering any runtime details. In Beam, there is a clear separation between runtime layer and programming layer.
"""

from venv import create
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import argparse 
from google.cloud import bigquery
from apache_beam.runners.runner import PipelineState
from threading import Timer

parser = argparse.ArgumentParser()

parser.add_argument('--input',
                        dest='input',
                        required=True,
                        help='Input file to process.')

path_args, pipeline_args = parser.parse_known_args()

inputs_pattern = path_args.input

# https://cloud.google.com/dataflow/docs/guides/setting-pipeline-options?hl=es-419#python
options = PipelineOptions(pipeline_args)

options.view_as(StandardOptions).streaming = True

p = beam.Pipeline(options = options)

def remove_last_colon(row):		# OXJY167254JK,11-09-2020,8:11:21,854A854,Chow M?ein:,65,Cash,Sadabahar,Delivered,5,Awesome experience
    row = row.decode()
    cols = row.split(',')		# [(OXJY167254JK) (11-11-2020) (8:11:21) (854A854) (Chow M?ein:) (65) (Cash) ....]
    item = str(cols[4])			# item = Chow M?ein:
    
    if item.endswith(':'):
        cols[4] = item[:-1]		# cols[4] = Chow M?ein

    return ','.join(cols)		# OXJY167254JK,11-11-2020,8:11:21,854A854,Chow M?ein,65,Cash,Sadabahar,Delivered,5,Awesome experience
	

def remove_special_characters(row):    # oxjy167254jk,11-11-2020,8:11:21,854a854,chow m?ein,65,cash,sadabahar,delivered,5,awesome experience
    import re
    cols = row.split(',')			# [(oxjy167254jk) (11-11-2020) (8:11:21) (854a854) (chow m?ein) (65) (cash) ....]
    ret = ''
    for col in cols:
        clean_col = re.sub(r'[?%&]','', col)
        ret = ret + clean_col + ','			# oxjy167254jk,11-11-2020,8:11:21,854a854,chow mein:,65,cash,sadabahar,delivered,5,awesome experience,
    ret = ret[:-1]						# oxjy167254jk,11-11-2020,8:11:21,854A854,chow mein:,65,cash,sadabahar,delivered,5,awesome experience
    return ret


def print_row(row):
    print(row)

def to_json(csv_str):
    fields = csv_str.split(',')
    json_str = {
        "customer_id": fields[0],
        "date": fields[1],
        "timestamp": fields[2],
        "order_id": fields[3],
        "items": fields[4],
        "amount": fields[5],
        "mode": fields[6],
        "restaurant": fields[7],
        "status": fields[8],
        "ratings": fields[9],
        "feedback": fields[10],
        "new_col": fields[11]
    }

    return json_str


# Pcollection is a unified storage entity of Beam that can store any batch or streaming data.
# Same like we have dataset and RDDs in Spark, we have Pcollections in beam.

cleaned_data = (
    p
    | beam.io.ReadFromPubSub(topic=inputs_pattern)
    | beam.Map(remove_last_colon)
    | beam.Map(lambda row: row.lower()) # take each row from previous stage and apply lower case
    | beam.Map(remove_special_characters)
    | beam.Map(lambda row: row+',1')
)

delivered_orders = (
    cleaned_data
    | 'delivered filter' >> beam.Filter(lambda row: row.split(',')[8] == 'delivered')  # label to the transform, label has to be different for Ptransforms

)

other_orders = (
    cleaned_data
    | 'Undelivered filter' >> beam.Filter(lambda row: row.split(',')[8] != 'delivered')  # label to the transform, label has to be different for Ptransforms
)

# (cleaned_data
#  | 'count total' >> beam.combiners.Count.Globally() # 920
#  | 'total map' >>  beam.Map(lambda x: 'Total Count:' +str(x)) # Total Count: 920
#  | 'print total' >> beam.Map(print_row)
# )


client = bigquery.Client() # Account key is not necessary because we are going to run the code from cloudshell

dataset_id = f'{client.project}.dataset_food_orders_latest'
try:
    client.get_dataset(dataset_id)

except:
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset.description = "Dataset created from Python"
    dataset_ref = client.create_dataset(dataset, timeout = 30) # Make an API request.


"""Table Creation
 
Beam has a library to create and load data in BigQuery tables.
 
We will be using apache_beam.io.WriteToBigQuery(
	table_name,
	schema,
	create_disposition,
	write_disposition,
	additional_bq_parameters
)
 
Takes input as .json but we have p-collections in .csv format; so we'll first convert to .json
"""

delivered_table_spec = 'bigquery-demo-354214:dataset_food_orders_latest.delivered_orders'
other_table_spec = 'bigquery-demo-354214:dataset_food_orders_latest.other_orders'

table_schema = 'customer_id:STRING,date:STRING,timestamp:STRING,order_id:STRING,items:STRING,amount:STRING,mode:STRING,restaurant:STRING,status:STRING,ratings:STRING,feedback:STRING,new_col:STRING'


(delivered_orders
    | 'delivered to json' >> beam.Map(to_json)
    | 'Write delivered' >> beam.io.WriteToBigQuery( 
        delivered_table_spec,
        schema=table_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        additional_bq_parameters={'timePartitioning': {'type': 'DAY'}}
        )
)

(other_orders
    | 'other to json' >> beam.Map(to_json)
    | 'Write other' >> beam.io.WriteToBigQuery( # to write in bigquery , data must be in json format
        other_table_spec,
        schema=table_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        additional_bq_parameters={'timePartitioning': {'type': 'DAY'}}
        )
)

# Creating Daily View for delivery orders
def create_view():
    view_name = 'daily_food_orders'
    dataset_ref = client.dataset('dataset_food_orders')
    view_ref = dataset_ref.table(view_name)
    view_to_create = bigquery.Table(view_ref)

    view_to_create.view_query = """
                                    select
                                        *
                                    from `bigquery-demo-354214.dataset_food_orders.delivered_orders`
                                    where _PARTITIONDATE = DATE(current_date())
                                """
    view_to_create.view_use_legacy_sql = False

    try:
        client.create_table(view_to_create)
    except:
        print('View already exists')

t = Timer(5.0, create_view)
t.start()

ret = p.run()
ret.wait_until_finish()
if ret.state == PipelineState.DONE:
    print("Success!!")
else:
    print("Error running beam pipeline")

