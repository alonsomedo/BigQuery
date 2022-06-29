from google.cloud import pubsub_v1
import time


publisher = pubsub_v1.PublisherClient()

topic_name = 'projects/bigquery-demo-354214/topics/data_stream_from_file' # [project]/[project-name]/[topics]/[topic-name]

try:
    publisher.create_topic(topic_name)
except:
    print('Topic already exists')

with open('food_orders.csv') as file:
    for row in file:
        data = row
        future = publisher.publish(topic_name, data=data.encode("utf-8"))
        print(future.result())
        time.sleep(1)