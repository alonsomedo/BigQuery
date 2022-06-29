from google.cloud import pubsub_v1
import time


publisher = pubsub_v1.PublisherClient()

project_id = 'bigquery-demo-354214'
topic_id = 'data_stream_from_file' 

topic_path = publisher.topic_path(project_id, topic_id) # [project]/[project-name]/[topics]/[topic-name]

try:
    topic = publisher.create_topic(request={"name": topic_path})
    print(f"Created topic: {topic.name}")
except:
    print('Topic already exists')



with open('food_orders.csv') as file:
    for row in file:
        data = row.encode("utf-8")
        future = publisher.publish(topic_path, data=data)
        print(future.result())
        time.sleep(1)