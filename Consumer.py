from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads

consumer = KafkaConsumer(
    'Location-Data-Feed',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8')))

# client = MongoClient('localhost:27017')
# collection = client.LocationDataFeed.LocationDataFeed


for message in consumer:
    message = message.value
    print(message)
    # collection.insert_one(message)
    # print('{} added to {}'.format(message, collection))

# End
