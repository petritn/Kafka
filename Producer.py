from time import sleep
import pandas as pd
import csv
from json import dumps
from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

# Read the csv file and convert it into a dictionary
csvFilePath = "/users/Petrit/Desktop/Sample_LSR.csv"

data = {}

with open(csvFilePath) as csvFile:
    csvReader = csv.DictReader(csvFile)
    for csvRow in csvReader:
        imsi_ = csvRow["IMSI"]
        data[imsi_] = csvRow
        producer.send('Location-Data-Feed', value=data[imsi_])
        sleep(1)

# Convert LSR csv file to a JSON file format
lsr_csv = pd.DataFrame(pd.read_csv("/users/Petrit/Desktop/Sample_LSR.csv"))
lsr_csv.to_json("/users/Petrit/Desktop/LSR_JSON", orient="records")

'''
lsr_ = pd.read_csv("/users/Petrit/Desktop/Sample_LSR.csv")
r, c = lsr_.shape

# Alternative of exporting a single column at a time...
for i in range(r):
    row_of_data: list = lsr_.iloc[i, 1]
    producer.send('Location-Data-Feed', value=row_of_data)
    sleep(5)
'''

# End

