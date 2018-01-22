#!/home/cloudera/anaconda3/bin/python

import pandas as pd
from kafka import KafkaProducer
import shlex, subprocess
import time


subprocess.call(shlex.split('kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic taxiEventsFlow'))

dfTrips=pd.read_csv('/media/sf_agu/TFM/dataset/tripdata_2017-06.csv')

producer = KafkaProducer(bootstrap_servers='localhost:9092')

def tripToKafka(trip):
	tripString=trip.astype(str).str.cat(sep=',')
	future = producer.send('taxiEventsFlow',tripString.encode('utf-8'))
	result = future.get(timeout=1)
	time.sleep(.05)

dfTrips.apply(tripToKafka,axis=1)