from ensurepip import bootstrap
import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers = ["localhost:9092"])

TAXI_TRIPS_TOPIC = "taxi-trips"
with open("../data/trips/yellow_tripdata_2021-01.csv", "r") as f:
    next(f)
    for row in f:
        producer.send(TAXI_TRIPS_TOPIC, row.encode("utf-8")) # 택시 토픽 데이터를 인코딩 하여 보냄
        print(row)
        time.sleep(1)
        
producer.flush() # 프로듀서를 깨끗히 지워줌