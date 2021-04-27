from kafka import KafkaProducer
import random

def exemplo_producer():
    # Create an instance of the Kafka producer
    producer = KafkaProducer(bootstrap_servers='localhost:29092', value_serializer=lambda v: str(v).encode('utf-8'))

    # Call the producer.send method with a producer-record
    print("Ctrl+c to Stop")
    count = 1
    while count < 1000:
        producer.send('kafka-python-topic', random.randint(1, 999))
        count+=1
