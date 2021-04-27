from kafka import KafkaConsumer

def exemplo_consumer():
    consumer = KafkaConsumer('kafka-python-topic',
                             group_id='my-group',
                             bootstrap_servers=['localhost:29092'])
    print('começou...')
    print(consumer)
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                              message.offset, message.key,
                                              message.value))
