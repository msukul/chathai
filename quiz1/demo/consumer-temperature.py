from confluent_kafka import Consumer

c = Consumer({
    'bootstrap.servers': 'localhost:9092,localhost:9192,localhost:9292',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['iot-temperature'])

while True:
    msg = c.poll(0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    value = msg.value()

    if value is None:
        value = -1
    else:
        value = msg.value().decode('utf-8')
        
    kvalue = msg.key().decode('utf-8')
    print('Received message: {0} , {1}'.format(kvalue, value))
    
c.close()

