from confluent_kafka import Consumer, SerializingProducer, KafkaError

conf = {
    'bootstrap.servers': 'localhost:9092',
}
consumer = Consumer(conf |
                    {'group.id': 'flight-group',
                     'auto.offset.reset': 'earliest',
                     'enable.auto.commit': False}
                    )
producer = SerializingProducer(conf)

if __name__ == "__main__":
    consumer.subscribe(['flight'])
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                print(msg.value())
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.close()