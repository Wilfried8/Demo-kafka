from confluent_kafka import DeserializingConsumer
from confluent_kafka import Consumer, KafkaError

def consume_messages():
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest'
    }

    consumer = DeserializingConsumer(conf)
    topics = ['profile']  # Replace with your Kafka topic

    consumer.subscribe(topics)

    try:
        while True:
            msg = consumer.poll(timeout=1000)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event - not an error
                    continue
                else:
                    print(msg.error())
                    break

            print(f"Received message: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()

if __name__ == "__main__":
    consume_messages()
