import json
import time

from faker import Faker
from datetime import datetime
from confluent_kafka import SerializingProducer
from confluent_kafka import deserializing_consumer

fake = Faker()

def generate_profile():
    user = fake.simple_profile()

    return {
        'userid': fake.uuid4(),
        'username': user['username'],
        'name': user['name'],
        'sex': user['sex'],
        'mail': user['mail']
    }

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f"Message delivered to {msg.topic} [{msg.partition()}]")
def main():
    topic = "profile"
    producer = SerializingProducer({
        'bootstrap.servers':'localhost:9092'
    })

    # Time this line is execute
    current_time = datetime.now()

    while (datetime.now() - current_time).seconds < 90:
        try:
            p = generate_profile()
            print(p)

            producer.produce(
                topic=topic,
                key=p['username'],
                value=json.dumps(p),
                on_delivery=delivery_report

            )
            producer.poll(0)

            time.sleep(2)
        except BufferError:
            print("Buffer full! Waiting...")
            time.sleep(2)
        except Exception as e:
            print(e)

if __name__=="__main__":
    main()