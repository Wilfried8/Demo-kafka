from confluent_kafka import Consumer, KafkaError
from confluent_kafka import DeserializingConsumer
import json
import psycopg2

def consume_and_insert_messages():
    kafka_conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest'
    }

    postgres_conf = {
        'dbname': 'tayo',
        'user': 'postgres',
        'password': 'postgres',
        'host': 'postgres',
        'port': '5433'
    }

    consumer = DeserializingConsumer(kafka_conf)
    topics = ['profile']
    consumer.subscribe(topics)

    try:
        conn = psycopg2.connect(**postgres_conf)
        cursor = conn.cursor()

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

            data = json.loads(msg.value())
            insert_query = """
                INSERT INTO profile (userid, username, name, sex, mail)
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                data['userid'],
                data['username'],
                data['name'],
                data['sex'],
                data['mail']
            ))
            conn.commit()

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()
        cursor.close()
        conn.close()

if __name__ == "__main__":
    consume_and_insert_messages()
