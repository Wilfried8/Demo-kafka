from confluent_kafka import DeserializingConsumer
from confluent_kafka import Consumer, KafkaError
import json
import psycopg2


def consume_messages():
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest'
    }
    postgres_conf = {
        'database': 'footballdb',
        'user': 'postgres',
        'password': 'admin',
        'host': 'localhost',
        'port': '5432'
    }

    consumer = DeserializingConsumer(conf)
    topics = ['profile']  # Replace with your Kafka topic

    consumer.subscribe(topics)

    try:
        # conn = psycopg2.connect(postgres_conf)
        print("coneection;;;;;;")
        conn = psycopg2.connect(
            dbname=postgres_conf['database'],
            user=postgres_conf['user'],
            password=postgres_conf['password'],
            host=postgres_conf['host'],
            port=postgres_conf['port']
        )
        cursor = conn.cursor()
        print("coneection end;;;;;;")

        cursor.execute("select * from equipes")
        records = cursor.fetchall()

        print(records)
        while True:
            msg = consumer.poll(timeout=10)

            if msg is None:
                print("messge none")
                continue
            if msg.error():
                print("messge error")
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event - not an error
                    continue
                else:
                    print(msg.error())
                    break


            print(f"Received message: {msg.value().decode('utf-8')}")

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
    consume_messages()
