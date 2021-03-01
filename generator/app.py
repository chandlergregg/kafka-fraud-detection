import os
import json
from kafka import KafkaProducer
from time import sleep
from transactions import create_random_transaction

# Set vars for URL, topic, transaction count
KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
TRANSACTIONS_TOPIC = os.environ.get("TRANSACTIONS_TOPIC")
TRANSACTIONS_PER_SECOND = float(os.environ.get("TRANSACTIONS_PER_SECOND"))
SLEEP_TIME = 1 / TRANSACTIONS_PER_SECOND

def main():

    # Produces transactions to transactions topic
    producer = KafkaProducer(
        bootstrap_servers = KAFKA_BROKER_URL,
        value_serializer = lambda value: json.dumps(value).encode(),
    )
    
    # Create and send transactions, then sleep given interval
    while True:
        transaction: dict = create_random_transaction()
        producer.send(TRANSACTIONS_TOPIC, value = transaction)
        print(transaction)
        sleep(SLEEP_TIME)

if __name__ == "__main__":
    main()