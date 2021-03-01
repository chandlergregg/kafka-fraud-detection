from kafka import KafkaConsumer, KafkaProducer
import os
import json

# Set vars for URL, topics
KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
TRANSACTIONS_TOPIC = os.environ.get("TRANSACTIONS_TOPIC")
LEGIT_TOPIC = os.environ.get("LEGIT_TOPIC")
FRAUD_TOPIC = os.environ.get("FRAUD_TOPIC")

def is_suspicious(transaction: dict) -> bool:
    """
    Returns true when amount from input dict is below 900, otherwise false
    Used to determine fraudulent transactions
    """
    return transaction["amount"] >= 900

def main():

    # Consume from generated transactions topic
    consumer = KafkaConsumer(
        TRANSACTIONS_TOPIC,
        bootstrap_servers = KAFKA_BROKER_URL,
        value_deserializer = json.loads,
    )

    # Producer to legit and fraud topics
    producer = KafkaProducer(
        bootstrap_servers = KAFKA_BROKER_URL,
        value_serializer = lambda value: json.dumps(value).encode(),
    )

    # Loop through messages and send to appropriate topic, print to console
    for message in consumer:       
        transaction: dict = message.value
        topic = FRAUD_TOPIC if is_suspicious(transaction) else LEGIT_TOPIC
        producer.send(topic, value = transaction)
        print(topic, transaction)

if __name__ == "__main__":
    main()
