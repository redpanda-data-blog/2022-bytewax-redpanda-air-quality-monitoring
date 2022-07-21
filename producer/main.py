import asyncio
import json
import os
from datetime import datetime

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from random import randint

BOOTSTRAP_SERVERS = (
    "redpanda:9092"
    if os.getenv("RUNTIME_ENVIRONMENT") == "DOCKER"
    else "localhost:9092"
)


async def generate_air_quality_data(sensor_name):
    # Create a Kafka producer to interact with Redpanda
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

    # Create a loop to send data to the Redpanda topic
    while True:
        # Generate a random integer between 0 and 100 as air quality value
        # format timestamp to milliseconds
        data = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "value": randint(0, 100),
        }

        # Send the data to the Redpanda topic
        producer.send(
            "air-quality",
            key=sensor_name.encode("utf-8"),
            value=json.dumps(data).encode("utf-8"),
        )
        print(f"Sent data to Redpanda: {data}, sleeping for 3 seconds")
        await asyncio.sleep(5)


async def main():
    sensors = ["Sensor1", "Sensor2", "Sensor3", "Sensor4", "Sensor5"]
    # Create and run an async task for every sensor
    tasks = [generate_air_quality_data(sensor) for sensor in sensors]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    # Create kafka topics if running in Docker.
    if os.getenv("RUNTIME_ENVIRONMENT") == "DOCKER":
        admin_client = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVERS, client_id="air-quality-producer"
        )
        topics = ["air-quality", "air-quality-anomalies"]
        # Check if topics already exist first
        existing_topics = admin_client.list_topics()
        for topic in topics:
            if topic not in existing_topics:
                admin_client.create_topics(
                    [NewTopic(topic, num_partitions=1, replication_factor=1)]
                )
    asyncio.run(main())
