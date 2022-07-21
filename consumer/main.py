import datetime
import json
import os

from bytewax import Dataflow, spawn_cluster, Emit, AdvanceTo, inputs
from kafka import KafkaConsumer, KafkaProducer
from river import anomaly

BOOTSTRAP_SERVERS = (
    "redpanda:9092"
    if os.getenv("RUNTIME_ENVIRONMENT") == "DOCKER"
    else "localhost:9092"
)


producer = KafkaProducer(
    value_serializer=lambda m: json.dumps(m).encode("utf-8"),
    bootstrap_servers=BOOTSTRAP_SERVERS,
)


class AnomalyDetector(anomaly.HalfSpaceTrees):
    def update(self, data):
        normalized_value = float(data["aiq_avg"][0]) / 100
        self.learn_one({"value": normalized_value})
        data["score"] = self.score_one({"value": normalized_value})
        return self, data


def get_records():
    consumer = KafkaConsumer(
        "air-quality",
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
    )
    for message in consumer:
        key = message.key.decode("utf-8")
        value = json.loads(message.value.decode("utf-8"))
        data = {
            "sensor_name": key,
            "timestamp": datetime.datetime.strptime(
                value["timestamp"], "%Y-%m-%d %H:%M:%S.%f"
            ),
            "value": value["value"],
        }
        yield data


def input_builder(worker_index, worker_count):
    epoch = 0
    # Ensure inputs are sorted by timestamp
    sorted_inputs = inputs.sorted_window(
        get_records(), datetime.timedelta(seconds=5), lambda x: x["timestamp"]
    )
    # All inputs within a tumbling window are part of the same epoch.
    tumbling_window = inputs.tumbling_epoch(
        sorted_inputs, datetime.timedelta(seconds=5), lambda x: x["timestamp"]
    )
    for message in tumbling_window:
        yield AdvanceTo(epoch)
        yield Emit(message)
        epoch += 1


def output_builder(worker_index, worker_count):
    def send_to_redpanda(previous_feed):
        if previous_feed[1][1]["anomaly"]:
            payload = previous_feed[1][1]
            payload["timestamp"] = payload["timestamp"][0].strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )
            key = previous_feed[1][1]["sensor_name"][0].encode("utf-8")
            print(f"Sending anomalous data to Redpanda: {payload} with key {key}")
            producer.send("air-quality-anomalies", key=key, value=json.dumps(payload))

    return send_to_redpanda


def group_by_sensor(data):
    return data[1]["sensor_name"], [data[1]]


def annotate_with_anomaly(data):
    data[1]["anomaly"] = data[1]["score"] > 0.7
    return data


def calculate_avg(air_quality_events):
    sensor_name, sensor_data = air_quality_events
    total_events = len(sensor_data)
    first_event = sensor_data[0]
    aiq_values = [event["value"] for event in sensor_data]
    aiq_avg = sum(aiq_values) / total_events

    return (
        sensor_name,
        {
            "sensor_name": [sensor_name],
            "timestamp": [first_event["timestamp"]],
            "aiq_avg": [aiq_avg],
        },
    )


# Create a dataflow
flow = Dataflow()
# Group by sensor name
flow.map(group_by_sensor)
# Calculate the rolling average of Air Quality values
flow.map(calculate_avg)
# Calculate anomaly score in tumbling window of 5 seconds
flow.stateful_map(
    builder=lambda key: AnomalyDetector(n_trees=5, height=3, window_size=5, seed=42),
    mapper=AnomalyDetector.update,
)
# Annotate with anomaly
flow.map(annotate_with_anomaly)
# Send to anomaly Redpanda
flow.capture()

if __name__ == "__main__":
    spawn_cluster(flow, input_builder, output_builder, worker_count_per_proc=1)
