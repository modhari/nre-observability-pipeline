import json
import os

from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPICS = os.environ.get("KAFKA_TOPICS", "nre.incidents,nre.plans").split(",")

INFLUXDB_URL = os.environ["INFLUXDB_URL"]
INFLUXDB_TOKEN = os.environ["INFLUXDB_TOKEN"]
INFLUXDB_ORG = os.environ["INFLUXDB_ORG"]
INFLUXDB_BUCKET = os.environ["INFLUXDB_BUCKET"]


def to_bool_str(value):
    return "true" if bool(value) else "false"


def build_point(event):
    payload = event.get("payload", {})

    if event.get("event_type") == "incident_snapshot":
        return (
            Point("nre_incidents")
            .tag("fabric", event.get("fabric", "unknown"))
            .tag("device", event.get("device", "unknown"))
            .tag("root_cause", event.get("root_cause", "unknown"))
            .tag("approval_required", to_bool_str(payload.get("approval_required", False)))
            .field("safe_action_count", len(payload.get("safe_actions", [])))
            .field("gated_action_count", len(payload.get("gated_actions", [])))
            .field("suppressed_action_count", len(payload.get("suppressed_actions", [])))
            .field("execution_enabled", payload.get("execution_enabled", False))
            .field("payload_json", json.dumps(event))
            .time(event.get("ts"), WritePrecision.NS)
        )

    elif event.get("event_type") == "plan_snapshot":
        return (
            Point("nre_plans")
            .tag("fabric", event.get("fabric", "unknown"))
            .tag("device", event.get("device", "unknown"))
            .tag("root_cause", event.get("root_cause", "unknown"))
            .tag("approval_required", to_bool_str(payload.get("approval_required", False)))
            .field("safe_step_count", len(payload.get("safe_steps", [])))
            .field("gated_step_count", len(payload.get("gated_steps", [])))
            .field("skipped_action_count", len(payload.get("skipped_actions", [])))
            .field("execution_enabled", payload.get("execution_enabled", False))
            .field("payload_json", json.dumps(event))
            .time(event.get("ts"), WritePrecision.NS)
        )

    return None


def main():
    consumer = KafkaConsumer(
        *[t.strip() for t in KAFKA_TOPICS],
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="kafka_influx_writer",
    )

    influx = InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG,
    )

    write_api = influx.write_api(write_options=SYNCHRONOUS)

    print("writer started")

    for msg in consumer:
        event = msg.value

        point = build_point(event)
        if not point:
            continue

        write_api.write(
            bucket=INFLUXDB_BUCKET,
            org=INFLUXDB_ORG,
            record=point,
        )

        print(f"wrote {event.get('event_type')} {event.get('incident_id')}")


if __name__ == "__main__":
    main()
