import json
import time
import random
import numpy as np
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

class SyntheticDataProducer:
    """
    Synthetic Kafka producer for complex, dirty, and misaligned
    meteorological and air quality data.
    """

    def __init__(
        self,
        kafka_broker: str,
        weather_topic: str,
        aqi_topic: str,
        cities: list[str],
        producer_id: str = "data_engine_v3_pro",
        sleep_seconds: int = 3
    ):
        self.kafka_broker = kafka_broker
        self.weather_topic = weather_topic
        self.aqi_topic = aqi_topic
        self.cities = cities
        self.producer_id = producer_id
        self.sleep_seconds = sleep_seconds
        self.producer = None

    def initialize_kafka_topics(self) -> None:
        """Creates Kafka topics with 3 partitions for scalability."""
        admin = KafkaAdminClient(bootstrap_servers=self.kafka_broker)
        topics = [
            NewTopic(self.weather_topic, num_partitions=3, replication_factor=1),
            NewTopic(self.aqi_topic, num_partitions=3, replication_factor=1)
        ]

        for topic in topics:
            try:
                admin.create_topics([topic])
                print(f"[INFO] Topic created: {topic.name}")
            except TopicAlreadyExistsError:
                print(f"[INFO] Topic exists: {topic.name}")
            except Exception as exc:
                print(f"[ERROR] Topic creation failed: {topic.name} | {exc}")

    def _initialize_producer(self) -> None:
        """Initializes KafkaProducer with a safe key_serializer for None values."""
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_broker,
            # Handle None keys to avoid .encode() errors during null-dimension anomalies
            key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=50
        )

    def _generate_weather_event(self) -> tuple[str, dict]:
        """Generates a weather event with potential quality issues and anomalies."""
        city = random.choice(self.cities)
        anomaly_chance = random.random()

        event = {
            "record_id": random.randint(100_000, 999_999),
            "city": city,
            "temperature_c": round(np.random.normal(22, 8), 2),
            "humidity_pct": random.randint(15, 95),
            "timestamp": datetime.utcnow().isoformat(),
            "source_system": "weather_api_v1",
            "producer_id": self.producer_id,
            "data_quality_flag": "VALID",
            "scd_type": "TYPE_1"
        }

        # Inject intentional data quality anomalies
        if anomaly_chance < 0.08:
            event["temperature_c"] = random.choice([-273, 999]) # Physical outliers
            event["data_quality_flag"] = "OUTLIER"
        elif anomaly_chance < 0.12:
            event["city"] = None # Null dimension challenge
            event["data_quality_flag"] = "NULL_DIMENSION"
        elif anomaly_chance < 0.16:
            event["timestamp"] = "INVALID_TIMESTAMP" # Parsing format challenge
            event["data_quality_flag"] = "INVALID_FORMAT"
        elif anomaly_chance < 0.20:
            event["humidity_pct"] = "UNKNOWN" # Schema drift/Type mismatch challenge
            event["data_quality_flag"] = "SCHEMA_DRIFT"

        # Simulate SCD (Slowly Changing Dimension) Type 4 scenarios
        if random.random() < 0.05:
            event["scd_type"] = "TYPE_4"
            event["is_current"] = False

        return event["city"], event

    def _generate_aqi_event(self) -> tuple[str, dict]:
        """Generates an AQI event with realistic timestamp misalignment."""
        city = random.choice(self.cities)
        pm25 = np.random.lognormal(mean=3.2, sigma=0.7)

        event = {
            "record_id": random.randint(100_000, 999_999),
            "city": city,
            "pm25": round(pm25, 2),
            "aqi_index": int(pm25 * 1.9),
            # Timestamp Misalignment: AQI data is recorded +/- 7 mins vs Weather data
            "timestamp": (
                datetime.utcnow() + 
                timedelta(minutes=random.randint(-7, 7))
            ).isoformat(),
            "sensor_status": "OK",
            "source_system": "aqi_sensor_net",
            "producer_id": self.producer_id,
            "data_quality_flag": "VALID"
        }

        if random.random() < 0.10:
            event["sensor_status"] = "NEEDS_CALIBRATION"
            event["data_quality_flag"] = "DEGRADED_SENSOR"

        return city, event

    def run(self) -> None:
        """Starts the streaming loop."""
        if not self.producer:
            self._initialize_producer()

        print("[INFO] Streaming synthetic data (Ctrl+C to stop)")

        try:
            while True:
                # Produce Weather Data
                w_key, w_event = self._generate_weather_event()
                self.producer.send(self.weather_topic, key=w_key, value=w_event)

                # Produce AQI Data
                a_key, a_event = self._generate_aqi_event()
                self.producer.send(self.aqi_topic, key=a_key, value=a_event)

                # Occasional Duplicate for Deduplication testing (5% chance)
                if random.random() < 0.05:
                    self.producer.send(self.weather_topic, key=w_key, value=w_event)

                print(
                    f"[SENT] {datetime.now().strftime('%H:%M:%S')} - "
                    f"Weather({w_key}) | AQI({a_key}) | "
                    f"Quality: W={w_event['data_quality_flag']} A={a_event['data_quality_flag']}"
                )

                time.sleep(self.sleep_seconds)

        except KeyboardInterrupt:
            print("\n[INFO] Producer stopped by user.")
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        """Gracefully flushes and closes the Kafka producer."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            print("[INFO] Kafka producer closed.")

if __name__ == "__main__":
    # Initialize and run the producer
    producer_service = SyntheticDataProducer(
        kafka_broker="localhost:9092",
        weather_topic="weather_data_complex",
        aqi_topic="air_quality_complex",
        cities=["Tehran", "New York", "London", "Tokyo", "Berlin"],
        sleep_seconds=3
    )

    producer_service.initialize_kafka_topics()
    producer_service.run()