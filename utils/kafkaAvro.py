import time
import logging

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField


class KafkaAVRO:
    def __init__(
        self,
        confluent_config: dict,
        log_handler: logging.Logger = logging.getLogger(),
    ) -> None:
        self.kafka_config = confluent_config["kafka-cluster"]
        self.sr_config = confluent_config["schema-registry"]
        self.basket_topic = confluent_config["data"]["basket"]["topic"]
        self.basket_schema = open(
            confluent_config["data"]["basket"]["schema"],
            "r",
        ).read()
        self.checkin_topic = confluent_config["data"]["checkin"]["topic"]
        self.checkin_schema = open(
            confluent_config["data"]["checkin"]["schema"],
            "r",
        ).read()
        self.checkout_topic = confluent_config["data"]["checkout"]["topic"]
        self.checkout_schema = open(
            confluent_config["data"]["checkout"]["schema"],
            "r",
        ).read()
        self.log_handler = log_handler
        self._next_producer_poll = 0
        self._set_schema_registry()
        self._set_kafka_producer()

    def _error_cb(
        self,
        err,
    ) -> None:
        logging.error(f"error_cb: {err}")

    def _set_schema_registry(self) -> None:
        # Schema Registry client
        schema_registry_client = SchemaRegistryClient(self.sr_config)
        # Serialisers
        self.avro_serializer = {
            self.basket_topic: AvroSerializer(
                schema_registry_client,
                self.basket_schema,
            ),
            self.checkin_topic: AvroSerializer(
                schema_registry_client,
                self.checkin_schema,
            ),
            self.checkout_topic: AvroSerializer(
                schema_registry_client,
                self.checkout_schema,
            ),
        }

    def _set_kafka_producer(self) -> None:
        # Kafka Producer client
        self.kafka_producer = Producer(
            self.kafka_config,
            error_cb=self._error_cb,
            logger=self.log_handler,
        )

    def produce_message(
        self,
        topic: str,
        key: str,
        payload: dict,
    ) -> None:
        try:
            if self._next_producer_poll < time.time():
                self.kafka_producer.poll(0)
                self._next_producer_poll = time.time() + 10
            self.kafka_producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=self.avro_serializer[topic](
                    payload,
                    SerializationContext(
                        topic,
                        MessageField.VALUE,
                    ),
                ),
            )
        except Exception as err:
            logging.error(
                f"Invalid input ({err}), discarding record: {topic}, {payload}"
            )
