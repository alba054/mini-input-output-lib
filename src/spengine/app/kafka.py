from kafka import KafkaConsumer, KafkaProducer
import json


class Producer:
    producer: KafkaProducer
    topic: str

    def __init__(self, servers: list[str], topic: str):
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=servers)

    def produce(self, message: any):
        self.producer.send(self.topic, json.dumps(message).encode("utf-8"))


class KafkaBaseConsumer:
    consumer: KafkaConsumer

    def __init__(
        self,
        servers: list[str],
        source_topic: list[str],
        group_id: str,
        offset_reset: str,
        max_poll_records: int,
        auto_commit: bool = True,
    ):
        self.bootstrap_servers = servers
        self.auto_offset_reset = offset_reset
        self.group_id = group_id
        self.enable_auto_commit = auto_commit
        self.max_poll_records = max_poll_records
        self.topics = source_topic
        self.consumer = KafkaConsumer(
            bootstrap_servers=servers,
            auto_offset_reset=offset_reset,
            group_id=group_id,
            enable_auto_commit=auto_commit,
            max_poll_records=max_poll_records,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        self.consumer.subscribe(source_topic)

    def reconnect(self):
        self.consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            auto_offset_reset=self.auto_offset_reset,
            group_id=self.group_id,
            enable_auto_commit=self.enable_auto_commit,
            max_poll_records=self.max_poll_records,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        self.consumer.subscribe(self.topics)


class Consumer(KafkaBaseConsumer):
    def __iter__(self):
        while True:
            message_ = self.consumer.poll(60)
            for _, message_list in message_.items():
                # yield message_list
                for message in message_list:
                    yield message


# for batch kafka
class ConsumerBatch(KafkaBaseConsumer):
    def __iter__(self):
        while True:
            message_ = self.consumer.poll(60)
            for _, message_list in message_.items():
                # yield message_list
                yield message_list
