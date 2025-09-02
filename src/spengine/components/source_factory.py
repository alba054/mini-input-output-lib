from spengine.app.beanstalkd import BeanstalkConsumer
from spengine.app.kafka import Consumer
from spengine.data_source.beanstalkd_subject import BeanstalkdSubject
from spengine.data_source.data_source_subject import DataSourceSubject
from spengine.data_source.file_subject import FileSubject
from spengine.data_source.kafka_subject import KafkaSource
from spengine.data_source.xls_subject import XlsSubject
from spengine.data_source.rabbit_subject import RabbitSource
from spengine.processor.base_processor import BaseProcessor


class RabbitSourceFactory:
    @staticmethod
    def build(config: dict, processors: list[BaseProcessor]) -> RabbitSource:
        return RabbitSource(
            processors=processors,
            host=config["host"],
            port=config["port"],
            user=config["username"],
            password=config["password"],
            vhost=config["vhost"],
            exchange=config["exchange"],
            exchange_type=config["exchangeType"],
            prefetch_count=config["prefetchCount"],
            queue_name=config["queueName"],
            routing_key=config.get("routingKey"),
            durable=config.get("durable", True),
        )


class KafkaSourceFactory:
    @staticmethod
    def build(config: dict, processors: list[BaseProcessor]) -> KafkaSource:
        return KafkaSource(
            processors=processors,
            consumer=Consumer(
                servers=config["brokers"],
                auto_commit=config["commit"],
                group_id=config["groupId"],
                max_poll_records=config["poll"],
                offset_reset=config["offset"],
                source_topic=config["topic"],
            ),
        )


class FileSourceFactory:
    @staticmethod
    def build(config: dict, processors: list[BaseProcessor]) -> FileSubject:
        return FileSubject(
            config["filepath"],
            processors=processors,
        )


class XlsSourceFactory:
    @staticmethod
    def build(config: dict, processors: list[BaseProcessor]) -> XlsSubject:
        return XlsSubject(
            config["filepath"],
            processors=processors,
        )


class BeanstalkdFactory:
    @staticmethod
    def build(config: dict, processors: list[BaseProcessor]) -> BeanstalkdSubject:
        return BeanstalkdSubject(
            BeanstalkConsumer(config["host"], config["port"], config["topic"]),
            processors=processors,
        )


class SourceFactory:
    @staticmethod
    def build(config: dict, processors: list[BaseProcessor]) -> DataSourceSubject:
        if config["source"] == "kafka":
            return KafkaSourceFactory.build(config["metadata"], processors=processors)
        elif config["source"] == "file":
            return FileSourceFactory.build(config["metadata"], processors=processors)
        elif config["source"] == "xls":
            return XlsSourceFactory.build(config["metadata"], processors=processors)
        elif config["source"] == "beanstalk":
            return BeanstalkdFactory.build(config["metadata"], processors=processors)
        elif config["source"] == "rabbit":
            return RabbitSourceFactory.build(config["metadata"], processors=processors)
