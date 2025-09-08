import boto3
from spengine.app.beanstalkd import BeanstalkProducer
from spengine.app.kafka import Producer
from spengine.app.postgre import PgSaService
from spengine.app.s3 import S3
from spengine.app.rabbitmq_producer import RabbitPublisher
from spengine.target_output.rabbit_target_output import RabbitTargetOutput
from spengine.base.observer import BaseObserver
from spengine.model.pg_metadata import PgMetadata, PgUpdateMetadata
from spengine.target_output.beanstalkd_target_output import BeanstalkdTargetOutput
from spengine.target_output.file_target_output import FileTargetOutput
from spengine.target_output.kafka_target_output import KafkaTargetOutput
from spengine.target_output.pg_target_output import PgTargetOutput
from spengine.target_output.s3_target_output import S3TargetOutput
from spengine.model.elastic_metadata import ElasticMetadata
from spengine.app._elastic import ElasticClient
from spengine.target_output.elastic_target_output import EsTargetOutput


class EsOutputFactory:
    @staticmethod
    def build(config: dict) -> EsTargetOutput:
        return EsTargetOutput(
            db=ElasticClient(
                host=config["host"],
                password=config["password"],
                port=config["port"],
                username=config["username"],
            ),
            metadata=ElasticMetadata(**config["elasticMetadata"]),
        )


class RabbitOutputFactory:
    @staticmethod
    def build(config: dict) -> RabbitTargetOutput:
        return RabbitTargetOutput(
            producer=RabbitPublisher(
                durable=config.get("durable", True),
                exchange=config["exchange"],
                exchange_type=config["exchangeType"],
                host=config["host"],
                name=config["name"],
                password=config["password"],
                port=config["port"],
                queue_name=config["queueName"],
                user=config["user"],
                vhost=config.get("vhost", "/"),
                routing_key=config.get("routingKey"),
            ),
            excluded=config.get("exclude", []),
        )


class KafkaOutputFactory:
    @staticmethod
    def build(config: dict) -> KafkaTargetOutput:
        return KafkaTargetOutput(
            producer=Producer(
                servers=config["brokers"],
                topic=config["topic"],
            ),
            excluded=config.get("exclude", []),
        )


class FileOutputFactory:
    @staticmethod
    def build(config: dict) -> FileTargetOutput:
        return FileTargetOutput(filepath=config.get("filepath"))


class PgOutputFactory:
    @staticmethod
    def build(config: dict) -> PgTargetOutput:
        metadatas = []

        for m in config["data"]:
            pgmetadata = PgMetadata(**m)
            if m.get("update_metadata") is not None:
                pgmetadata.update_metadata = PgUpdateMetadata(
                    **{
                        "updated_values": m["update_metadata"]["updated_values"],
                        "condition": m["update_metadata"].get("condition"),
                        "condition_values": m["update_metadata"].get("condition_values"),
                    }
                )
            metadatas.append(pgmetadata)
        return PgTargetOutput(
            db=PgSaService(
                db=config["db"],
                host=config["host"],
                password=config["password"],
                port=config["port"],
                username=config["username"],
            ),
            # metadatas=[PgMetadata(**m) for m in config["data"]],
            metadatas=metadatas,
        )


class S3OutputFactory:
    @staticmethod
    def build(config: dict) -> S3TargetOutput:
        return S3TargetOutput(
            s3_client=S3(
                bucket_name=config["bucketName"],
                client=boto3.client(
                    "s3",
                    endpoint_url=config["endpoint"],
                    aws_access_key_id=config["accessKey"],
                    aws_secret_access_key=config["secretKey"],
                ),
                content_type=(
                    config.get("contentType") if config.get("contentType") is not None else "application/octet-stream"
                ),
            ),
        )


class BeanstalkdOutputFactory:
    @staticmethod
    def build(config: dict) -> BeanstalkdTargetOutput:
        return BeanstalkdTargetOutput(
            producer=BeanstalkProducer(host=config["host"], port=config["port"]),
            excluded=config.get("exclude", []),
            priority=config["priority"],
            ttr=config["ttr"],
            topic=config["topic"],
        )


class OutputFactory:
    @staticmethod
    def build(config: dict) -> BaseObserver:
        if config["target"] == "kafka":
            return KafkaOutputFactory.build(config["metadata"])
        elif config["target"] == "file":
            return FileOutputFactory.build(config["metadata"])
        elif config["target"] == "pg":
            return PgOutputFactory.build(config["metadata"])
        elif config["target"] == "s3":
            return S3OutputFactory.build(config["metadata"])
        elif config["target"] == "beanstalkd":
            return BeanstalkdOutputFactory.build(config["metadata"])
        elif config["target"] == "rabbit":
            return RabbitOutputFactory.build(config["metadata"])
        elif config["target"] == "elastic":
            return EsOutputFactory.build(config["metadata"])
