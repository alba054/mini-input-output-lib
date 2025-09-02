from pydantic.v1 import BaseSettings


class KafkaConfig(BaseSettings):
    KAFKA_BOOTSTRAPS: str
    KAFKA_AUTO_OFFSET_RESET: str
    KAFKA_GROUP_ID: str
    KAFKA_MAX_POLL_RECORDS: int
    KAFKA_TOPIC: str
    KAFKA_AUTO_COMMIT: bool

    KAFKA_BOOTSTRAPS_TARGET: str
    KAFKA_AUTO_OFFSET_RESET_TARGET: str
    KAFKA_GROUP_ID_TARGET: str
    KAFKA_MAX_POLL_RECORDS_TARGET: int
    KAFKA_TOPIC_TARGET: str
    KAFKA_AUTO_COMMIT_TARGET: bool

    class Config:
        env_file = ".env"
