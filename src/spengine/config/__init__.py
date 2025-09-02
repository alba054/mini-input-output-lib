import dotenv

from spengine.config.kafka_config import KafkaConfig

# from config.postgres_config import PostgreConfig

dotenv.load_dotenv(override=True)

kafka_config = KafkaConfig()
# pg_config = PostgreConfig()
