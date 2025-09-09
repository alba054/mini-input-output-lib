from loguru import logger
from spengine.app.kafka import Producer
from spengine.base.observer import BaseObserver
from spengine.data_source.data_source_subject import DataSourceSubject


class KafkaTargetOutput(BaseObserver):
    producer: Producer
    excluded: list[str]

    # the order of processor matters
    # because processor will be called in the order of the list
    def __init__(self, producer: Producer, excluded: list[str]):
        super().__init__()
        self.producer = producer
        self.excluded = excluded

    def update(self, subject: DataSourceSubject):
        data = subject.data

        try:
            if data is None:
                return

            data = {k: v for k, v in data.items() if k not in self.excluded}

            self.producer.produce(data)
            logger.info("success produce")
        except Exception as e:
            logger.error(e)
            return
