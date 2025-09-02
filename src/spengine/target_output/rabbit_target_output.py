import json
from loguru import logger
from spengine.app.rabbitmq_producer import RabbitPublisher
from spengine.base.observer import BaseObserver
from spengine.data_source.data_source_subject import DataSourceSubject


class RabbitTargetOutput(BaseObserver):
    producer: RabbitPublisher
    excluded: list[str]

    # the order of processor matters
    # because processor will be called in the order of the list
    def __init__(self, producer: RabbitPublisher, excluded: list[str]):
        super().__init__()
        self.producer = producer
        self.excluded = excluded

        self.producer.start()

    def update(self, subject: DataSourceSubject):
        data = subject.data

        try:
            if data is None:
                return

            data = {k: v for k, v in data.items() if k not in self.excluded}

            self.producer.publish(json.dumps(data).encode())
            logger.info("success produce")
        except KeyboardInterrupt:
            self.producer.stop()
            self.producer.join()
        except Exception as e:
            logger.error(e)
            return
