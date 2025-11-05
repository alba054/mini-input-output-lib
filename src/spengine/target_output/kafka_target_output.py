from loguru import logger
from spengine.app.kafka import Producer
from spengine.base.observer import BaseObserver
from spengine.data_source.data_source_subject import DataSourceSubject


class KafkaTargetOutput(BaseObserver):
    producer: Producer
    excluded: list[str]
    batch: bool

    # the order of processor matters
    # because processor will be called in the order of the list
    def __init__(self, producer: Producer, excluded: list[str], batch: bool = False):
        super().__init__()
        self.producer = producer
        self.excluded = excluded
        self.batch = batch

    def update(self, subject: DataSourceSubject):
        data = subject.data

        try:
            if data is None:
                return

            if isinstance(data, dict):
                data = {k: v for k, v in data.items() if k not in self.excluded}

            if isinstance(data, dict):
                if len(data.keys()) == 0:
                    return

            if not self.batch:
                self.producer.produce(data)
            if self.batch:
                if isinstance(data, list):
                    for d in data:
                        if d is None:
                            continue
                        elif isinstance(d, dict):
                            if len(d.keys()) == 0:
                                continue

                        self.producer.produce(d)
                else:
                    self.producer.produce(data)

            logger.info("success produce")
        except Exception as e:
            logger.error(e)
            return
