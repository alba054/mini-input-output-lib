from loguru import logger
from spengine.app.kafka import Consumer
from spengine.data_source.data_source_subject import DataSourceSubject
from spengine.processor.base_processor import BaseProcessor


class KafkaSource(DataSourceSubject):
    consumer: Consumer

    def __init__(self, consumer: Consumer, processors: list[BaseProcessor]):
        super().__init__(processors=processors)
        self.consumer = consumer

    def notify(self):
        for message in self.consumer:
            self.data = message.value
            self.additional_info = {"kafka_timestamp": message.timestamp, "kafka_topic": message.topic}

            for processor in self.processors:
                try:
                    self.data = processor.process(data=self.data, additional_info=self.additional_info)
                except Exception as e:
                    logger.error(e)
                    self.data = None
                    break

            for observer in self._service_observers:
                try:
                    observer.update(self)
                except Exception as e:
                    import traceback

                    traceback.print_exc()
                    logger.error(e)
                    continue
