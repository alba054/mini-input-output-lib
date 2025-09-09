from loguru import logger
from spengine.app.kafka import Consumer, ConsumerBatch
from spengine.data_source.data_source_subject import DataSourceSubject
from spengine.processor.base_processor import BaseProcessor


class KafkaBatchSource(DataSourceSubject):
    consumer: Consumer

    def __init__(self, consumer: ConsumerBatch, processors: list[BaseProcessor]):
        super().__init__(processors=processors)
        self.consumer = consumer

    def notify(self):
        for message in self.consumer:
            self.data = message.value
            self.additional_info = {"kafka_timestamp": message.timestamp}

            for processor in self.processors:
                self.data = processor.process(data=self.data, additional_info=self.additional_info)

            for observer in self._service_observers:
                try:
                    observer.update(self)
                except Exception as e:
                    logger.error(e)
                    continue
