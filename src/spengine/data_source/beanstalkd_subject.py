import json
from loguru import logger
from spengine.app.beanstalkd import BeanstalkConsumer
from spengine.data_source.data_source_subject import DataSourceSubject
from greenstalk import TimedOutError

from spengine.processor.base_processor import BaseProcessor


class BeanstalkdSubject(DataSourceSubject):
    consumer: BeanstalkConsumer

    def __init__(self, consumer: BeanstalkConsumer, processors: list[BaseProcessor]):
        super().__init__(processors=processors)
        self.consumer = consumer

    def notify(self):
        counter = 0
        job = None
        while True:
            try:
                counter += 1
                if counter == int(1e5):
                    counter = 0
                    self.consumer.reconnect()

                job = self.consumer.reserve()
                if job is None:
                    continue

                self.data = json.loads(job.body)
                for processor in self.processors:
                    self.data = processor.process(data=self.data)

                for observer in self._service_observers:
                    observer.update(self)

                self.consumer.delete(job)
            except TimedOutError as _:
                self.consumer.reconnect()
            except BrokenPipeError as _:
                self.consumer.reconnect()
            except Exception as e:
                import traceback

                traceback.print_exc()
                logger.error(e)
                if job is not None:
                    self.consumer.bury(job)
                continue
