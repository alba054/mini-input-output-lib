from loguru import logger
from spengine.app.beanstalkd import BeanstalkProducer
from spengine.base.observer import BaseObserver
from spengine.data_source.data_source_subject import DataSourceSubject


class BeanstalkdTargetOutput(BaseObserver):
    producer: BeanstalkProducer
    excluded: list[str]
    _priority: int
    _ttr: int
    _topic: str

    # the order of processor matters
    # because processor will be called in the order of the list
    def __init__(
        self,
        producer: BeanstalkProducer,
        excluded: list[str],
        priority: int,
        ttr: int,
        topic: str,
    ):
        super().__init__()
        self.producer = producer
        self.excluded = excluded
        self._priority = priority
        self._ttr = ttr
        self._topic = topic

    def update(self, subject: DataSourceSubject):
        data = subject.data

        self.producer.use(self._topic)
        try:

            if data is None:
                return

            data = {k: v for k, v in data.items() if k not in self.excluded}

            self.producer.put(data, self._priority, self._ttr)
            logger.info("success produce")
        except Exception as e:
            logger.error(e)
            self.producer.reconnect()
            self.producer.use(self._topic)
            return
