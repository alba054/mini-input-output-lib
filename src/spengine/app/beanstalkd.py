import json
from venv import logger
from greenstalk import Client, TimedOutError


class BeanstalkProducer:
    bs: Client
    _host: str
    _port: int

    def __init__(self, host: str, port: int):
        self._host = host
        self._port = port

        self.bs = Client((host, port))

    def reconnect(self):
        self.bs = Client((self._host, self._port))

    def use(self, topic: str):
        self.bs.use(topic)

    def put(self, data: dict, priority=2**16, ttr=60 * 60):
        try:
            self.bs.put(json.dumps(data), ttr=ttr, priority=priority)
        except Exception as e:
            logger.error(e)


class BeanstalkConsumer:
    bs: Client
    _host: str
    _port: int
    _topic: str

    def __init__(self, host: str, port: int, topic: str):
        self._host = host
        self._port = port
        self._topic = topic
        self.bs = Client((host, port))
        self.bs.watch(topic)

    def reconnect(self):
        self.bs = Client((self._host, self._port))
        self.bs.watch(self._topic)

    def reserve(self):
        try:
            job = self.bs.reserve(60 * 60)
            if job is None:
                return None
            return job
        except TimedOutError as e:
            raise e
        except Exception as e:
            logger.error(e)
            return None

    def bury(self, job):
        try:
            self.bs.bury(job)
        except TimedOutError as e:
            raise e
        except Exception as e:
            logger.error(e)
            return None

    def delete(self, job):
        try:
            self.bs.delete(job)
        except TimedOutError as e:
            raise e
        except Exception as e:
            logger.error(e)
            return None
