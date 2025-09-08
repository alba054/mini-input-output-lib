import json
import time
from loguru import logger
from spengine.app.rabbitmq_consumer import RabbitConsumer
from spengine.data_source.data_source_subject import DataSourceSubject
from spengine.processor.base_processor import BaseProcessor


class RabbitSource(DataSourceSubject):
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        vhost: str,
        processors: list[BaseProcessor],
        exchange: str,
        exchange_type: str,
        queue_name: str,
        routing_key: str = "",
        prefetch_count: int = 1,
        durable: bool = True,
        tls: bool = False,
    ):
        super().__init__(processors=processors)
        self._reconnect_delay = 0
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.vhost = vhost
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.queue_name = queue_name
        self.routing_key = routing_key
        self.prefetch_count = prefetch_count
        self.durable = durable
        self.tls = tls

        self._consumer = RabbitConsumer(
            self.host,
            self.port,
            self.user,
            self.password,
            self.vhost,
            self.exchange,
            self.exchange_type,
            self.queue_name,
            self.on_message,
            self.routing_key,
            self.durable,
            tls=self.tls,
        )

    def on_message(self, _unused_channel, basic_deliver, properties, body):
        self.data = json.loads(body)

        try:
            for processor in self.processors:
                self.data = processor.process(data=self.data, additional_info=self.additional_info)

            for observer in self._service_observers:
                observer.update(self)

            self._consumer.acknowledge_message(basic_deliver.delivery_tag)
        except Exception as e:
            import traceback

            traceback.print_exc()
            logger.error(e)

    def notify(self):
        while True:
            try:
                self._consumer.run()
            except Exception as e:
                logger.error(e)
                self._consumer.stop()
                break
            self._maybe_reconnect()

    def _maybe_reconnect(self):
        if self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            logger.info("Reconnecting after {reconnect_delay} seconds")
            time.sleep(reconnect_delay)
            self._consumer = RabbitConsumer(
                self.host,
                self.port,
                self.user,
                self.password,
                self.vhost,
                self.exchange,
                self.exchange_type,
                self.queue_name,
                self.on_message,
                self.routing_key,
                self.durable,
                tls=self.tls,
            )

    def _get_reconnect_delay(self):
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay
