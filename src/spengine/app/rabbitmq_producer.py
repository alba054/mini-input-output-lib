import ssl
import threading
from pika import ConnectionParameters, BlockingConnection, PlainCredentials, SSLOptions


class RabbitPublisher(threading.Thread):
    def __init__(
        self,
        name: str,
        queue_name: str,
        exchange: str,
        exchange_type: str,
        host: str,
        port: int,
        user: str,
        password: str,
        vhost: str,
        durable: bool = True,
        routing_key: str = None,
        tls: bool = False,
    ):
        super().__init__()
        self.daemon = True
        self.is_running = True
        self.name = name
        self.queue = queue_name
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.vhost = vhost
        self.durable = durable
        self.routing_key = routing_key

        credentials = PlainCredentials(self.user, self.password)

        ssl_context = None

        if tls:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

        parameters = ConnectionParameters(
            self.host,
            self.port,
            credentials=credentials,
            virtual_host=self.vhost,
            ssl_options=SSLOptions(context=ssl_context) if ssl_context is not None else None,
        )
        self.connection = BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type, durable=self.durable)
        self.channel.queue_declare(queue=self.queue, durable=self.durable)
        self.channel.queue_bind(exchange=self.exchange, queue=self.queue, routing_key=self.routing_key)

    def run(self):
        while self.is_running:
            self.connection.process_data_events(time_limit=1)

    def _publish(self, message: str | bytes):
        self.channel.basic_publish(self.exchange, self.queue, body=message)

    def publish(self, message: str | bytes):
        self.connection.add_callback_threadsafe(lambda: self._publish(message))

    def stop(self):
        print("Stopping...")
        self.is_running = False
        # Wait until all the data events have been processed
        self.connection.process_data_events(time_limit=1)
        if self.connection.is_open:
            self.connection.close()
        print("Stopped")
