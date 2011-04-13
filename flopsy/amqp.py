try:
    import json # Python 2.6 and above
except ImportError:
    try:
        import simplejson as json # Python 2.5
    except ImportError:
        raise ImportError(
            'Flopsy requires simplejson for your Python version. '
            'Check <http://pypi.python.org/pypi/simplejson/>.')

import uuid
from amqplib import client_0_8 as amqp

__all__ = ['Broker', 'Consumer', 'Publisher',]


class Broker(object):
    def __init__(self, host='127.0.0.1', user='guest', password='guest', 
                 vhost='/', port=5672, insist=False):

        self.host = host
        self.user = user
        self.password = password
        self.vhost = vhost
        self.port = port
        self.insist = insist


    def connect(self):
        return amqp.Connection(
            host='%s:%s' % (self.host, self.port),
            userid=self.user,
            password=self.password,
            virtual_host=self.vhost,
            insist=self.insist
        )


class Consumer(object):
    def __init__(self, broker, routing_key='flopsy', exchange='flopsy.default',
                 queue='/', durable=True, exclusive=False, auto_delete=False,
                 exchange_type='direct'):

        self.callbacks = {}

        self.broker = broker
        self.routing_key = routing_key
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.queue = queue
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self._channel = None

    @property
    def channel(self):
        if self._channel and self._channel.connection:
            return self._channel
        elif self._channel:
            self.close()
        self._channel = self.broker.connect().channel()
        self._channel.queue_declare(
            queue=self.queue,
            durable=self.durable,
            exclusive=self.exclusive,
            auto_delete=self.auto_delete
        )
        self._channel.exchange_declare(
            exchange=self.exchange,
            type=self.exchange_type,
            durable=self.durable,
            auto_delete=self.auto_delete
        )
        self._channel.queue_bind(
            queue=self.queue,
            exchange=self.exchange,
            routing_key=self.routing_key
        )
        self._channel.basic_consume(
            queue=self.queue,
            no_ack=True,
            callback=self.dispatch,
            consumer_tag=str(uuid.uuid4())
        )
        return self._channel

    def close(self):
        if self._channel:
            connection = self._channel.connection
            try:
                self._channel.close()
                connection.close()
            except:
                self._channel = None

    def wait(self):
        while True:
            self.channel.wait()

    def dispatch(self, message):
        decoded = json.loads(message.body)
        message.body = decoded['data']
        callback = self.callbacks.get(decoded['kind'])
        if callback:
            callback(message)

    def register(self, kind, callback):
        self.callbacks[kind] = callback

    def unregister(self, kind):
        del self.callbacks[kind]


class Publisher(object):
    def __init__(self, broker, routing_key='flopsy', exchange='flopsy.default',
                 delivery_mode=2):

        self.broker = broker 
        self._channel = None
        self.exchange = exchange
        self.routing_key = routing_key
        self.delivery_mode = delivery_mode

    @property
    def channel(self):
        if self._channel and self._channel.connection:
            return self._channel
        elif self._channel:
            self.close()
        self._channel = self.broker.connect().channel()
        return self._channel

    def send(self, kind, message=None):
        encoded = json.dumps({'kind': kind, 'data': message or {}})
        message = amqp.Message(encoded)
        message.properties['delivery_mode'] = self.delivery_mode
        self.channel.basic_publish(
            message,
            exchange=self.exchange,
            routing_key=self.routing_key
        )
        return message

    def close(self):
        if self._channel:
            connection = self._channel.connection
            try:
                self._channel.close()
                connection.close()
            except:
                self._channel = None


