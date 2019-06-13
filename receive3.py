# coding=utf-8
"""
一个通用的rabbitmq生产者和消费者。使用多个线程消费同一个消息队列。
"""
from collections.abc import Callable
import functools
import time
from threading import Lock
from pika import BasicProperties
from rabbit import Rabbit
from concurrent.futures import ThreadPoolExecutor
import threading


class RabbitmqPublisher(object):
    def __init__(self, queue_name):
        self._queue_name = queue_name
        channel = Rabbit().create_channel()
        channel.queue_declare(queue=queue_name, durable=True)
        self.channel = channel
        self.lock = Lock()
        self._current_time = None
        self.count_per_minute = None
        self._init_count()
        print(f'{self.__class__} 被实例化了')

    def _init_count(self):
        self._current_time = time.time()
        self.count_per_minute = 0

    def publish(self, msg):
        with self.lock:  # 亲测pika多线程publish会出错。
            self.channel.basic_publish(exchange='',
                                       routing_key=self._queue_name,
                                       body=msg,
                                       properties=BasicProperties(
                                           delivery_mode=2,  # make message persistent
                                       )
                                       )
            print(f'放入 {msg} 到 {self._queue_name} 队列中')
            self.count_per_minute += 1
            if time.time() - self._current_time > 60:
                self._init_count()
                print(f'一分钟内推送了 {self.count_per_minute} 条消息到 {self.channel.connection} 中')


class RabbitmqConsumer(object):
    def __init__(self, queue_name, consuming_function: Callable = None, threads_num=100, max_retry_times=3):
        """
        :param queue_name:
        :param consuming_function: 处理消息的函数，函数有且只能有一个参数，参数表示消息。是为了简单，放弃策略和模板来强制参数。
        :param threads_num:
        :param max_retry_times:
        """
        self._queue_name = queue_name
        self.consuming_function = consuming_function
        self.threadpool = ThreadPoolExecutor(threads_num)
        self._max_retry_times = max_retry_times
        print(f'{self.__class__} 被实例化')
        self.rabbitmq_helper = Rabbit()
        channel = self.rabbitmq_helper.create_channel()
        channel.queue_declare(queue=self._queue_name, durable=True)
        channel.basic_qos(prefetch_count=threads_num)
        self.channel = channel

    def start_consuming_message(self):
        def callback(ch, method, properties, body):
            msg = body.decode()
            print(f'从rabbitmq取出的消息是：  {msg}')
            # ch.basic_ack(delivery_tag=method.delivery_tag)
            self.threadpool.submit(self.__consuming_function, ch, method, properties, msg)

        self.channel.basic_consume(on_message_callback=callback,
                                   queue=self._queue_name,
                                   )
        self.channel.start_consuming()

    @staticmethod
    def ack_message(channelx, delivery_tagx):
        """Note that `channel` must be the same pika channel instance via which
        the message being ACKed was retrieved (AMQP protocol constraint).
        """
        if channelx.is_open:
            channelx.basic_ack(delivery_tagx)
        else:
            # Channel is already closed, so we can't ACK this message;
            # log and/or do something that makes sense for your app in this case.
            pass

    def __consuming_function(self, ch, method, properties, msg, current_retry_times=0):
        if current_retry_times < self._max_retry_times:
            # noinspection PyBroadException
            try:
                self.consuming_function(msg)
                # ch.basic_ack(delivery_tag=method.delivery_tag)
                self.rabbitmq_helper.conn.add_callback_threadsafe(functools.partial(self.ack_message, ch, method.delivery_tag))
            except Exception as e:
                print(f'函数 {self.consuming_function}  第{current_retry_times+1}次发生错误，\n 原因是{e}')
                self.__consuming_function(ch, method, properties, msg, current_retry_times + 1)
        else:
            print(f'达到最大重试次数 {self._max_retry_times} 后,仍然失败')
            # ch.basic_ack(delivery_tag=method.delivery_tag)
            self.rabbitmq_helper.conn.add_callback_threadsafe(functools.partial(self.ack_message, ch, method.delivery_tag))


if __name__ == '__main__':
    # rabbitmq_publisher = RabbitmqPublisher('queue_test')
    # [rabbitmq_publisher.publish(str(i)) for i in range(20)]
    def f(msg):
        thread_id = threading.get_ident()
        print(f'{thread_id}: {msg}')
        time.sleep(2)


    rabbitmq_consumer = RabbitmqConsumer('queue_test', consuming_function=f, threads_num=5000)
    rabbitmq_consumer.start_consuming_message()

