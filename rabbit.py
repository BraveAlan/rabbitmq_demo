# -*- coding:utf-8 -*-

import pika


class Rabbit(object):

    def __init__(self, host="localhost"):

        self.conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.conn.channel()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def create_channel(self):
        return self.conn.channel()

    # 声明一个队列
    def declare_queue(self, queue_name='', durable=False, exclusive=False):
        result = self.channel.queue_declare(queue=queue_name, durable=durable, exclusive=exclusive)
        # 返回队列名
        return result.method.queue

    # 声明一个临时队列
    def declare_temp_queue(self):
        result = self.channel.queue_declare(queue='', exclusive=True)
        # 返回队列名
        return result.method.queue

    # 声明一个交换机
    def declare_exchange(self, ex, ex_type):
        self.channel.exchange_declare(exchange=ex, exchange_type=ex_type)

    # 队列绑定交换机
    def bind_queue(self, ex, queue_name, binding_key):
        self.channel.queue_bind(exchange=ex, queue=queue_name, routing_key=binding_key)

    # 生产一个消息
    def produce(self, msg, r_key, ex='', durable=True):
        delivery_mode = 2 if durable else 1
        self.channel.basic_publish(exchange=ex,
                                   routing_key=r_key,
                                   body=msg,
                                   properties=pika.BasicProperties(
                                       delivery_mode=delivery_mode  # 2 make message persistent
                                   ))

    def register_consumer(self, queue_name, callback, no_ack=False):
        if not callback:
            print("callback function is null, please implemention it")
            return
        else:
            self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=no_ack)

    def start_consuming(self):
        print('[*] Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()

    # 在同一时刻，不要发送超过1条消息给一个工作者，直到它已经处理了上一条消息并且作出了响应
    def set_qos(self, prefetch_count=1):
        self.channel.basic_qos(prefetch_count=prefetch_count)

    def close(self):
        self.conn.close()

    def test(self, msg):
        print(msg)

