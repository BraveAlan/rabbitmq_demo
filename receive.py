from queue import Queue
from multiprocessing.pool import ThreadPool
import threading
from time import sleep
import datetime

from rabbit import Rabbit

class Model(object):
    q = Queue()  # local queue before thread pool, put msg into q when recved from mq
    threadNum = 100
    # 线程数
    # deal with api start req and sample finish req
    pool = ThreadPool(int(threadNum))

    def __init__(self):
        pass

    def worker(self):
        while True:
            method = self.q.get()
            method()

    def start_pool(self):
        numsTasks = int(self.threadNum)
        for i in range(numsTasks):
            self.pool.apply_async(self.worker)

    def algorithmCallback(self, ch, method, properties, body):
        sleep(1)
        nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f'{nowTime}: {body}')
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def consume_from_rmq(self):
        rabbit = Rabbit()
        rabbit.declare_queue("algorithm", durable=True)
        rabbit.register_consumer(queue_name="algorithm", callback=self.algorithmCallback, no_ack=False)
        rabbit.start_consuming()

    def start(self):
        self.start_pool()
        self.q.put(self.consume_from_rmq())

    def stop(self):
        pass

    def callback(self, ch, method, properties, body):
        sleep(1)
        nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        threadId = threading.get_ident()
        print(f'{nowTime}:{threadId}:{body}')
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def receive_msg(self):
        rabbit = Rabbit()
        rabbit.declare_queue("algorithm", durable=True)
        rabbit.register_consumer(queue_name="algorithm", callback=self.callback, no_ack=False)
        rabbit.start_consuming()

    def test(self):
        sleep(1)
        threadId = threading.get_ident()
        print(f'hello {threadId}')

    def my_start(self):
        for i in range(2):
            t_msg = threading.Thread(target=self.receive_msg)
            t_msg.start()
            t_msg.join(0)

# def algorithmCallback(ch, method, properties, body):
#     print(body)

if __name__ == '__main__':
    Model().my_start()
    # Model().receive_msg()
    # rabbit = Rabbit()
    # rabbit.declare_queue("algorithm", durable=True)
    # rabbit.register_consumer(queue_name="algorithm", callback=algorithmCallback, no_ack=False)
    # rabbit.start_consuming()

