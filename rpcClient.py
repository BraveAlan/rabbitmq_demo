import pika
import uuid
import json
from rabbit import Rabbit



class RpcClient(object):

    def __init__(self, routing_key, host="localhost"):
        # routing_key - 'rpc_queue'
        self.routing_key = routing_key

        self.rabbit = Rabbit(host)
        # 队列名，随机
        self.callback_queue_name = self.rabbit.declare_queue(exclusive=True)
        self.rabbit.register_consumer(queue_name=self.callback_queue_name, callback=self.on_response)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.rabbit.close()

    def on_response(self, ch, method, props, body):
        """
        收到消息就调用
        :param ch: 管道内存对象地址
        :param method: 消息发给哪个queue
        :param props:
        :param body: 数据对象
        :return:
        """
        # 判断本机生成的ID与生产端发过来的ID是否相等
        if self.corr_id == props.correlation_id:
            # 将body值 赋值给self.response
            self.response = body

    def call(self, msg):
        """
        发送dict
        :param msg:
        :type msg: dict
        :return: dict
        """
        # 随机的字符串
        self.response = None
        self.corr_id = str(uuid.uuid4())

        # routing_key='rpc_queue' 发一个消息到rpc_queue内
        self.rabbit.channel.basic_publish(exchange='',
                                   routing_key=self.routing_key,
                                   properties=pika.BasicProperties(
                                       # 执行命令之后消费端将结果返回给self.callaback_queue这个队列中
                                       reply_to=self.callback_queue_name,
                                       # 生成UUID 发送给消费端
                                       correlation_id=self.corr_id,
                                   ),
                                   # 发的消息，必须传入字符串，不能传数字
                                   body=json.dumps(msg))

        # 监听数据
        while self.response is None:
            # 没有消息不阻塞
            self.rabbit.conn.process_data_events()
        return json.loads(str(self.response, encoding="utf-8"))






