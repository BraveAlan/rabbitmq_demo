from rabbit import Rabbit
import json
if __name__ == '__main__':
    rabbit = Rabbit()
    rabbit.declare_queue("queue_test", durable=True)
    test_data = {
        'action': 'launch',
        'owner': 'aiur_system',
        'eval_info': {
            'name': '【测试数据】视频-视频黑白边检测模型评测任务',
            'model': '003',
            'desc': 'today is friday',
            'sample_pattern': '',
            'num': '200',
            'extra': {'source': 'commercialization'}
        }
    }
    for i in range(10000):
        temp = json.dumps(test_data)
        rabbit.produce(f'{i}: {temp}', "queue_test")
    rabbit.close()
