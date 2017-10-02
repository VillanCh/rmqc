# RMQC

RmqC 是 "RabbitMQ Clients". 他并不是一个完整特性支持的 RabbitMQ 客户端，是基于 Pika 开发的一个 RabbitMQ 客户端的 Shortcuts，可以直接添加子模块在你的项目中使用，作为一个 <del>表面</del> 异步的 RabbitMQ 客户端：至少使用起来，他的 Publisher 和 Consumer 非常的 "异步"。

实际上，Rmqc 却是使用了 pika 的  `BlockingConnection` 做的，并不是 SelectConnection，因为我们默认 Pika 提供的 RabbitMQ 客户端是不稳定的（经过理论“端到端原则”和本人的一些简单实践，这么做是有一定道理）。

同样的 RMQC 限定了 RabbitMQ 的使用方法：

* Publisher 只能发布到 Exchange 不能关心任何的具体的 RabbitMQ 的 Queue
* Consumer 只关心 Exchange 和 Routing_key 并不会关心任何 Publisher
* MessageGetter 同 Consumer 只是消息是主动获取（Poll），而不是被动接受



## Examples

一个 Publisher 的测试实例

```python
#!/usr/bin/env python
#coding:utf-8
"""
  Author:  v1ll4n --<cnss>
  Purpose: Test Publisher
  Created: 09/30/17
"""

import unittest
import time

from ..publisher import Publisher

class PublisherTester(unittest.TestCase):
    """"""

    def test_publisher(self):
        """"""
        publisher = Publisher()
        publisher.add_exchange("testexchange", "direct")
        publisher.serve_as_thread()
        
        for i in range(4):
            publisher.feed(message='testmessage-{}', 
                           exchange='testexchange', 
                           routing_key='testkey')
            
        
        time.sleep(1)
        publisher.stop()
```

Consumer.py

```Python
#!/usr/bin/env python
#coding:utf-8
"""
  Author:  v1ll4n --<>
  Purpose: Consumer
  Created: 10/01/17
"""

import unittest
import time

from ..publisher import Publisher
from ..consumer import Consumer


def callback(mt, pro, body):
    print('messeage from testexchange:testkey {}'.format(body))
    return True


class ConsumerTester(unittest.TestCase):
    """"""

    def test_consumer(self):
        """"""
        consumer = Consumer()
        consumer.add_exchange(exchange='testexchange', type='direct')
        consumer.add_exchange(exchange='exchangename', type='direct')
        consumer.declare_queue('queue')
        consumer.queue_bind_exchange(exchange="testexchange", routing_key='testkey')
        consumer.queue_bind_exchange(exchange="exchangename", routing_key='testkey1')
        

        
        consumer.register_callback('testexchange', 'testkey', callback)

        consumer.serve_as_thread()
        
        print('serve consumer successfully')
        
        publisher = Publisher()
        publisher.add_exchange("testexchange", "direct")
        publisher.add_exchange("exchangename", "direct")
        publisher.serve_as_thread()
        
        
        print('serve publisher successfully')
        for i in range(4):
            publisher.feed(message='testmessage-{}', 
                           exchange='testexchange', 
                           routing_key='testkey')
            
        print('testmessage exchange publish ')
        
        for i in range(8):
            publisher.feed(message='exchangename-{}'.format(i),
                           exchange='exchangename',
                           routing_key='testkey1')
            
        print('exchangename exchange publish')
        time.sleep(1)
        
        print('stop publsiher')
        publisher.stop()
        
        while not consumer.message_queue.empty():
            mt, pro, body = consumer.message_queue.get()
            print('received from method_frame:{} body:{}'.format(mt, body))
            consumer.ack(mt)
        
        print('stop consumer')
        consumer.stop()
        
        print('shutdown')
 

if __name__ == '__main__':
    unittest.main()
```

Message_getter.py

```python
#!/usr/bin/env python
#coding:utf-8
"""
  Author:  v1ll4n --<>
  Purpose: MessageGetter
  Created: 10/02/17
"""

import time

import unittest

from ..msggetter import MessageGetter
from ..publisher import Publisher

class MessageGetterTester(unittest.TestCase):
    """"""

    def test_message_getter(self):
        """"""
        msgtr = MessageGetter()
        msgtr.add_exchange('testexchange', 'direct')
        msgtr.add_exchange('exchangename', 'direct')
        msgtr.declare_queue('testqueue')
        msgtr.queue_bind_exchange('testexchange', 'test1')
        msgtr.queue_bind_exchange('exchangename', 'test22')
        
        publisher = Publisher()
        publisher.add_exchange('testexchange', 'direct')
        publisher.add_exchange('exchangename', 'direct')
        publisher.serve_as_thread()
        
        for i in range(100):
            publisher.feed('testexchange', 'test1', 'this is a message from testexchange')
            publisher.feed('exchangename', 'test22', 'this is a message from exchangename')
        
        time.sleep(1)
        
        while True:
            msg = msgtr.get()
            print(msg)
            getok, properties, body = msg
            
            if not getok:
                break
            
            msgtr.ack(getok)
    
        publisher.stop()
        msgtr.close_connection()

if __name__ == '__main__':
    unittest.main()
```

