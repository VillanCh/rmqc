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