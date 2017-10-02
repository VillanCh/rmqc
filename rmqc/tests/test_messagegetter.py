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