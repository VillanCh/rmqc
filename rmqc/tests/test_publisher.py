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