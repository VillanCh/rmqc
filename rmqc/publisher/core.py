#!/usr/bin/env python
#coding:utf-8
"""
  Author:  v1ll4n --<>
  Purpose: Publisher
  Created: 09/30/17
"""

try:
    import queue
except:
    import Queue as queue

import time
import warnings
import logging
import traceback
import threading

import pika

from ..common import RMQCEXIF
from ..config import log_level

logger = logging.getLogger('root.{}'.format(__name__))
logger.setLevel(log_level)

class Publisher(RMQCEXIF):
    """"""

    
    def _get_channel(self, connection):
        """"""
        assert isinstance(connection, pika.BlockingConnection), \
               'not a valid connection'
        
        logger.debug('getting channel.')
        channel = connection.channel()
        
        # declare exchange
        for (exchange, exchange_p) in self.exchanges.items():
            assert isinstance(exchange_p, tuple)
            assert len(exchange_p) == 3
            exchange, etype, declare_config = exchange_p
            channel.exchange_declare(
                exchange=exchange,
                exchange_type=etype,
                **declare_config
            )
            logger.debug('declare exchange: {}:{} with {}'.format(exchange, etype, \
                                                                  declare_config))
            
        # start confirm
        logger.debug('start confirming delivery')
        channel.confirm_delivery()
        
        logger.debug('finished the channel')
        return channel
    
    def _mainloop(self):
        """"""
        connection = self.connection = self._connect()
        channel = self.channel = self._get_channel(connection)
        
        self._working = True
        
        logger.debug('entering the mainloop')
        while self._working:
            if self._msg_queue.empty():
                time.sleep(0.1)
                continue
            
            msg = self._msg_queue.get()
            ename, rkey, body, config = msg
            
            try:
                delivery_flag = channel.basic_publish(ename, rkey, body,
                                                      properties=config)
            except Exception as e:
                self._msg_queue.put(msg)
                raise e
            
            # ack publisher
            if delivery_flag:
                logger.debug('publish success for msg: {}'\
                             .format(msg))
            else:
                logger.debug('failed to publish: {}, requeue.'\
                             .format(msg))
                self._msg_queue.put(msg)
        
        channel.close()
        connection.close()
        
    
    def publish(self, exchange, routing_key, message, **properties):
        """"""
        basic_properties = pika.BasicProperties(**properties)
        msg = (exchange, routing_key, message, basic_properties)
        
        self._msg_queue.put(msg)
    
    feed = publish
    
