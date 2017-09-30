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

import logging
import threading

import pika

from ..config import default_config

logger = logging.getLogger('root.{}'.format(__name__))

class Publisher(object):
    """"""

    def __init__(self):
        """"""
        logger.debug('getting connection params')
        self.pika_connection_params = default_config.get_connection_param()
        
        logger.debug('init message queue')
        self._msg_queue = queue.Queue()
        
        self.exchanges = {}

        self._working = True
        
        logger.debug('init success')
    
    def add_exchange(self, exchange, type, **declare_config):
        """"""
        assert exchange not in self.exchanges, 'existed exchange.'
        self.exchanges[exchange] = (exchange, type, declare_config)
    
    def serve_as_thread(self):
        """"""
        pass
    
    def _connect(self):
        """"""
        connection = pika.BlockingConnection(self.pika_connection_params)
        return connection
    
    def _get_channel(self, connection):
        """"""
        assert isinstance(connection, pika.BlockingConnection), \
               'not a valid connection'
        
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
            
        # start confirm
        channel.confirm_delivery()
        
        return channel
    
    def _mainloop(self):
        """"""
        channel = self._get_channel(self._connect())
        
        self._working = True
        
        while self._working:
            if self._msg_queue.empty():
                continue
            
            msg = self._msg_queue.get()
            ename, rkey, body, config = msg
            if channel.basic_publish(ename, rkey, body,
                                     properties=config):
                logger.debug('publish success for msg: {}'\
                             .format(msg))
            else:
                logger.debug('failed to publish: {}, requeue.'\
                             .format(msg))
                self._msg_queue.put(msg)
    
    def publish(self, exchange, routing_key, message, **properties):
        """"""
        basic_properties = pika.BasicProperties(**properties)
        msg = (exchange, routing_key, message, basic_properties)
        
        self._msg_queue.put(msg)
    
    feed = publish
    
    def serve(self):
        """"""
        logger.debug('starting serving the publisher.')
        self._mainloop()
        logger.debug('stopped serving the publisher')
        
    def serve_as_thread(self):
        """"""
        # serve in a thread
        self.publishing_thread = threading.Thread(
            name='publishing-thread',
            target=self.serve
        )   
        #self.publishing_thread.daemon = True
        self.publishing_thread.start()
    
    def stop(self):
        """"""
        self._working = False