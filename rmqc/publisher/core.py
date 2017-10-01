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
        self._retry_interval = default_config.get_retry_interval()
        
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
        logger.debug('connecting to {}'.format(self.pika_connection_params))
        connection = pika.BlockingConnection(self.pika_connection_params)
        logger.debug('connected.')
        return connection
    
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
    
    def serve(self):
        """"""
        logger.debug('starting serving the publisher.')
        while True:
            try:
                self._mainloop()
                break
            except Exception as e:
                warnings.warn(traceback.format_exc())
                
                logger.info('closing the connection and channel')
                try:
                    self.channel.close()
                    self.connection.close()
                except:
                    pass
                
                logger.warning('reconnecting in {} seconds.'\
                               .format(self._retry_interval))
                
                time.sleep(self._retry_interval)
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
        if hasattr(self, 'publishing_thread'):
            thd = getattr(self, 'publishing_thread')
            if isinstance(thd, threading.Thread):
                if thd.is_alive():
                    self._working = False
                    try:
                        self.publishing_thread.join()
                    except:
                        pass