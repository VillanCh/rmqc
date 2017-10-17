#!/usr/bin/env python
#coding:utf-8
"""
  Author:  v1ll4n --<chaitin>
  Purpose: 
  Created: 10/01/17
"""

import pika
import logging

from ..common import RMQCEXIF

logger = logging.getLogger('root.{}'.format(__name__))

class Consumer(RMQCEXIF):
    """"""
    
    @property
    def queue_name(self):
        """"""
        return self._queue_config.get('queue')
    
    def _initial(self):
        """"""
        self._queue_config = {}
        self._queue_bindings = []
        self._callback_table = {}

    def _get_channel(self, connection):
        """"""
        assert self._queue_config != {}, 'no rabbitmq queue declare'
        assert isinstance(connection, pika.BlockingConnection)
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
        
        # declare queue
        channel.queue_declare(**self._queue_config)
        logger.debug('declare queue:{}'.format(self._queue_config))
        
        # bind queue
        for i in self._queue_bindings:
            i['queue'] = self.queue_name
            channel.queue_bind(**i)
            logger.debug('bind queue:{} with {}'.format(self.queue_name, i))
        
        return channel

    def queue_bind_exchange(self, exchange, routing_key=None, arguments=None):
        """"""
        params = {
            'exchange': exchange,
            'routing_key': routing_key,
            'arguments': arguments,
        }
        
        if params not in self._queue_bindings:
            self._queue_bindings.append(params)
        
    def declare_queue(self, queue='', passive=False, durable=False, 
                      exclusive=False, 
                      auto_delete=False, 
                      arguments=None):
        """"""
        assert self._queue_config == {}, 'exsited queue declare . ' + \
               '(A consumer only has one rabbitmq queue), you cannot declare again'
        
        queue_declare_config = {
            'queue': queue,
            'passive': passive,
            'durable': durable,
            'exclusive': exclusive,
            'auto_delete': auto_delete,
            'arguments': arguments,
        }
        
        self._queue_config.update(queue_declare_config)
    
    def _mainloop(self):
        """"""
        connection = self.connection = self._connect()
        channel = self.channel = self._get_channel(connection)
        
        self._working = True
        
        logger.debug('entering the mainloop')
        channel.basic_consume(self._default_callback, self.queue_name)
        channel.start_consuming()
        
        logger.debug('stopped the consuming')
        channel.close()
        connection.close()
    
    def _default_callback(self, ch, mt, pro, body):
        """"""
        # collecting basic information
        exchange = mt.exchange
        routing_key = mt.routing_key
        callback = self.get_callback(exchange, routing_key)
        if callback:
            try:
                ack_flag = callback(mt, pro, body)
            except:
                ack_flag = False
            
            if ack_flag:
                self.ack(mt)
                return
            else:
                self.nack(mt)
                return
        else:
            self._msg_queue.put((mt, pro, body))
    
    def ack(self, method_frame, multiple=False):
        """"""
        try:
            self.channel.basic_ack(method_frame.delivery_tag, multiple)
            logger.debug('acked the message-{}'.format(method_frame.delivery_tag))
            return True
        except:
            logger.debug('ack failed for the message-{}'.format(method_frame.delivery_tag))
            return False
    
    def nack(self, method_frame, multiple=False, requeue=True):
        """"""
        try:
            self.channel.basic_nack(method_frame.delivery_tag, multiple, requeue)
            logger.debug('reject the message-{}'.format(method_frame.delivery_tag))
            return True
        except:
            return False        
        
    
    def register_callback(self, exchange, routing_key, callback):
        """"""
        key = (exchange, routing_key)
        assert key not in self._callback_table, 'existed callback, you should ' + \
               'unregister the callback first (if you want to change it.)'
        
        self._callback_table[key] = callback
    
    def unregister_callback(self, exchange, routing_key):
        """"""
        key = (exchange, routing_key)
        if key in self._callback_table:
            del self._callback_table[key]
    
    def get_callback(self, exchange, routing_key):
        """"""
        return self._callback_table.get((exchange, routing_key))
    
    def stop(self):
        """"""
        logger.debug('stopping consuming.')
        self._working = False
        self.channel.stop_consuming()
    
    @property
    def message_queue(self):
        """"""
        return self._msg_queue

    
    