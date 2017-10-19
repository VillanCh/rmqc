#!/usr/bin/env python
#coding:utf-8
"""
  Author:  v1ll4n --<>
  Purpose: MessageGetter
  Created: 10/02/17
"""

import time
import pika
import logging
import traceback

from ..common import RMQCBase

logger = logging.getLogger('root.{}'.format(__name__))

class MessageGetter(RMQCBase):
    """"""
    
    def _reset_channel(self):
        """"""
        logger.info('resetting channel...')
        self.connection = self._connect()
        self.channel = self._get_channel(self.connection)
        

    @property
    def queue_name(self):
        """"""
        return self._queue_config.get('queue')
    
    def _initial(self):
        """"""
        # init channel and connection
        self.channel = None
        self.connection = None
        
        # init exchange config
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

    def get(self, no_ack=False, retry_times=5, retry_timeout=0.5):
        """"""
        if not self.channel:
            self._reset_channel()
        
        for _ in range(retry_times):
            try:
                msg = self.channel.basic_get(self.queue_name, no_ack)
                return msg
            except Exception as e:
                # cleaning the old connection
                try:
                    self.channel.close()
                    self.connection.close()
                except:
                    pass
                
                logger.warning(traceback.format_exc())
                time.sleep(retry_timeout)
                
                # reconnecting
                try:
                    self._reset_channel()
                except:
                    pass
    
    def ack(self, delivery_tag_or_getOk, multiple=False):
        """"""
        if not delivery_tag_or_getOk:
            return False
        
        delivery_tag = delivery_tag_or_getOk if not hasattr(delivery_tag_or_getOk, 'delivery_tag') \
            else getattr(delivery_tag_or_getOk, 'delivery_tag')
        try:
            self.channel.basic_ack(delivery_tag, multiple)
            logger.debug('acked the message-{}'.format(delivery_tag))
            return True
        except:
            logger.debug('ack failed for the message-{}'.format(delivery_tag))
            return False
    
    def nack(self, delivery_tag_or_getOk, multiple=False, requeue=True):
        """"""
        if not delivery_tag_or_getOk:
            return 
        
        delivery_tag = delivery_tag_or_getOk if not hasattr(delivery_tag_or_getOk, 'delivery_tag') \
            else getattr(delivery_tag_or_getOk, 'delivery_tag')
        try:
            self.channel.basic_nack(delivery_tag, multiple, requeue)
            logger.debug('not acked the message-{}'.format(delivery_tag))
            return True
        except:
            logger.debug('ack failed for the message-{}'.format(delivery_tag))
            return False        
    
    
    def close_connection(self):
        """"""
        # close connection and reset channel as None
        try:
            self.channel.close()
        except:
            pass
        
        self.channel = None

        try:
            self.connection.close()
        except:
            pass
        
        self.connection = None
    
    def clear_all(self):
        """"""
        queue_name = self._queue_config.get('queue')
        
        if not self.channel:
            return
        
        try:
            for params in self._queue_bindings: 
                if 'queue' in params:
                    self.channel.queue_unbind(**params)
                else:
                    self.channel.queue_unbind(queue_name,
                                              **params)
        except Exception as e:
            traceback.print_exc()
        
        try:
            self.channel.queue_delete(queue_name)
        except Exception as e:
            traceback.print_exc()
            