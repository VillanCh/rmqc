#!/usr/bin/env python
#coding:utf-8
"""
  Author:   --<>
  Purpose: Common Interface
  Created: 10/01/17
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

from .config import current_config

logger = logging.getLogger('root.{}'.format(__name__))

class RMQCBase:
    """"""

    def __init__(self, config=None):
        """"""
        self.config = config if config else current_config
        assert isinstance(self.config, type(current_config)), 'not valid config type.'
        
        logger.debug('getting connection params')
        self.pika_connection_params = self.config.get_connection_param()
        
        logger.debug('init message queue')
        self._msg_queue = queue.Queue()
        
        self.exchanges = {}

        self._working = True
        self._retry_interval = self.config.get_retry_interval()
        
        self._initial()    
        
        logger.debug('init success')
        
    def _initial(self):
        """"""
        pass
    
    @property
    def thread_name(self):
        """"""
        return 'mainloop-{}'.format(str(self.__class__))
    
    def add_exchange(self, exchange, type, **declare_config):
        """"""
        assert exchange not in self.exchanges, 'existed exchange.'
        self.exchanges[exchange] = (exchange, type, declare_config) 
    
    def _connect(self):
        """"""
        logger.debug('connecting to {}'.format(self.pika_connection_params))
        connection = pika.BlockingConnection(self.pika_connection_params)
        logger.debug('connected.')
        return connection
    
    def _get_channel(self, connection):
        """"""
        raise NotImplemented()
    
    

class RMQCEXIF(RMQCBase):
    """"""


    
    def _mainloop(self):
        """"""
        raise NotImplemented()
    
    
    def serve(self):
        """"""
        self._working = True
        logger.debug('starting serving {}.'.format(self.thread_name))
        while self._working:
            try:
                self._mainloop()
                break
            except Exception as e:
                if self._working:
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
                else:
                    break
        logger.debug('stopped serving {}.'.format(self.thread_name))
        
        print('serve.....................')
    
    def stop(self):
        """"""
        if hasattr(self, 'main_thread'):
            thd = getattr(self, 'main_thread')
            if isinstance(thd, threading.Thread):
                if thd.is_alive():
                    self._working = False
                    try:
                        self.main_thread.join()
                    except:
                        pass
    
    def serve_as_thread(self):
        """"""
        # serve in a thread
        self.main_thread = threading.Thread(
            name=self.thread_name,
            target=self.serve
        )   
        self.main_thread.daemon = True
        self.main_thread.start()    