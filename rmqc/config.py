#!/usr/bin/env python
#coding:utf-8
"""
  Author:  v1ll4n --<chaitin.cn>
  Purpose: rmqc config
  Created: 09/30/17
"""

import pika
import json
import logging

current_config = None
log_level = logging.INFO

KW_CONNECTION_EXTRA_CONFIG = 'connection_extra_config'
KW_RMQC_EXTRA_CONFIG = 'rmqc_extra_config'

_DEFAULT_CONFIG = {
    'host': '127.0.0.1',
    'port': 5672,
    'virtual_host': 'rabbitvhost',
    'username': 'guest',
    'password': 'guest',
    
    # connection extra config is the param in pika.BlockingConnection()
    # except basic params(host port virtual_host cred)
    KW_CONNECTION_EXTRA_CONFIG: {},
    
    # used by rmqc (not for pika)
    KW_RMQC_EXTRA_CONFIG: {
        'connection_retry_interval': 1,
    }
}

class RmqConfig(object):
    """"""

    #----------------------------------------------------------------------
    def __init__(self, **config):
        """Constructor"""
        self.config = None
        
        self.connection_ec = config.pop(KW_CONNECTION_EXTRA_CONFIG, {})
        assert isinstance(self.connection_ec, dict)
        
        self.rmqc_ec = config.pop(KW_RMQC_EXTRA_CONFIG, {})
        assert isinstance(self.rmqc_ec, dict)
        
        if self.config == None:
            self.config = {}
            self.config.update(_DEFAULT_CONFIG)
        
        self.config.update(config)
        if self.connection_ec:
            self.config[KW_CONNECTION_EXTRA_CONFIG] = self.connection_ec
        
        if self.rmqc_ec:
            self.config[KW_RMQC_EXTRA_CONFIG] = self.rmqc_ec
    
    def get_connection_param(self):
        """"""
        user = self.config.get('username', 'guest')
        _pass = self.config.get('password', 'guest')
        
        cred = pika.PlainCredentials(
            username=user,
            password=_pass,
        )
        
        host = self.config.get('host', '127.0.0.1')
        port = self.config.get('port', 5672)
        virtual_host = self.config.get('virtual_host', 'rabbitvhost')
        conn_ec = self.config.get(KW_CONNECTION_EXTRA_CONFIG, {})
        
        pika_param = pika.ConnectionParameters(
            host=host,
            port=port,
            virtual_host=virtual_host,
            credentials=cred,
            **conn_ec
        )
        
        return pika_param
    
    @classmethod
    def update_from(cls, host='127.0.0.1', port=5672,
                    virtual_host='rabbitvhost', username='guest',
                    password='guest', 
                    connection_extra_config={}, 
                    rmqc_extra_config={}):
        """"""
        return cls(host=host, port=port,
                   virtual_host=virtual_host,
                   username=username,
                   password=password,
                   connection_extra_config=connection_extra_config,
                   rmqc_extra_config=rmqc_extra_config)

    def get_retry_interval(self):
        """"""
        cfg = self.config.get(KW_RMQC_EXTRA_CONFIG)
        interval = cfg.get('connection_retry_interval')
        
        assert isinstance(interval, (int, float))
        return interval
    

def get_config(filename=None):
    """"""
    if not filename:
        return RmqConfig.update_from()
    
    with open(filename) as f:
        text = f.read()
        params = json.loads(text)
        assert isinstance(params, dict), 'params must be a dict from json.'
    
    return RmqConfig.update_from(**params)

current_config = get_config()
