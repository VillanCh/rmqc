#!/usr/bin/env python
#coding:utf-8
"""
  Author:  v1ll4n --<chaitin>
  Purpose: Main Entry
  Created: 09/30/17
"""

# define logging config
import logging
import sys

root_logger = logging.getLogger('root')

# fmt and hdlr
fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s : %(message)s')
hdlr = logging.StreamHandler(sys.stdout)
hdlr.setFormatter(fmt)

# add hdlr to root logger
root_logger.addHandler(hdlr)
root_logger.setLevel(logging.DEBUG)
