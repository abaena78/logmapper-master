# -*- coding: utf-8 -*-
"""
Created on Sun Sep 17 08:16:42 2017

@author: abaena
"""

import sys
import logging
from logging.handlers import RotatingFileHandler


def configureLogger(loggerfilepath):
    """
    Configuracion general del logger
    """
    
    ch = logging.StreamHandler(sys.stdout)
    ch.set_name("logmapper")
    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    for handler in root.handlers:
        root.removeHandler(handler)
    root.addHandler(ch) 
    
    fileHandler = logging.FileHandler(loggerfilepath)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fileHandler.setFormatter(formatter)
#    root.addHandler(fileHandler)  
    
    rotateHandler = RotatingFileHandler(loggerfilepath, maxBytes=10000000, backupCount=2)
    rotateHandler.set_name('rotateHandler')
    rotateHandler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')) 
    rotateHandler.setLevel(logging.INFO)
    root.addHandler(rotateHandler)    
    
    
#%%
"""
*******************************************************************************
Module Execution
This code helps to developer to know the usage of the module
*******************************************************************************
"""
if __name__ == '__main__':    
    print('Start module execution:')
    configureLogger('/tmp/log.log')
    
    logger = logging.getLogger(__name__)
    
    logger.info("start logger test")
    logger.debug("debug message")
    logger.info("info message")
    logger.warning("warning message")
    logger.error("error message")
    logger.critical("critical message")
    
    try:
        2/0
    except Exception as exc:
        logger.exception("exception message")