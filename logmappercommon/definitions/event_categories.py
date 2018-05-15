#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 25 14:06:39 2018

@author: abaena
"""

from enum import Enum


#LOG EVENT NUMERICAR RANGES
#TRACE
EV_TRACE_MIN=10
EV_TRACE_MAX=19
#CRITICAL
EV_CRIT_MIN=40
EV_CRIT_MAX=49
#ERROR
EV_ERR_MIN=50
EV_ERR_MAX=79
#WARNING
EV_WARN_MIN=80
EV_WARN_MAX=99

class LogEventCategories(Enum):
    NONE = 0
    
#---EV_TRACE_MIN=10
    TRACE_NODE = 10    
    TRACE_MAIN_NODE = 11
    TRACE_BOOT = 12    
#---EV_TRACE_MAX=19
    
  
#---EV_CRIT_MIN=40
    CRITICAL = 12
#---EV_CRIT_MAX=49    
    
#---EV_ERR_MIN=50
    ERROR = 50
    DATA_ERROR = 51
    VIEW_ERROR = 52
    DB_ERROR = 53
    AUTH_ERROR = 54
    NET_ERROR = 55    
#---EV_ERR_MAX=79  
    
#---EV_WARN_MIN=80
    WARNING = 80   
    EVENT_BOOT = 81    
#---EV_WARN_MAX=99



    
        
    
def getValuesLogEventCategories():
    return [
         LogEventCategories.EVENT_BOOT,
         
         LogEventCategories.CRITICAL,
         LogEventCategories.WARNING ,         
         
         LogEventCategories.ERROR,
         LogEventCategories.DATA_ERROR,
         LogEventCategories.VIEW_ERROR,
         LogEventCategories.DB_ERROR,
         LogEventCategories.AUTH_ERROR,
         LogEventCategories.NET_ERROR
    ]         

class ComponentEventCategories(Enum):
    LOGRECORDS = 0
    THREADS = 1
    
def getValuesComponentEventCategories():
    return [
         ComponentEventCategories.LOGRECORDS,
         ComponentEventCategories.THREADS
    ]      
    
class HostEventCategories(Enum):
    UNKNOWN = 0
    ERROR = 1
    CRITICAL = 2
    WARNING = 3
    KNOWN_ERROR = 4
    KNOWN_WARN = 5
    HARD_ERROR = 6
    SECURITY_WARN = 7
    INIT_ERROR = 8
    NET_ERROR = 9
    
def getValuesHostEventCategories():
    return [
         HostEventCategories.UNKNOWN,
         HostEventCategories.ERROR,
         HostEventCategories.CRITICAL,
         HostEventCategories.WARNING,
         HostEventCategories.KNOWN_ERROR,
         HostEventCategories.KNOWN_WARN,
         HostEventCategories.HARD_ERROR,
         HostEventCategories.SECURITY_WARN,
         HostEventCategories.INIT_ERROR,
         HostEventCategories.NET_ERROR
    ]        
    
    
      