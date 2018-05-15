# -*- coding: utf-8 -*-
"""
Created on Fri Feb  9 09:10:17 2018

@author: abaena
"""

#******************************************************************************
#Add logmapper-agent directory to python path for module execution
#******************************************************************************
if __name__ == '__main__':    
    import os, sys
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..','..'))) 
#******************************************************************************
    
import os
import logging
import datetime

import pandas as pd
import numpy as np
    
import config.config as cfg
import logmappercommon.utils.postgres_util as db
import logmappermaster.dao.master_dao as masterdao
import logmappermaster.transformer.tranformer as transformer
import logmappercommon.definitions.logmapperkeys as lmkey

#%%
"""
Global Initialization. Constants definitions.
"""

logger = logging.getLogger(__name__)

EMPTY_CHARACTER = np.nan #'?'
EMPTY_VALUE=0

COLNAME_PERFORMACE = 'PERFORMANCE'
COLNAME_DATE = 'date'
COLNAME_PERIOD = 'period'


#%%


def transformComponentDataDeprecated(cursor, componentId, df):
    logger.debug('transformComponentData:'+str(len(df)))
    logger.debug(str(df.columns))
    
    
    dft = pd.DataFrame({ 
            'date' : df['date'],
#            'weekday' : df['weekday'],
            'period' : df['period'],
            'componentId' : df['componentId'],
            
            'cpu' : df['MAX(cpu)'],            
            'cpu_user' : df['max(cpu_user)'],
            'cpu_sys' : df['MAX(cpu_sys)'],
            'cpu_idle' : df['MIN(cpu_idle)'],
            
            'mem' : df['MAX(mem)'],
            'swap' : df['MAX(swap)'],
            'diskusage' : df['MAX(diskusage)'],
            'pids' : df['MAX(pids)'],
            'cnxs' : df['MAX(cnxs)'],
            'users' : df['MAX(users)'],
            
            'openfiles' : df['MAX(openfiles)'],
            
            'disk_io_rate_r' : df['MAX(disk_io_rate_r)'],
            'disk_io_rate_w' : df['MAX(disk_io_rate_w)'],
            
            'net_io_rate_in' : df['MAX(net_io_rate_r)'],
            'net_io_rate_out' : df['MAX(net_io_rate_s)'],
            
            'net_err_rate_in' : df['SUM(net_err_rate_in)'],
            'net_err_rate_out' : df['SUM(net_err_rate_out)'],
            'net_drop_rate_in' : df['SUM(net_drop_rate_in)'],
            'net_drop_rate_out' : df['SUM(net_drop_rate_out)']
                        }) 
    
#    colname_host_list =[
#       'date', 'period', 'componentId',
#       'cpu', 'cpu_user', 'cpu_sys', 'cpu_idle',
#       'mem', 'swap', 'diskusage', 'pids', 'cnxs', 'users',
#       'openfiles', 'disk_io_rate_r', 'disk_io_rate_w', 
#       'net_io_rate_in', 'net_io_rate_out', 
#       'net_err_rate_in', 'net_err_rate_out', 'net_drop_rate_in', 'net_drop_rate_out'
#       ]   
#    colname_events_list = []
    

#        
#        
#    dft=pd.concat((dft, df[colname_events_list]), axis='columns')
#    dft=pd.concat((dft, df['PERFORMANCE']), axis='columns')
    


    #restore column order    
#    cols = colname_host_list+colname_events_list+['PERFORMANCE']   
    dft = dft.dropna()
#    dft = dft[cols]
    dft.set_index('date', inplace=True)
    
#==============================================================================
# Fuzzy transform
#==============================================================================     
    
    
#    cpu = df[['cpu_avg', 'cpu_idle_avg']].apply(lambda x:transformer.transformFuzzy('cpu', [('cpu', x[0]), ('cpu_idle', x[1])]), axis=1)
#    memory = df['mem_avg'].map(lambda x:transformer.transformFuzzy('memory', [('memory', x)]))
#    diskusage = df['diskusage_avg'].map(lambda x:transformer.transformFuzzy('diskusage', [('diskusage', x)]))
#    pids = df['pids_avg'].map(lambda x:transformer.transformFuzzy('pids', [('pids', x)]))
#    cnxs = df['cnxs_avg'].map(lambda x:transformer.transformFuzzy('cnxs', [('cnxs', x)]))
#    users = df['users_avg'].map(lambda x:transformer.transformFuzzy('users', [('users', x)]))
#    disk_io_rate_r = df['disk_io_rate_r_avg'].map(lambda x:transformer.transformFuzzy('disk_io_rate_r', [('disk_io_rate_r', x)]))
#    disk_io_rate_w = df['disk_io_rate_w_avg'].map(lambda x:transformer.transformFuzzy('disk_io_rate_w', [('disk_io_rate_w', x)]))
#    net_io_rate_in = df['net_io_rate_in_avg'].map(lambda x:transformer.transformFuzzy('net_io_rate_in', [('net_io_rate_in', x)]))
#    net_io_rate_out = df['net_io_rate_out_avg'].map(lambda x:transformer.transformFuzzy('net_io_rate_out', [('net_io_rate_out', x)]))
#    openfiles = df['mem_avg'].map(lambda x:transformer.transformFuzzy('memory', [('memory', x)]))
#    swap = df['mem_avg'].map(lambda x:transformer.transformFuzzy('memory', [('memory', x)]))
#    
#    N6_count = df['N6_count'].map(lambda x:transformer.transformFuzzy('counter', [('counter', x)]))
    
#    dft['cpu'] = dft['cpu'].map(lambda x:transformer.transformFuzzy('memory', [('memory', x)]))
#    dft['cpu_user'] = dft['cpu_user'].map(lambda x:transformer.transformFuzzy('memory', [('memory', x)]))
#    dft['cpu_sys'] = dft['cpu_sys'].map(lambda x:transformer.transformFuzzy('memory', [('memory', x)]))
#    dft['cpu_idle'] = dft['cpu_idle'].map(lambda x:transformer.transformFuzzy('memory', [('memory', x)]))
#    dft['mem'] = dft['mem'].map(lambda x:transformer.transformFuzzy('memory', [('memory', x)]))
#    dft['swap'] = dft['swap'].map(lambda x:transformer.transformFuzzy('memory', [('memory', x)]))
#    dft['diskusage'] = dft['diskusage'].map(lambda x:transformer.transformFuzzy('diskusage', [('diskusage', x)]))
#    dft['pids'] = dft['pids'].map(lambda x:transformer.transformFuzzy('pids', [('pids', x)]))
#    dft['cnxs'] = dft['cnxs'].map(lambda x:transformer.transformFuzzy('cnxs', [('cnxs', x)]))
#    dft['users'] = dft['users'].map(lambda x:transformer.transformFuzzy('users', [('users', x)]))
#    dft['openfiles'] = dft['openfiles'].map(lambda x:transformer.transformFuzzy('counter', [('counter', x)]))
#    dft['disk_io_rate_r'] = dft['disk_io_rate_r'].map(lambda x:transformer.transformFuzzy('counter', [('counter', x)]))
#    dft['disk_io_rate_w'] = dft['disk_io_rate_w'].map(lambda x:transformer.transformFuzzy('counter', [('counter', x)]))
#    dft['net_io_rate_in'] = dft['net_io_rate_in'].map(lambda x:transformer.transformFuzzy('counter', [('counter', x)]))
#    dft['net_io_rate_out'] = dft['net_io_rate_out'].map(lambda x:transformer.transformFuzzy('counter', [('counter', x)]))
#    dft['net_err_rate_in'] = dft['net_err_rate_in'].map(lambda x:transformer.transformFuzzy('counter', [('counter', x)]))
#    dft['net_err_rate_out'] = dft['net_err_rate_out'].map(lambda x:transformer.transformFuzzy('counter', [('counter', x)]))
#    dft['net_drop_rate_in'] = dft['net_drop_rate_in'].map(lambda x:transformer.transformFuzzy('counter', [('counter', x)]))
#    dft['net_drop_rate_out'] = dft['net_drop_rate_out'].map(lambda x:transformer.transformFuzzy('counter', [('counter', x)]))
#    
#    for colname in colname_events_list:
#        dft[colname] = dft[colname].map(lambda x:transformer.transformFuzzy('counter', [('counter', x)]))    
    
#==============================================================================
# MaxMin transform and porcentage
#==============================================================================    
    
    dft['cpu'] = dft['cpu'].map(lambda x:transformer.transformPercentage(x) )
    dft['cpu_user'] = dft['cpu_user'].map(lambda x:transformer.transformPercentage(x) )
    dft['cpu_sys'] = dft['cpu_sys'].map(lambda x:transformer.transformPercentage(x) )
    dft['cpu_idle'] = dft['cpu_idle'].map(lambda x:transformer.transformPercentage(x) )
    dft['mem'] = dft['mem'].map(lambda x:transformer.transformPercentage(x) )
    dft['swap'] = dft['swap'].map(lambda x:transformer.transformPercentage(x) )
    dft['diskusage'] = dft['diskusage'].map(lambda x:transformer.transformPercentage(x)  )
    dft['pids'] = dft['pids'].map(lambda x:transformer.transformMaxMin(x, 10000) )
    dft['cnxs'] = dft['cnxs'].map(lambda x:transformer.transformMaxMin(x, 10000) )
    dft['users'] = dft['users'].map(lambda x:transformer.transformMaxMin(x, 100) )
    dft['openfiles'] = dft['openfiles'].map(lambda x:transformer.transformMaxMin(x, 1000000) )
    dft['disk_io_rate_r'] = dft['disk_io_rate_r'].map(lambda x:transformer.transformMaxMin(x, 30000) )
    dft['disk_io_rate_w'] = dft['disk_io_rate_w'].map(lambda x:transformer.transformMaxMin(x, 30000) )
    dft['net_io_rate_in'] = dft['net_io_rate_in'].map(lambda x:transformer.transformMaxMin(x, 10000) )
    dft['net_io_rate_out'] = dft['net_io_rate_out'].map(lambda x:transformer.transformMaxMin(x, 10000) )
    dft['net_err_rate_in'] = dft['net_err_rate_in'].map(lambda x:transformer.transformMaxMin(x, 10) )
    dft['net_err_rate_out'] = dft['net_err_rate_out'].map(lambda x:transformer.transformMaxMin(x, 10) )
    dft['net_drop_rate_in'] = dft['net_drop_rate_in'].map(lambda x:transformer.transformMaxMin(x, 10) )
    dft['net_drop_rate_out'] = dft['net_drop_rate_out'].map(lambda x:transformer.transformMaxMin(x, 10) )
    
#    for colname in colname_events_list:
#        dft[colname] = dft[colname].map(lambda x:transformer.transformMaxMin(x, 60) )
        
#==============================================================================
# return dataframe transformed
#==============================================================================        
    
    return dft

    
    
def getColumnsByCategoryAndType(cTypes, category, mType):
    colnames = []
    for cType in cTypes:
        if category != None and mType != None:
            if cType['category'] == category and cType['type'] == mType:
                colnames.append(cType['name'])
        elif category != None:
            if cType['category'] == category:
                colnames.append(cType['name'])      
        elif mType != None:
            if cType['type'] == mType:
                colnames.append(cType['name']) 
        else:
            colnames.append(cType['name'])
    return colnames
        


def configureMeasureTypes(conn):
    cursor = conn.cursor()

    #==========================================================================
    # Host metrics
    #==========================================================================

    measureType = masterdao.findMeaureType(cursor, 'MAX(cpu)', lmkey.DATATYPE_MONITOR_HOST)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_PERCENTAGE, True) 
    
    measureType = masterdao.findMeaureType(cursor, 'max(cpu_user)', lmkey.DATATYPE_MONITOR_HOST)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_PERCENTAGE, True)  

    measureType = masterdao.findMeaureType(cursor, 'MAX(cpu_sys)', lmkey.DATATYPE_MONITOR_HOST)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_PERCENTAGE, True)  

    measureType = masterdao.findMeaureType(cursor, 'MIN(cpu_idle)', lmkey.DATATYPE_MONITOR_HOST)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_PERCENTAGE, True)  
    
    measureType = masterdao.findMeaureType(cursor, 'MAX(mem)', lmkey.DATATYPE_MONITOR_HOST)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_PERCENTAGE, True)  
#    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_FUZZY_1, True)  
    
    measureType = masterdao.findMeaureType(cursor, 'MAX(swap)', lmkey.DATATYPE_MONITOR_HOST)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_PERCENTAGE, True)  

    measureType = masterdao.findMeaureType(cursor, 'MAX(diskusage)', lmkey.DATATYPE_MONITOR_HOST)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_PERCENTAGE, True)  
#    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_FUZZY_1, True) 

    measureType = masterdao.findMeaureType(cursor, 'MAX(pids)', lmkey.DATATYPE_MONITOR_HOST)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_MINMAX, True)  

    measureType = masterdao.findMeaureType(cursor, 'MAX(cnxs)', lmkey.DATATYPE_MONITOR_HOST)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_MINMAX, True)   

    measureType = masterdao.findMeaureType(cursor, 'MAX(disk_io_rate_w)', lmkey.DATATYPE_MONITOR_HOST)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_MINMAX, True) 

    measureType = masterdao.findMeaureType(cursor, 'MAX(disk_io_rate_r)', lmkey.DATATYPE_MONITOR_HOST)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_MINMAX, True)
    
    measureType = masterdao.findMeaureType(cursor, 'MAX(net_io_rate_r)', lmkey.DATATYPE_MONITOR_HOST)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_MINMAX, True)    

    measureType = masterdao.findMeaureType(cursor, 'MAX(net_io_rate_s)', lmkey.DATATYPE_MONITOR_HOST)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_MINMAX, True)

    measureType = masterdao.findMeaureType(cursor, 'MAX(openfiles)', lmkey.DATATYPE_MONITOR_HOST)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_MINMAX, True)     

       
    #==========================================================================
    # Host events
    #========================================================================== 
    
    measureType = masterdao.findMeaureType(cursor, 'MAX(users)', lmkey.DATATYPE_MONITOR_HOST)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_EVENT, lmkey.TRANSF_TYPE_MINMAX, False)
    
    measureType = masterdao.findMeaureType(cursor, 'SUM(net_err_rate_in)', lmkey.DATATYPE_MONITOR_HOST)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_EVENT, lmkey.TRANSF_TYPE_MINMAX, True)
    
    measureType = masterdao.findMeaureType(cursor, 'SUM(net_err_rate_out)', lmkey.DATATYPE_MONITOR_HOST)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_EVENT, lmkey.TRANSF_TYPE_MINMAX, True)

    measureType = masterdao.findMeaureType(cursor, 'SUM(net_drop_rate_in)', lmkey.DATATYPE_MONITOR_HOST)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_EVENT, lmkey.TRANSF_TYPE_MINMAX, True)
    
    measureType = masterdao.findMeaureType(cursor, 'SUM(net_drop_rate_out)', lmkey.DATATYPE_MONITOR_HOST)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_EVENT, lmkey.TRANSF_TYPE_MINMAX, True)  
       
    measureType = masterdao.findMeaureType(cursor, 'SUM(net_drop_rate_out)', lmkey.DATATYPE_MONITOR_HOST)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_EVENT, lmkey.TRANSF_TYPE_MINMAX, True) 

    #==========================================================================
    # Host disabled
    #==========================================================================
    measureType = masterdao.findMeaureType(cursor, 'max(openfiles_rate)', lmkey.DATATYPE_MONITOR_HOST)    
    masterdao.updateMeasureType(cursor, measureType['id'], measureType['category'], lmkey.TRANSF_TYPE_MINMAX, False)  
    
    #==========================================================================
    # Microservice metrics
    #==========================================================================
    measureType = masterdao.findMeaureType(cursor, 'MAX(memused)', lmkey.DATATYPE_MONITOR_MICROSERVICE)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_MINMAX, True) 

    measureType = masterdao.findMeaureType(cursor, 'MAX(heapused)', lmkey.DATATYPE_MONITOR_MICROSERVICE)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_MINMAX, True) 

    measureType = masterdao.findMeaureType(cursor, 'MAX(nonheapused)', lmkey.DATATYPE_MONITOR_MICROSERVICE)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_MINMAX, True) 

    measureType = masterdao.findMeaureType(cursor, 'MAX(threads)', lmkey.DATATYPE_MONITOR_MICROSERVICE)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_MINMAX, True) 

    measureType = masterdao.findMeaureType(cursor, 'MAX(sessions)', lmkey.DATATYPE_MONITOR_MICROSERVICE)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_MINMAX, True)   
    
    #==========================================================================
    # Microservice events
    #==========================================================================
    
    measureType = masterdao.findMeaureType(cursor, 'MAX(classes)', lmkey.DATATYPE_MONITOR_MICROSERVICE)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_EVENT, lmkey.TRANSF_TYPE_MINMAX, True) 

    measureType = masterdao.findMeaureType(cursor, 'COUNT(fail)', lmkey.DATATYPE_MONITOR_MICROSERVICE)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_EVENT, lmkey.TRANSF_TYPE_MINMAX, True)    

    #==========================================================================
    # Microservice disabled
    #==========================================================================

    measureType = masterdao.findMeaureType(cursor, 'MAX(dsconn)', lmkey.DATATYPE_MONITOR_MICROSERVICE)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_MINMAX, False) 
    
    #==========================================================================
    # Postgres
    #==========================================================================
    
    measureType = masterdao.findMeaureType(cursor, 'MAX(conns)', lmkey.DATATYPE_MONITOR_POSTGRES)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_MINMAX, True) 

    measureType = masterdao.findMeaureType(cursor, 'MAX(locks)', lmkey.DATATYPE_MONITOR_POSTGRES)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_EVENT, lmkey.TRANSF_TYPE_MINMAX, True)     
    
    measureType = masterdao.findMeaureType(cursor, 'COUNT(fail)', lmkey.DATATYPE_MONITOR_POSTGRES)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_EVENT, lmkey.TRANSF_TYPE_MINMAX, True) 

    #==========================================================================
    # Tomcat
    #==========================================================================
    
    measureType = masterdao.findMeaureType(cursor, 'MAX(memused)', lmkey.DATATYPE_MONITOR_TOMCAT)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_MINMAX, True) 

    measureType = masterdao.findMeaureType(cursor, 'MAX(threads)', lmkey.DATATYPE_MONITOR_TOMCAT)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_MINMAX, True)  

    measureType = masterdao.findMeaureType(cursor, 'MAX(threadsBusy)', lmkey.DATATYPE_MONITOR_TOMCAT)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_MINMAX, True)  

    measureType = masterdao.findMeaureType(cursor, 'MAX(workers)', lmkey.DATATYPE_MONITOR_TOMCAT)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_MINMAX, True)     
    
    measureType = masterdao.findMeaureType(cursor, 'COUNT(fail)', lmkey.DATATYPE_MONITOR_TOMCAT)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_EVENT, lmkey.TRANSF_TYPE_MINMAX, True)     
    
    #==========================================================================
    # log
    #==========================================================================

    measureType = masterdao.findMeaureType(cursor, 'logCount', lmkey.DATATYPE_LOG_METRICS)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_MINMAX, True)   
    
    measureType = masterdao.findMeaureType(cursor, 'logTraceCount', lmkey.DATATYPE_LOG_METRICS)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_METRIC, lmkey.TRANSF_TYPE_MINMAX, True) 
    
    
    #==========================================================================
    # log events
    #==========================================================================    

    measureType = masterdao.findMeaureType(cursor, 'logEventsCriticalCount', lmkey.DATATYPE_LOG_METRICS)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_EVENT, lmkey.TRANSF_TYPE_MINMAX, True)  

    measureType = masterdao.findMeaureType(cursor, 'logEventsErrorCount', lmkey.DATATYPE_LOG_METRICS)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_EVENT, lmkey.TRANSF_TYPE_MINMAX, True)

    measureType = masterdao.findMeaureType(cursor, 'logEventsWarningCount', lmkey.DATATYPE_LOG_METRICS)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_EVENT, lmkey.TRANSF_TYPE_MINMAX, True) 



    measureType = masterdao.findMeaureType(cursor, 'TRACE_BOOT', lmkey.DATATYPE_LOG_EVENTS)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_EVENT, lmkey.TRANSF_TYPE_MINMAX, True)

    measureType = masterdao.findMeaureType(cursor, 'ERROR', lmkey.DATATYPE_LOG_EVENTS)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_EVENT, lmkey.TRANSF_TYPE_MINMAX, True)

    measureType = masterdao.findMeaureType(cursor, 'DATA_ERROR', lmkey.DATATYPE_LOG_EVENTS)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_EVENT, lmkey.TRANSF_TYPE_MINMAX, True)    
    
    measureType = masterdao.findMeaureType(cursor, 'VIEW_ERROR', lmkey.DATATYPE_LOG_EVENTS)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_EVENT, lmkey.TRANSF_TYPE_MINMAX, True)

    measureType = masterdao.findMeaureType(cursor, 'DB_ERROR', lmkey.DATATYPE_LOG_EVENTS)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_EVENT, lmkey.TRANSF_TYPE_MINMAX, True)

    measureType = masterdao.findMeaureType(cursor, 'AUTH_ERROR', lmkey.DATATYPE_LOG_EVENTS)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_EVENT, lmkey.TRANSF_TYPE_MINMAX, True) 

    measureType = masterdao.findMeaureType(cursor, 'NET_ERROR', lmkey.DATATYPE_LOG_EVENTS)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_EVENT, lmkey.TRANSF_TYPE_MINMAX, True)

    measureType = masterdao.findMeaureType(cursor, 'WARNING', lmkey.DATATYPE_LOG_EVENTS)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_EVENT, lmkey.TRANSF_TYPE_MINMAX, True)

    measureType = masterdao.findMeaureType(cursor, 'EVENT_BOOT', lmkey.DATATYPE_LOG_EVENTS)    
    masterdao.updateMeasureType(cursor, measureType['id'], lmkey.MEASURE_CAT_EVENT, lmkey.TRANSF_TYPE_MINMAX, True)     
     
    conn.commit()
    
def updateTransformParameters(conn, mTypes, df):
    cursor = conn.cursor()
    for mType in mTypes:
        if mType['transfType'] == lmkey.TRANSF_TYPE_MINMAX:
            k1 = 0
            k2 = df.max()[mType['name']] * 2
            if k2 == 0 : k2 = 10
            masterdao.updateMeasureSource(cursor, mType['measureSourceId'], True, k1, k2)
        else:
            masterdao.updateMeasureSource(cursor, mType['measureSourceId'], True, None, None)
    conn.commit()
    
    
def transformMeasure(colSerie, mType):
#    logger.debug("transformMeasure:"+str(mType))    
    if mType['transfType'] == lmkey.TRANSF_TYPE_MINMAX:
        colSerieT = colSerie.map(lambda x:transformer.transformMaxMin(x, mType['k2']) )
    elif mType['transfType'] == lmkey.TRANSF_TYPE_PERCENTAGE:
        colSerieT = colSerie.map(lambda x:transformer.transformPercentage(x) )
    elif mType['transfType'] == lmkey.TRANSF_TYPE_FUZZY_1:
        colSerieT = colSerie.map(lambda x:transformer.transformFuzzy('memory', [('memory', x)]))        
        
        
    else:
        colSerieT = colSerie
    
    return colSerieT

def selectAndTransformAll(df, mTypes):
    colnames = getColumnsByCategoryAndType(mTypes, None, None)
    for mType in mTypes:
        if mType['name'] in colnames:
            df[mType['name']] = transformMeasure(df[mType['name']], mType)
       
    colnames.append(COLNAME_PERFORMACE)
    return df[colnames].dropna()

def selectAndTransformMetricsAndEvents(df, mTypes):
    colnames = getColumnsByCategoryAndType(mTypes, None, None)
    for mType in mTypes:
        if mType['name'] in colnames:
            df[mType['name']] = transformMeasure(df[mType['name']], mType)
       
    return df[colnames].dropna()

def selectAndTransformMetrics(df, mTypes):
    colnames = getColumnsByCategoryAndType(mTypes, lmkey.MEASURE_CAT_METRIC, None)
    for mType in mTypes:
        if mType['name'] in colnames:
            df[mType['name']] = transformMeasure(df[mType['name']], mType)
        
    return df[colnames].dropna()

def selectAndTransformMetricsAndPerformance2(df, mTypes):
    colnames = getColumnsByCategoryAndType(mTypes, lmkey.MEASURE_CAT_METRIC, None)
    for mType in mTypes:
        if mType['name'] in colnames:
            df[mType['name']] = transformMeasure(df[mType['name']], mType)
        
    colnames.append(COLNAME_PERFORMACE)
    return df[colnames]

def selectAndTransformMetricsAndPerformance(df, mTypes):
    colnames = getColumnsByCategoryAndType(mTypes, lmkey.MEASURE_CAT_METRIC, None)
    for mType in mTypes:
        if mType['name'] in colnames:
            df[mType['name']] = transformMeasure(df[mType['name']], mType)
        
    colnames.append(COLNAME_PERFORMACE)
    return df[colnames].dropna()

def selectAndTransformEvents(df, mTypes):
    colnames = getColumnsByCategoryAndType(mTypes, lmkey.MEASURE_CAT_EVENT, None)
#    df = df.dropna()
    for mType in mTypes:
        if mType['name'] in colnames:
            df[mType['name']] = transformMeasure(df[mType['name']], mType)
       
    return df[colnames].dropna()

    
    

#%%

if __name__ == '__main__':
    print('Start module execution:')
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')     

    cfg.createDefaultConfigfile()
    config=cfg.loadConfig() 
    
#==============================================================================
    
#    start = datetime.datetime(2018,3, 18, 12, 0, 0)
#    end = datetime.datetime(2018, 4, 2, 21, 40, 0)     
    
#==============================================================================
    connDbMaster=db.connectDb()
    
    configureMeasureTypes(connDbMaster)
     
    cursor = connDbMaster.cursor() 
    hosts = masterdao.findHosts(cursor)
#    hosts = [ masterdao.findHostByKey(cursor, "web") ]   
    for host in hosts:
        components = masterdao.findComponentsByHostId(cursor, host['id'])
        for component in components:
            logger.debug('Process:'+str(component))   
            
            mTypes = masterdao.findMeasuresTypesByComponent(cursor, component['id'])           
            df = pd.read_csv('/logmapper/data/arrange_'+component['key']+'.csv')
            df.set_index('date', inplace=True)
            updateTransformParameters(connDbMaster, mTypes, df)
            
#            dft = selectAndTransformAll(df, mTypes)
#            dft = selectAndTransformMetrics(df, mTypes)
#            dft = selectAndTransformEvents(df, mTypes)
#            print(dft)

    connDbMaster.close()
 
    print("End module execution:"+str(datetime.datetime.now()))  