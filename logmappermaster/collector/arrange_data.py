# -*- coding: utf-8 -*-
"""
Created on Wed Feb  7 11:05:09 2018

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
import logmappercommon.utils.logmapper_util as lmutil
import logmappermaster.dao.master_dao as masterdao
import logmappercommon.definitions.logmapperkeys as lmkey

#%%
"""
Global Initialization. Constants definitions.
"""

logger = logging.getLogger(__name__)

EMPTY_CHARACTER = np.nan #'?'
EMPTY_VALUE=0

#%%
"""
*******************************************************************************
CREA DATOS TIPO PANDA CON MATRIZ PARA PROCESAMIENTO DE ARCHIVOS
*******************************************************************************
"""



#%%
"""
*******************************************************************************

*******************************************************************************
"""

def arrangeColumns(conn):
    cursor = conn.cursor()
    df = pd.read_sql_query("SELECT * FROM lmp_measure_type", connDbMaster) 
    
    idxbase = 0

    for index, row in df[df['type'] == lmkey.DATATYPE_MONITOR_HOST].sort_values('index_in').iterrows():
        masterdao.updateIndexMeasureType(cursor, row['id'], idxbase)
        idxbase += 1

    for index, row in df[df['type'] == lmkey.DATATYPE_MONITOR_MICROSERVICE].sort_values('index_in').iterrows():
        masterdao.updateIndexMeasureType(cursor, row['id'], idxbase)
        idxbase += 1   

    for index, row in df[df['type'] == lmkey.DATATYPE_MONITOR_TOMCAT].sort_values('index_in').iterrows():
        masterdao.updateIndexMeasureType(cursor, row['id'], idxbase)
        idxbase += 1 

    for index, row in df[df['type'] == lmkey.DATATYPE_MONITOR_POSTGRES].sort_values('index_in').iterrows():
        masterdao.updateIndexMeasureType(cursor, row['id'], idxbase)
        idxbase += 1

    for index, row in df[df['type'] == lmkey.DATATYPE_LOG_METRICS].sort_values('index_in').iterrows():
        masterdao.updateIndexMeasureType(cursor, row['id'], idxbase)
        idxbase += 1         
        
    for index, row in df[df['type'] == lmkey.DATATYPE_LOG_EVENTS].sort_values('index_in').iterrows():
        masterdao.updateIndexMeasureType(cursor, row['id'], idxbase)
        idxbase += 1

    conn.commit()
    cursor.close()         


def getDataHeader(cursor, componentId):
    
    header = ["date", "period", "componentId"]
    
    types = masterdao.findMeasuresTypesByComponent(cursor, componentId)
    typelist = [r['name'] for r in types]
    header = header + typelist
    header.append('PERFORMANCE')
    header.append('PERFORMANCE_MIN')
    logger.debug("fields="+str(header))
    return header      


def getDataByComponent(cursor, componentId, hostId, start, end, ref):
    logger.debug("getDataPathsByComponent:"+str(componentId))
      
    hostTypes = masterdao.findHostMeasuresTypesByComponent(cursor, componentId)  
    mtypes = masterdao.findMeasuresTypesByComponent(cursor, componentId)
    data = [] 
    startLoop = start
    while startLoop < end:
        endLoop = startLoop + datetime.timedelta(minutes = lmutil.PERIOD)
        period = lmutil.calcTimeGroup(startLoop)

        datarow = [startLoop, period, componentId]
        datarow += getDataHost(cursor, hostId, startLoop, ref, hostTypes)
        
        #if no data then continue to next record
        #only check first column (cpu)
        if np.isnan(datarow[3]):
            startLoop = endLoop
            continue
              
        datarow += getDataEvents(cursor, componentId, startLoop, ref, mtypes)
        datarow += getDataPerformance(cursor, startLoop, componentId)
#        logger.debug("R="+str(datarow))
        data.append(datarow)         
        
        startLoop = endLoop 
    
        
    return data

#%%
def getDataHost(cursor, hostId, date, ref, hostTypes):
    data = [EMPTY_CHARACTER]*len(hostTypes)  
    row = masterdao.findHostMeasureByDate(cursor, hostId, date, ref)    
    if not row:
        return data  
    
    #only assign enabled measureTypes
    i=0
    for t in hostTypes:
        data[i] = row[t['indexIn']-1]
        i += 1
    return data 

def getDataEvents(cursor, componentId, date, ref, mtypes):
    data = []
    for m in mtypes:
        if m['type'] == lmkey.DATATYPE_MONITOR_HOST:
            continue
        value = masterdao.findMeasureValue(cursor, date, m['id'], ref)
        if value == None:
            value = EMPTY_VALUE
        data.append(value)
     
    return tuple(data)  


def getDataPerformance(cursor, date, componentId):    
    data = [EMPTY_CHARACTER]*2
    row = masterdao.findPerformanceValues(cursor, date, componentId)
    if row: return row    
    return tuple(data)  
       
       
#%%
    

    
def arrangeComponentData(cursor, componentId, hostId, start, end, ref):
    header = getDataHeader(cursor, componentId)
    data = getDataByComponent(cursor, componentId, hostId, start, end, ref)
    df=pd.DataFrame(data[1:], columns=header) 
    df.set_index('date', inplace=True)
    return df


    
if __name__ == '__main__':
    print('Start module execution:')
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')     

    cfg.createDefaultConfigfile()
    config=cfg.loadConfig()   
    
#==============================================================================
    start = datetime.datetime(2018, 4, 22, 16, 0, 0)
    end =   datetime.datetime(2018, 4, 24, 14, 0, 0)  

    start = datetime.datetime(2018, 4, 22, 16, 0, 0)
    end =   datetime.datetime(2018, 4, 28, 18, 0, 0)    
    
#==============================================================================
# Prueba
#==============================================================================    
    connDbMaster=db.connectDb()  
    cursor = connDbMaster.cursor()

#==============================================================================
# cargar datos en panda  y salvar en excel
#============================================================================== 
   
    connDbMaster=db.connectDb()
    cursor = connDbMaster.cursor() 
    
    arrangeColumns(connDbMaster)


    hosts = masterdao.findHosts(cursor)
#    hosts = [ masterdao.findHostByKey(cursor, "device") ]
    for host in hosts:
        components = masterdao.findComponentsByHostId(cursor, host['id'])
        for component in components:
            logger.debug('Process:'+str(component))  
            
            df = arrangeComponentData(cursor, component['id'], component['hostId'], start, end, ref=True)
#            df.to_sql('data_arranged', con=connDbMaster, if_exists="replace") 
            df.to_csv('/logmapper/data/arrange_'+component['key']+'.csv')
#            df.to_excel('/logmapper/data/arrange2_'+component['key']+'.xls')    
    
    connDbMaster.close()
 
    print("End module execution:"+str(datetime.datetime.now()))   