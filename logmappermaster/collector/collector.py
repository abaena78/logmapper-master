# -*- coding: utf-8 -*-
"""
Created on Wed Feb  7 08:09:52 2018

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
import json

    
import config.config as cfg
import logmappercommon.utils.postgres_util as db
import logmappercommon.utils.logmapper_util as lmutil
import logmappercommon.definitions.event_categories as cat
import logmappermaster.dao.master_dao as masterdao  
import logmappermaster.collector.agentclient as agentclient 
import logmappercommon.definitions.logmapperkeys as lmkey

#%%
"""
Global Initialization. Constants definitions.
"""

logger = logging.getLogger(__name__)


def collectDataConfig(config, conn, report):
    logger.debug('collectDataConfig:')
    
    cursor = conn.cursor() 
    
    #==========================================================================
    # 
    #==========================================================================    
    data = report.getAgent()
    data = json.loads(data)
    
    agentkey =  data['agentKey'] 
    name = data['agentKey']
    ip = data['agentIp']
    port = data['agentPort']  
    hostName = data['host']
        
    host = masterdao.findHostByKey(cursor, hostName)
    if not host:
        host = masterdao.createHost(conn, hostName, hostName)
            
    agent = masterdao.findAgentByKey(cursor, agentkey)
    
    if not agent:
        agent = masterdao.createAgent(conn, agentkey, name, host['id'], ip, port, True)

    
#%%
    #==========================================================================
    # 
    #==========================================================================            
    data = report.getReaders()
    data = json.loads(data)
    readers = data['readers']

    for item in readers:
        key = item['component']
        name = item['component']
        
        component = masterdao.findComponentByKey(cursor, key)
        if not component:
            component = masterdao.createComponent(conn, key, name, host['id'])
        
        sourcetype = lmkey.SOURCE_TYPE_READER
        key = item['readerKey']+"-"+sourcetype
        enable = item['enable']  
        
        source = masterdao.findSourceByKey(cursor, key)
        if not source:
            source = masterdao.createSource(conn, key, item['readerKey'], agent['id'], sourcetype, enable)
        
        for category in cat.getValuesLogEventCategories():
            name = category.name  
            measType = lmkey.DATATYPE_LOG_EVENTS
            
            mtype =  masterdao.findMeaureType(cursor, name, measType)
            if not mtype:
                mtype = masterdao.createMeasureType(conn, name, measType, lmkey.MEASURE_CAT_EVENT, True, category.value)  
                
            measureSource = masterdao.findMeaureSource(cursor, component['id'], mtype['id'])
            if not measureSource:
                masterdao.createMeasureSource(conn, component['id'], mtype['id'], source['id'], True)                
                
                
                
        data = report.getLogMetricsColnames(item['readerKey'])
        data = json.loads(data)
        for col in data['colnames']:
            name = col['name'] 
            index = col['idx']
            measType = data['datatype']
            category = lmkey.MEASURE_CAT_METRIC
            
            mtype =  masterdao.findMeaureType(cursor, name, measType)
            if not mtype:
                mtype = masterdao.createMeasureType(conn, name, measType, category, True, index)
                
            measureSource = masterdao.findMeaureSource(cursor, component['id'], mtype['id'])
            if not measureSource:
                masterdao.createMeasureSource(conn, component['id'], mtype['id'], source['id'], True)                
            
            
 
    #==========================================================================
    # 
    #==========================================================================            
    data = report.getMonitors()
    data = json.loads(data)
    monitors = data['monitors']

    for item in monitors:
        for componentKey in item['components'].split(','):
            component = masterdao.findComponentByKey(cursor, componentKey)
            if not component:
                component = masterdao.createComponent(conn, componentKey, componentKey, host['id'])
            
            sourcetype = item['type']
            key = item['monitorKey']+"-"+sourcetype
            enable = item['enable']  
            
            source = masterdao.findSourceByKey(cursor, key)
            if not source:        
                source = masterdao.createSource(conn, key, item['monitorKey'], agent['id'], sourcetype, enable)  
                conn.commit()
                
            data = report.getLogMonMeasuresColnames(item['monitorKey']) 
            data = json.loads(data)
            for col in data['colnames']:
                name = col['name'] 
                index = col['idx']
                measType = data['datatype']
                category = lmkey.MEASURE_CAT_METRIC
                
                mtype =  masterdao.findMeaureType(cursor, name, measType)
                if not mtype:
                    mtype = masterdao.createMeasureType(conn, name, measType, category, True, index) 
                    
                measureSource = masterdao.findMeaureSource(cursor, component['id'], mtype['id'])
                if not measureSource:
                    masterdao.createMeasureSource(conn, component['id'], mtype['id'], source['id'], True)

 
    conn.commit() 
    cursor.close() 
    
#%%
    
"""
*******************************************************************************

*******************************************************************************
"""

def collectDataMonitor(config, conn, report, source, start, end):
    logger.debug('collectDataMonitor:'+str(source))
     
    cursor = conn.cursor() 
    
    data = report.getMonMeasures(source['name'], start, end)
    data = json.loads(data)
    
    if source['name'] != data['monitorKey']:
        logger.warn("Data error:"+str(data))
        return
    
    source = masterdao.findSourceByKey(cursor, source['key'])
    if not source:
        logger.warning("source not found")
        return
    
    monitorMeasures = data['measures'] 
    hostId = source['hostId']
    sourcetype = source['type']
      
    for item in monitorMeasures:     
        date = item[0]
        period = lmutil.calcTimeGroup(lmutil.datetimeParse(date))
         
        if sourcetype == lmkey.SOURCE_TYPE_HOST: 
            masterdao.createHostMeasure(cursor, date, period, hostId, source['id'],
                                         item[1], item[2], item[3], item[4], item[5], item[6], item[7], item[8], item[9], item[10], 
                                         item[11], item[12], item[13], item[14], 
                                         item[15], item[16], 
                                         item[17], item[18], item[19], item[20]                                          
                                  )        
        else: 
            index = 0
            for metric in item:           
                measureType = masterdao.findMeaureTypeByDataTypeAndIndex(cursor, data['datatype'], index)   
                index+=1
                if not measureType:
                    logger.warning("measure type not found")
                    continue  
                if not measureType['enable']:
                    continue
                masterdao.createMeasure(cursor, date, period, measureType['id'], source['id'], metric) 
    
    conn.commit()   
    
#%%

    
def collectDataTrace(config, conn, report, source):
    logger.debug('collectDataTrace:'+str(source))
    
    cursor = conn.cursor()
    
    source = masterdao.findReaderSourceByKey(cursor, source['key'])
    readerKey = source['name']
    componentId = source['componentId']
        
#%%
    #==========================================================================
    # 
    #========================================================================== 
    #data = report.getLogNodesByCategoryTrace(readerKey)
    data = report.getLogItemsTrace(readerKey)
    data = json.loads(data)
    logNodes = data['logItems']
    
    
          
    for item in logNodes:
        
        masterKey = readerKey+"_"+str(item[0])
        logKeyId = masterdao.findLogKeyId(cursor, masterKey)

        if not logKeyId:  
            masterdao.createLogKey(cursor, masterKey, componentId, item[1], item[3], item[4], item[5], item[6], item[7], item[8]) 

    conn.commit()
    
#%%
    #==========================================================================
    # 
    #========================================================================== 

#%%
    data = report.getPaths(readerKey)
    data = json.loads(data)
    logPaths = data['logPaths']
    
    for item in logPaths:
        masterKey = readerKey+"_"+str(item[0])
        keyNode1 = readerKey+"_"+str(item[1])
        keyNode2 = readerKey+"_"+str(item[2])
        
        
        logKey1Id = masterdao.findLogKeyId(cursor, keyNode1)
        logKey2Id = masterdao.findLogKeyId(cursor, keyNode2)
        
        
        if not (logKey1Id and logKey2Id): 
            logger.warning("path creation aborted, logKeys not found")
            continue
        
        pathId = masterdao.findLogPathId(cursor, logKey1Id, logKey2Id)
        if not pathId:
            masterdao.createLogPath(cursor, masterKey, componentId, logKey1Id, logKey2Id)
        
    conn.commit()    
 
    
def collectDataReader(config, conn, report, readerKey, hostId, componentId, start, end):
    logger.debug('collectDataReader:'+str(readerKey))
    
    #TODO VERIFICAR SI ESTO FUNCIONA BIEN
    end=lmutil.getBeforeMapperIntervalDate(end)
    cursor = conn.cursor()
    
    data = report.getPathMeasures(readerKey, start, end)
    data = json.loads(data)
    pathMeasures = data['pathMeasures']


#['READER_devicetypetok_20', '2017-09-26', 102, 172.4, 204.1164045613189, 0.229, 418.314, 34.5, 54.02, 239.0, 172.4] 

    for item in pathMeasures:
        key = readerKey+"_"+str(item[2])
        pathId = masterdao.findLogPathIdByKeyMaster(cursor, key)
        
        if not pathId: 
            logger.warning("logPath not found")
            continue

        masterdao.createPathMeasure(cursor, item[1], lmutil.calcTimeGroup(lmutil.datetimeParse(item[1])), pathId, hostId, 
                          item[3], item[4], item[5], item[6])
                
    conn.commit()       
    
    
    data = report.getLogEventsCount(readerKey, start, end)
    data = json.loads(data)
    eventsCount = data['logEventsCount']
    
    
    
    source = masterdao.findSourceByKey(cursor, readerKey)
    if not source:
        logger.warning("source not found")
        return  
    
    

    #TODO SE DEBE MANEJAR LA TABLA lmp_issuesT, POR AHORA SOLO lmp_logNodesT
    for item in eventsCount:
        logger.debug(item)       
        measureType = masterdao.findMeaureTypeByDataTypeAndIndex(cursor, data['datatype'], item[2])      
        if not measureType:
            logger.warning("measure type not found")
            continue         
        masterdao.createMeasure(cursor, item[1], lmutil.calcTimeGroup(lmutil.datetimeParse(item[1])), measureType['id'], source['id'], item[3])        
    conn.commit()
    
    
    data = report.getLogMetrics(readerKey, start, end)
    data = json.loads(data)
    metrics = data['metrics']

    #TODO SE DEBE MANEJAR LA TABLA lmp_issuesT, POR AHORA SOLO lmp_logNodesT
    for item in metrics:
        logger.debug(item)
        date = item[1]
        period = lmutil.calcTimeGroup(lmutil.datetimeParse(item[1]))
        index = 0
        for metric in item:           
            measureType = masterdao.findMeaureTypeByDataTypeAndIndex(cursor, data['datatype'], index)   
            index+=1
            if not measureType:
                logger.warning("measure type not found")
                continue  
            if not measureType['enable']:
                continue
            masterdao.createMeasure(cursor, date, period, measureType['id'], source['id'], metric)        
    conn.commit()    

      
  
    
#%%
    
def collectLogRecords(config, conn, report, source, start, end):
    logger.debug('collectLogRecords:'+str(source))
    
    cursor = conn.cursor()
    
    source = masterdao.findReaderSourceByKey(cursor, source['key'])
    readerKey = source['name']
    componentId = source['componentId']    
    
    data = report.getLogRecords(readerKey, start, end)
    data = json.loads(data)
    logEvents = data['logRecords']

    #TODO SE DEBE MANEJAR LA TABLA lmp_issuesT, POR AHORA SOLO lmp_logNodesT
    for item in logEvents:
        masterKey = readerKey+"_"+str(item[1])
        logKeyId = masterdao.findLogKeyId(cursor, masterKey)
        
        if not logKeyId: continue
    
        exectime = item[0]
        remoteCallKey = item[2]
        userKey = item[3]
           
        logRecordId = masterdao.findLogRecordId(cursor, exectime, logKeyId)
        if logRecordId:
            logger.warning("record already exists")
            continue 

        masterdao.createLogRecord(cursor, exectime, logKeyId, componentId, remoteCallKey, userKey)        
                     
    conn.commit() 
    
#%%

def collectAgentsConfig(config, conn):
    cursor = conn.cursor()
    agents = masterdao.findAgentsEnabled(cursor)
    for agent in agents:
        client = agentclient.LogMapperAgentClient(agent['ip'], agent['port'])
        client.open()        
        collectDataConfig(config, conn, client)
        client.close()   
        
def collectReadersTraceData(config, conn):
    cursor = conn.cursor() 
    agents = masterdao.findAgentsEnabled(cursor)
    for agent in agents:
        client = agentclient.LogMapperAgentClient(agent['ip'], agent['port'])
        client.open() 
            
        sources = masterdao.findSourcesByAgentIdAndType(cursor, agent['id'], lmkey.SOURCE_TYPE_READER)
        for source in sources:
            try:
                collectDataTrace(config, conn, client, source)               
            except Exception as exc:
                logger.exception("Exception collecting data from "+str(source))
                
        client.close()   
                
        
def collectSourcesData(config, conn, start, end, interval=5):
    cursor = conn.cursor() 
    agents = masterdao.findAgentsEnabled(cursor) 
    for agent in agents:
        client = agentclient.LogMapperAgentClient(agent['ip'], agent['port'])
        client.open() 
            
        sources = masterdao.findSourcesByAgentIdAndType(cursor, agent['id'], lmkey.SOURCE_TYPE_READER)
        for reader in sources:
            try:
                startLoop = start
                while startLoop < end:
                    endLoop = startLoop + datetime.timedelta(minutes = interval)
                    collectDataReader(config, conn, client, reader['name'], reader['hostId'], reader['componentId'], startLoop, endLoop)
                    startLoop = endLoop                
            except Exception as exc:
                logger.exception("Exception collecting data from "+str(reader))
                
                
        sources = masterdao.findSourcesByAgentIdAndType(cursor, agent['id'], lmkey.SOURCE_TYPE_HOST)
        for source in sources:
            try:
                startLoop = start
                while startLoop < end:
                    endLoop = startLoop + datetime.timedelta(minutes = interval)
                    collectDataMonitor(config, conn, client, source, startLoop, endLoop)
                    startLoop = endLoop                
            except Exception as exc:
                logger.exception("Exception collecting data from "+str(source))    
                
                
        sources = masterdao.findSourcesByAgentIdAndType(cursor, agent['id'], lmkey.SOURCE_TYPE_SPRINGMICROSERVICE)
        for source in sources:
            try:
                startLoop = start
                while startLoop < end:
                    endLoop = startLoop + datetime.timedelta(minutes = interval)
                    collectDataMonitor(config, conn, client, source, startLoop, endLoop)
                    startLoop = endLoop                
            except Exception as exc:
                logger.exception("Exception collecting data from "+str(source))     

        sources = masterdao.findSourcesByAgentIdAndType(cursor, agent['id'], lmkey.SOURCE_TYPE_TOMCAT)
        for source in sources:
            try:
                startLoop = start
                while startLoop < end:
                    endLoop = startLoop + datetime.timedelta(minutes = interval)
                    collectDataMonitor(config, conn, client, source, startLoop, endLoop)
                    startLoop = endLoop                
            except Exception as exc:
                logger.exception("Exception collecting data from "+str(source))     

        sources = masterdao.findSourcesByAgentIdAndType(cursor, agent['id'], lmkey.SOURCE_TYPE_POSTGRES)
        for source in sources:
            try:
                startLoop = start
                while startLoop < end:
                    endLoop = startLoop + datetime.timedelta(minutes = interval)
                    collectDataMonitor(config, conn, client, source, startLoop, endLoop)
                    startLoop = endLoop                
            except Exception as exc:
                logger.exception("Exception collecting data from "+str(source))                    
                
        client.close()        
    
def collectLogRecordsData(config, conn, start, end, interval=5):
    cursor = conn.cursor() 
    agents = masterdao.findAgentsEnabled(cursor) 
    for agent in agents:
        client = agentclient.LogMapperAgentClient(agent['ip'], agent['port'])
        client.open() 
            
        readers = masterdao.findSourcesByAgentIdAndType(cursor, agent['id'], lmkey.SOURCE_TYPE_READER)
        for reader in readers:
            try:
                startLoop = start
                while startLoop < end:
                    endLoop = startLoop + datetime.timedelta(minutes = interval)
                    collectLogRecords(config, conn, client, reader, startLoop, endLoop)
                    startLoop = endLoop                
            except Exception as exc:
                logger.exception("Exception collecting data from "+str(reader))
                
        client.close()  
        
#%%
    
if __name__ == '__main__':
    print('Start module execution:')
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')     

    cfg.createDefaultConfigfile()
    config=cfg.loadConfig()   
    
#==============================================================================
    
    
#    start = datetime.datetime(2018, 3, 11, 10, 40, 0)
#    end =   datetime.datetime(2018, 3, 11, 20, 25, 0)   
         
    
#    start = datetime.datetime(2018, 3, 18, 12, 0, 0)
#    end =   datetime.datetime(2018, 3, 18, 20, 15, 0)  
    
#    start = datetime.datetime(2018, 3, 18, 20, 15, 0)
#    end =   datetime.datetime(2018, 3, 19, 8, 45, 0)   

#    start = datetime.datetime(2018, 3, 26, 15, 50, 0)
#    end =   datetime.datetime(2018, 3, 26, 22, 5, 0) 
    
#    start = datetime.datetime(2018, 4, 2, 20, 45, 0)
#    end =   datetime.datetime(2018, 4, 2, 23, 0, 0) 

    start = datetime.datetime(2018, 4, 22, 12, 35, 0)
    end =   datetime.datetime(2018, 4, 22, 12, 45,0)  
       

#==============================================================================
# crea base de datos master con datos del agente 
#==============================================================================
    conn=db.connectDb()
    masterdao.createTablesBase(conn)  
    
    try:
#        collectAgentsConfig(config, conn)
#        collectReadersTraceData(config, conn)  
#        collectLogRecordsData(config, conn, start, end, interval=5) 
        collectSourcesData(config, conn, start, end, interval=5)
    except Exception as exc:
        logger.exception("Exception collecting data")    
     
    conn.commit()
    conn.close()


    print("End module execution:"+str(datetime.datetime.now()))