# -*- coding: utf-8 -*-
"""
Created on Wed Sep 27 04:34:08 2017

@author: abaena
"""

#******************************************************************************
#Add logmapper-agent directory to python path for module execution
#******************************************************************************
if __name__ == '__main__':    
    import os, sys
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..','..'))) 
#******************************************************************************

import logmappercommon.definitions.logmapperkeys as lmkey

import logging
import datetime


logger = logging.getLogger(__name__)

#%%

def createTablesBase(conn):
    """
    Create tables mapper logmapper
    """    
    logger.debug("createTablesBase")
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lmp_host
        (
          id SERIAL PRIMARY KEY,
          key TEXT UNIQUE,
          name TEXT
        )
    ''') 
        
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lmp_agent
        (
          id SERIAL PRIMARY KEY,
          key TEXT UNIQUE,
          name TEXT,
          host_id INTEGER,          
          ip TEXT,
          port INTEGER,
          enable BOOLEAN
        )
    ''')         
        
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lmp_component
        (
          id SERIAL PRIMARY KEY,
          key TEXT UNIQUE,
          name TEXT,
          host_id INTEGER,
          x REAL,
          y REAL,
          x_label REAL,
          y_label REAL
        )
    ''') 
        
        

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lmp_source
        (
          id SERIAL PRIMARY KEY,
          key TEXT UNIQUE,
          name TEXT,
          agent_id INTEGER,
          sourcetype TEXT,
          enable BOOLEAN
        )
    ''')    
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lmp_logkey
        (
          id SERIAL PRIMARY KEY,
          keyMaster TEXT UNIQUE NOT NULL,
          component_id INTEGER,
          key TEXT,          
          className TEXT,
          method TEXT,
          lineNumber TEXT,
          logLevel TEXT,
          text TEXT,
          extra TEXT,          
          category TEXT,
          data_index INTEGER,
          count INTEGER
        )
    ''')
        
        
        
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS index_logkey_key ON lmp_logkey(keyMaster);
    ''')          
       
      
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lmp_logpath
        (
          id SERIAL PRIMARY KEY,
          keyMaster TEXT UNIQUE NOT NULL,
          logkey1_id INTEGER NOT NULL,
          logkey2_id INTEGER NOT NULL, 
          component_id INTEGER NOT NULL,
          data_index INTEGER,
          samples_count INTEGER,
          duration_avg REAL,
          duration_std REAL,
          duration_max REAL,
          samples_start TIMESTAMP,
          samples_end TIMESTAMP
        )
    ''') 
        
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS index_logpath_key ON lmp_logpath (keyMaster);
    ''')         
        
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lmp_path_measure
        (
          id SERIAL PRIMARY KEY,
          date TIMESTAMP NOT NULL,
          period INTEGER NOT NULL,
          ref BOOLEAN DEFAULT FALSE,
          path_id INTEGER NOT NULL, 
          host_id INTEGER NOT NULL,
          count INTEGER,
          duration_avg REAL,
          duration_std REAL,
          duration_max REAL,
          performance REAL
        )
    ''')   
        
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS index_path_measure_date ON lmp_path_measure (date);
    ''') 

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS index_path_measure_path_id ON lmp_path_measure (path_id);
    ''')         
        
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lmp_host_measure
        (
          id SERIAL PRIMARY KEY,
          date TIMESTAMP NOT NULL,
          period INTEGER NOT NULL,
          host_id INTEGER NOT NULL,
          source_id INTEGER NOT NULL,
          ref BOOLEAN DEFAULT FALSE,
          
          cpu REAL,
          cpu_user REAL,
          cpu_sys REAL,
          cpu_idle REAL,          
          mem REAL,
          swap REAL,
          diskusage REAL,
          pids REAL,
          cnxs REAL,
          users REAL,
          
          disk_io_rate_w REAL,
          disk_io_rate_r REAL,
          
          net_io_rate_in REAL,
          net_io_rate_out REAL,

          openfiles REAL,
          openfiles_rate REAL,          
          
          net_err_rate_in REAL,
          net_err_rate_out REAL,
          net_drop_rate_in REAL,
          net_drop_rate_out REAL
        )
    ''') 
        
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS index_host_measure_date ON lmp_host_measure(date);
    ''') 

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS index_host_measure_host_id ON lmp_host_measure(host_id);
    ''')   

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS index_host_measure_source_id ON lmp_host_measure(source_id);
    ''')        
             
        
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lmp_measure_type
        (
          id SERIAL PRIMARY KEY,
          name TEXT,
          type TEXT,
          description TEXT,
          enable BOOLEAN,
          index_in INTEGER,
          index_out INTEGER,
          category INTEGER,
          transf_type INTEGER
        )
    ''')        
        
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lmp_measure
        (
          id SERIAL PRIMARY KEY,
          date TIMESTAMP,
          period INTEGER,
          ref BOOLEAN DEFAULT FALSE,
          type_id INTEGER NOT NULL, 
          source_id INTEGER NOT NULL,
          value REAL
        )
    ''') 
        
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS index_measure_date ON lmp_measure(date);
    ''') 

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS index_measure_type_id ON lmp_measure(type_id);
    ''') 
    
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS index_measure_source_id ON lmp_measure(source_id);
    ''')     
    
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lmp_measure_source
        (
          id SERIAL PRIMARY KEY,
          component_id INTEGER NOT NULL,
          type_id INTEGER NOT NULL,
          source_id INTEGER NOT NULL,
          enable BOOLEAN,
          k1 REAL,
          k2 REAL
        )
    ''')  
        
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS index_measure_source_component_id ON lmp_measure_source(component_id);
    ''') 

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS index_measure_source_type_id ON lmp_measure_source(type_id);
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lmp_measures_statistics
        (
          id SERIAL PRIMARY KEY,
          type_id INTEGER NOT NULL,
          samples_count INTEGER,
          count_avg REAL,
          count_std REAL,
          count_max REAL,
          samples_start TIMESTAMP,
          samples_end TIMESTAMP          
        )
    ''') 

   
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lmp_logrecord
        (
          id SERIAL PRIMARY KEY,
          exectime TIMESTAMP NOT NULL,
          logkey_id INTEGER NOT NULL, 
          component_id INTEGER NOT NULL,          
          remoteCallKey TEXT, 
          userKey TEXT,
          detail TEXT
        )
    ''') 
        
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS index_logrecord_exectime ON lmp_logrecord(exectime);
    ''') 

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS index_logrecord_logkey_id ON lmp_logrecord(logkey_id);
    ''') 
        
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lmp_remotecall
        (
          id SERIAL PRIMARY KEY,
          logkey1_id INTEGER NOT NULL,
          logkey2_id INTEGER NOT NULL
        )
    ''')  
        
        
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lmp_result
        (
          id SERIAL PRIMARY KEY,
          date TIMESTAMP NOT NULL,
          period INTEGER NOT NULL,
          host_id INTEGER NOT NULL,
          component_id INTEGER,         
          performance REAL,
          performance_min REAL,
          g1avg REAL,
          g2avg REAL,
          g3avg REAL,
          g4avg REAL,
          g1min REAL,
          g2min REAL,
          g3min REAL,
          g4min REAL,          
          predicted REAL,
          anomaly INTEGER
        )
    ''') 
        
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS index_result_date ON lmp_result(date);
    ''')  

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS index_result_component_id ON lmp_result(component_id);
    ''') 

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS index_result_host_id ON lmp_result(host_id);
    ''') 
    
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lmp_sourcecounters
        (
          id SERIAL PRIMARY KEY,
          date TIMESTAMP NOT NULL,
          source_id INTEGER NOT NULL,         
          count INTEGER,
          bytes INTEGER,
          records INTEGER,
          fails INTEGER
        )
    ''')  
        
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS index_sourcecounters_date ON lmp_sourcecounters(date);
    ''')  

    cursor.execute('''
        CREATE INDEX IF NOT EXISTS index_sourcecounters_source_id ON lmp_sourcecounters(source_id);
    ''')         
        
         
    conn.commit() 
    cursor.close()
    
#%%
    
def findHostByKey(cursor, key):
    logger.debug("findHostByKey:"+key)
    cursor.execute("SELECT id, key, name FROM lmp_host WHERE key = %s;", (key, ))
    row = cursor.fetchone()     
    if not row: return None
    return {'id' : row[0], 
            'key' : row[1], 
            'name' : row[2]
            }

def createHost(conn, key, name):
    logger.debug("createHost:"+key)  
    cursor = conn.cursor()
    cursor.execute(
    """
    INSERT INTO lmp_host(key, name) 
    VALUES (%s, %s)
    """
    , (key, name)
    )
    conn.commit()
    item = findHostByKey(cursor, key)
    cursor.close()
    return item
    
#%%
    

    
def findAgentByKey(cursor, key):
    logger.debug("findAgentByKey:"+key)
    cursor.execute("""
                   SELECT 
                    lmp_agent.id,
                    lmp_agent.key,
                    lmp_agent.host_id as hostId,
                    lmp_agent.ip as ip,
                    lmp_agent.port as port,
                    lmp_host.name as hostName
                   FROM lmp_agent 
                   INNER JOIN lmp_host ON lmp_agent.host_id = lmp_host.id
                   WHERE lmp_agent.key = %s;
                   """, (key, ))
    row = cursor.fetchone()     
    if not row: return None
    return {
            'id' : row[0], 
            'key' : row[1], 
            'hostId' : row[2],
            'ip' : row[3],
            'port' : row[4],
            'hostName' : row[5]
            }

def createAgent(conn, key, name, hostId, ip, port, enable):
    logger.debug("createAgent:"+key)  
    cursor = conn.cursor()
    cursor.execute(
    """
    INSERT INTO lmp_agent(key, name, host_id, ip, port, enable) 
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    , (key, name, hostId, ip, port, enable)
    )
    conn.commit()
    item = findAgentByKey(cursor, key)
    cursor.close()
    return item

#%%
    
def findComponentByKey(cursor, key):
    logger.debug("findComponentByKey:"+key)
    cursor.execute("""
                   SELECT 
                    lmp_component.id,
                    lmp_component.key,
                    lmp_component.name,
                    lmp_component.host_id as hostId,
                    lmp_host.name as hostName
                   FROM lmp_component 
                   INNER JOIN lmp_host ON lmp_component.host_id = lmp_host.id
                   WHERE lmp_component.key = %s;
                   """, (key, ))
    row = cursor.fetchone()     
    if not row: return None
    return {
            'id' : row[0], 
            'key' : row[1], 
            'name' : row[2],
            'hostId' : row[3],
            'hostName' : row[4]
            }

def createComponent(conn, key, name, hostId):
    logger.debug("createComponent:"+key)  
    cursor = conn.cursor()
    cursor.execute(
    """
    INSERT INTO lmp_component(key, name, host_id) 
    VALUES (%s, %s, %s)
    """
    , (key, name, hostId)
    )
    conn.commit()
    item = findComponentByKey(cursor, key)
    cursor.close()
    return item


#%%
    
def findSourceByKey(cursor, key):
    logger.debug("findSourceByKey:"+key)
    cursor.execute("""
       SELECT 
        lmp_source.id,
        lmp_source.key,
        lmp_source.name,
        lmp_source.agent_id,
        lmp_source.sourcetype,
        lmp_source.enable,
        lmp_host.id as hostId
       FROM lmp_source
        INNER JOIN lmp_agent ON lmp_source.agent_id = lmp_agent.id
        INNER JOIN lmp_host ON lmp_agent.host_id = lmp_host.id                   
        WHERE lmp_source.key = %s;
        """, (key, ))
    row = cursor.fetchone()     
    if not row: return None
    return {'id' : row[0], 
            'key' : row[1], 
            'name' : row[2],
            'agentId' : row[3],
            'type' : row[4],
            'enable' : row[5],
            'hostId' : row[6],
            }

def createSource(conn, key, name, agentId, sourcetype, enable):
    logger.debug("createSource:"+key)  
    cursor = conn.cursor()
    cursor.execute(
    """
    INSERT INTO lmp_source(key, name, agent_id, sourcetype, enable) 
    VALUES (%s, %s, %s, %s, %s)
    """
    , (key, name, agentId, sourcetype, enable)
    )  
    conn.commit()
    item = findSourceByKey(cursor, key)
    cursor.close()
    return item
 
#%%
    
def findMeaureType(cursor, name, measuretype):
    logger.debug("findMeaureType:"+name+"-"+measuretype)
    cursor.execute("""
                   SELECT 
                   id,
                   name,
                   type,
                   description,
                   category,
                   transf_type
                   FROM lmp_measure_type 
                   WHERE name = %s AND type = %s
                   """, (name, measuretype ))
    row = cursor.fetchone()     
    if not row: return None
    return {'id' : row[0], 
            'name' : row[1], 
            'type' : row[2],
            'description' : row[3],
            'category' : row[4],
            'transfType' : row[5]
            }
    
def findAllMeaureType(cursor):
    logger.debug("findAllMeaureType:")
    cursor.execute("""
                   SELECT 
                   id,
                   name,
                   type,
                   description,
                   category,
                   enable
                   FROM lmp_measure_type 
                   """)
    rows = cursor.fetchall()     
    rowsdict = []
    for row in rows:
        rowsdict.append(
                {'id' : row[0], 
                'name' : row[1], 
                'type' : row[2],
                'description' : row[3],
                'category' : row[4],
                'enable' : row[5]
                }
                )   
    return rowsdict

    
def findMeaureTypeByDataTypeAndIndex(cursor, datatype, index):
    logger.debug("findMeaureType:"+datatype+":"+str(index))
    cursor.execute("""
                   SELECT 
                   id,
                   name,
                   type,
                   description,
                   category,
                   enable
                   FROM lmp_measure_type 
                   WHERE 
                   type = %s AND index_in = %s
                   """, (datatype, index ))
    row = cursor.fetchone()     
    if not row: return None
    return {'id' : row[0], 
            'name' : row[1], 
            'type' : row[2],
            'descripcion' : row[3],
            'category' : row[4],
            'enable' : row[5]
            }    

def createMeasureType(conn, name, measType, category, enable, indexIn):
    logger.debug("createMeasureType:"+name+"-"+measType)  
    cursor = conn.cursor()
    cursor.execute(
    """
    INSERT INTO lmp_measure_type(name, type, category, enable, index_in) 
    VALUES (%s, %s, %s, %s, %s)
    """
    , (name, measType, category, enable, indexIn)
    )   
    conn.commit()
    item = findMeaureType(cursor, name, measType)
    cursor.close()
    return item

def findMeaureSource(cursor, componentId, measureTypeId):
    logger.debug("findMeaureSource:"+str(componentId)+"-"+str(measureTypeId))
    cursor.execute("""
                   SELECT 
                   id,
                   component_id,
                   type_id,
                   source_id,
                   enable
                   FROM lmp_measure_source
                   WHERE component_id = %s AND type_id = %s
                   """, (componentId, measureTypeId ))
    row = cursor.fetchone()     
    if not row: return None
    return {'id' : row[0], 
            'componentId' : row[1], 
            'typeId' : row[2],
            'sourceId' : row[3],
            'enable' : row[4]
            }
    
def createMeasureSource(conn, componentId, measureTypeId, sourceId, enable):
    logger.debug("createMeasureSource:"+str(componentId)+"-"+str(measureTypeId))  
    cursor = conn.cursor()
    cursor.execute(
    """
    INSERT INTO lmp_measure_source(component_id, type_id, source_id, enable) 
    VALUES (%s, %s, %s, %s)
    """
    , (componentId, measureTypeId, sourceId, enable)
    )   
    conn.commit()
    item = findMeaureSource(cursor, componentId, measureTypeId)
    cursor.close()
    return item


#%%
    
def findLogKey(cursor, key):
    logger.debug("findLogKey:"+key)
    cursor.execute("""
                   SELECT 
                   lmp_logkey.id AS id,
                   lmp_logkey.keyMaster AS keyMaster,
                   lmp_logkey.key AS key,
                   lmp_logkey.className AS className,
                   lmp_logkey.method AS method,
                   lmp_logkey.lineNumber AS lineNumber,
                   lmp_logkey.logLevel AS logLevel,
                   lmp_logkey.text AS text,
                   lmp_logkey.extra AS extra,
                   lmp_logkey.category AS category,
                   lmp_logkey.extra AS extra,
                   lmp_logkey.component_id AS componentId,
                   lmp_component.key AS componentKey,
                   lmp_host.key AS hostKey
                   FROM lmp_logkey 
                   INNER JOIN lmp_component ON lmp_logkey.component_id = lmp_component.id
                   INNER JOIN lmp_host ON lmp_component.host_id = lmp_host.id
                   WHERE keyMaster = %s;
                   """, (key, ))
    values = cursor.fetchone()     
    if not values: return None
    
    colnames = list(map(lambda x: x[0], cursor.description)) 
    return dict(zip(colnames, values))
    

    
def findLogKeyId(cursor, key):
    logger.debug("findLogKeyId:"+key)
    cursor.execute("""
                   SELECT id FROM lmp_logkey WHERE keyMaster = %s;
                   """, (key, ))
    row = cursor.fetchone()     
    if not row: return None
    return row[0]

def createLogKey(cursor, keyMaster, componentId, key, className, method, lineNumber, logLevel, text, category):
    logger.debug("createLogKey:"+keyMaster)  
    cursor.execute(
    """
    INSERT INTO lmp_logkey(keyMaster, component_id,
    key, className, method, lineNumber, logLevel, text, category ) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    , (keyMaster, componentId, key, className, method, lineNumber, logLevel, text, category)
    )


#%%

def findLogPathId(cursor, logKey1Id, logKey2Id):
    logger.debug("findLogPathId:"+str(logKey1Id)+"->"+str(logKey2Id))
    cursor.execute("SELECT id FROM lmp_logpath WHERE logkey1_id = %s AND logkey2_id = %s;", (logKey1Id, logKey2Id))
    row = cursor.fetchone()     
    if not row: return None
    return row[0]

def findLogPathIdByKeyMaster(cursor, keyMaster):
    logger.debug("findLogPathIdByKeyMaster:"+keyMaster)
    cursor.execute("SELECT id FROM lmp_logpath WHERE keyMaster = %s", (keyMaster, ))
    row = cursor.fetchone()     
    if not row: return None
    return row[0]


def createLogPath(cursor, keyMaster, componentId, logKey1Id, logKey2Id):
    logger.debug("createLogPath:"+str(logKey1Id)+"->"+str(logKey2Id))  
    cursor.execute(
    """
    INSERT INTO lmp_logpath(keyMaster, logkey1_id, logkey2_id, component_id ) 
    VALUES (%s, %s, %s, %s)
    """
    , (keyMaster, logKey1Id, logKey2Id, componentId)
    )
    
#%%  
    
def findLogRecordId(cursor, exectime, logKeyId):
    logger.debug("findLogPathId:"+str(exectime)+"->"+str(logKeyId))
    cursor.execute("SELECT id FROM lmp_logrecord WHERE exectime = %s AND logkey_id = %s;", (exectime, logKeyId))
    row = cursor.fetchone()     
    if not row: return None
    return row[0]

def createLogRecord(cursor, exectime, logKeyId, componentId, remoteCallKey, userKey):
    logger.debug("createLogRecord:"+str(exectime)+"->"+str(logKeyId))  
    cursor.execute(
    """
    INSERT INTO lmp_logrecord(
      exectime,
      logkey_id, 
      component_id,          
      remoteCallKey, 
      userKey     
    ) 
    VALUES (%s, %s, %s, %s, %s)
    """
    , (exectime, logKeyId, componentId, remoteCallKey, userKey)
    )
    
#%%  
    

def createPathMeasure(cursor, date, period, pathId, hostId, 
                      count, duration_avg, duration_std, duration_max):
    logger.debug("createPathMeasure:"+str(date)+": "+str(pathId)+"->"+str(duration_avg))
    
    cursor.execute("""
    SELECT id FROM lmp_path_measure 
    WHERE 
    date = %s AND path_id = %s
    """, (date, pathId)) 
    row = cursor.fetchone()
    if row:
        logger.warning("record already exists")
        return    
       
    cursor.execute(
    """
    INSERT INTO lmp_path_measure(date, period, path_id, host_id, 
    count, duration_avg, duration_std, duration_max
    ) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    , (date, period, pathId, hostId, 
        count, duration_avg, duration_std, duration_max)
    )

def createHostMeasure(cursor, date, period, hostId, sourceId, 
                      cpu ,cpu_user,cpu_sys ,cpu_idle , mem ,swap ,diskusage ,pids ,cnxs , users , 
                      disk_io_rate_w,disk_io_rate_r , net_io_rate_in ,net_io_rate_out ,
                      openfiles,openfiles_rate ,
                      net_err_rate_in ,net_err_rate_out ,net_drop_rate_in ,net_drop_rate_out                                           
                      ):
    logger.debug("createHostMeasure:"+str(date)+": "+str(hostId))    
    
    cursor.execute("""
    SELECT id FROM lmp_host_measure 
    WHERE 
    date = %s AND host_id = %s AND source_id = %s
    """, (date, hostId, sourceId)) 
    row = cursor.fetchone()
    if row:
        logger.warning("record already exists")
        return        
  
    cursor.execute(
    """
    INSERT INTO lmp_host_measure(
      date, period, host_id, source_id, 
      
      cpu ,
      cpu_user,
      cpu_sys ,
      cpu_idle ,          
      mem ,
      swap ,
      diskusage ,
      pids ,
      cnxs ,
      users ,
      
      disk_io_rate_w,
      disk_io_rate_r ,
      
      net_io_rate_in ,
      net_io_rate_out ,

      openfiles ,
      openfiles_rate ,          
      
      net_err_rate_in ,
      net_err_rate_out ,
      net_drop_rate_in ,
      net_drop_rate_out 
    ) 
    VALUES (%s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, 
    %s, %s, 
    %s, %s, %s, %s
    )
    """
    , (date, period, hostId, sourceId, 
       cpu ,cpu_user,cpu_sys ,cpu_idle , mem ,swap ,diskusage ,pids ,cnxs , users , 
       disk_io_rate_w,disk_io_rate_r , net_io_rate_in ,net_io_rate_out ,
       openfiles,openfiles_rate ,
       net_err_rate_in ,net_err_rate_out ,net_drop_rate_in ,net_drop_rate_out
       )
    )   
    
    
def createMeasure(cursor, date, period, typeId, sourceId, value):
    logger.debug("createMeasure:"+str(date)+": "+str(typeId)+","+str(sourceId)+"->"+str(value))  
    
    cursor.execute("""
    SELECT id FROM lmp_measure 
    WHERE 
    date = %s AND type_id = %s AND source_id = %s
    """, (date, typeId, sourceId)) 
    row = cursor.fetchone()
    if row:
        logger.warning("record already exists")
        return        
  
    cursor.execute(
    """
    INSERT INTO lmp_measure(
      date, period, type_id, source_id, value 
    ) 
    VALUES (%s, %s, %s, %s, %s)
    """
    , (date, period, typeId, sourceId, value)
    ) 
    
    
def createSourceCounters(cursor, date, sourceId, count, bytesp, records, fails):
    logger.debug("createSourceCounters:"+str(date)+": "+str(sourceId)+"->"+str(count))  
    
    cursor.execute("""
    SELECT id FROM lmp_sourcecounters 
    WHERE 
    date = %s AND source_id = %s
    """, (date, sourceId)) 
    row = cursor.fetchone()
    if row:
        logger.warning("record already exists")
        return        
  
    cursor.execute(
    """
    INSERT INTO lmp_sourcecounters(
      date, source_id, count, bytes, records, fails
    ) 
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    , (date, sourceId, count, bytesp, records, fails)
    ) 


 
#%%

def findHosts(cursor):
    logger.debug("findHosts:")
    cursor.execute("SELECT id, key, name FROM lmp_host")
    rows = cursor.fetchall()     
    rowsdict = []
    for row in rows:
        rowsdict.append(
                    {
                    'id' : row[0], 
                    'key' : row[1],
                    'name' : row[2]
                    }
                )   
    return rowsdict

  

def findComponentsByHostId(cursor, hostId):
    """ 
    ===========================================================================
    Find in DB readers with agent, host and component data
    =========================================================================== 

    **Args**:
        Key of reader
    **Returns**:
        Array of Dictionary
    """ 
    logger.debug("findComponentsByHostId:"+str(hostId))    
    
    sql = """
    SELECT 
    id,
    key,
    name,
    host_id
    FROM lmp_component
    WHERE host_id = %s 
    """
    filters = [hostId]    
    cursor.execute(
    sql, filters
    )    
    rows = cursor.fetchall() 
    
    rowsdict = []
    for row in rows:
        rowsdict.append(
                    {
                    'id' : row[0], 
                    'key' : row[1],
                    'name' : row[2],
                    'hostId' : row[3]
                    }
                )
    return rowsdict    
    
def findAgentsEnabled(cursor, agentKey = None):
    """ 
    ===========================================================================
    Find in DB readers with agent, host and component data
    =========================================================================== 

    **Args**:
        Key of reader
    **Returns**:
        Array of Dictionary
    """     
    
    sql = """
    SELECT 
    lmp_agent.id,
    lmp_agent.key,
    lmp_agent.host_id as hostId,
    lmp_agent.ip as ip,
    lmp_agent.port as port    
    FROM lmp_agent
    WHERE lmp_agent.enable = TRUE
    """
    filters = []
    if agentKey:
        sql += " AND lmp_agent.key = %s"
        filters = [agentKey]
    
    cursor.execute(
    sql, filters
    )    
    rows = cursor.fetchall() 
    
    rowsdict = []
    for row in rows:
        rowsdict.append(
                    {
                    'id' : row[0], 
                    'key' : row[1], 
                    'hostId' : row[2],
                    'ip' : row[3],
                    'port' : row[4]
                    }
                )
    
    return rowsdict    
    
    
def findSourcesByAgentIdAndType(cursor, agentId, sourcetype, sourceKey = None):
    """ 
    ===========================================================================
    Find in DB readers with agent, host and component data
    =========================================================================== 

    **Args**:
        Key of reader
    **Returns**:
        Array of Dictionary
    """ 
    logger.debug("findSourcesByAgentIdAndType:"+str(agentId)+":"+sourcetype)    
    
    sql = """
    SELECT DISTINCT
    lmp_source.id,
    lmp_source.key,
    lmp_source.name,
    lmp_host.id as hostId,
    lmp_host.key as hostKey,
    lmp_host.name as hostName,
    lmp_component.id as componentId,
    lmp_component.key as componentKey,
    lmp_component.name as componentName,
    lmp_source.sourcetype as sourcetype
    FROM lmp_measure_source
    INNER JOIN lmp_source ON lmp_measure_source.source_id = lmp_source.id
    INNER JOIN lmp_agent ON lmp_source.agent_id = lmp_agent.id
    INNER JOIN lmp_host ON lmp_agent.host_id = lmp_host.id
    INNER JOIN lmp_component ON lmp_measure_source.component_id = lmp_component.id
    WHERE lmp_agent.id = %s AND  lmp_source.enable = TRUE AND lmp_source.sourcetype = %s 
    """
    filters = [agentId, sourcetype]
    if sourceKey:
        sql += " AND lmp_source.key = %s"
        filters = [agentId, sourcetype, sourceKey]
    
    cursor.execute(
    sql, filters
    )    
    rows = cursor.fetchall() 
    
    rowsdict = []
    for row in rows:
        rowsdict.append(
                {
                'id' : row[0], 
                'key' : row[1],
                'name' : row[2],
                'hostId' : row[3],
                'hostKey' : row[4],
                'hostName' : row[5],
                'componentId' : row[6],
                'componentKey' : row[7],
                'componentName' : row[8],
                'sourcetype' : row[9]
                }
                )
    
    return rowsdict

def findSourcesByComponentId(cursor, componentId, sourcetype = None):
    """ 
    ===========================================================================
    Find in DB readers with agent, host and component data
    =========================================================================== 

    **Args**:
        Key of reader
    **Returns**:
        Array of Dictionary
    """ 
    logger.debug("findSourcesByComponentId:"+str(componentId))    
    
    sql = """
    SELECT DISTINCT
    lmp_source.id,
    lmp_source.key,
    lmp_source.name,
    lmp_host.id as hostId,
    lmp_host.key as hostKey,
    lmp_host.name as hostName,
    lmp_component.id as componentId,
    lmp_component.key as componentKey,
    lmp_component.name as componentName,
    lmp_source.sourcetype as sourcetype
    FROM lmp_measure_source
    INNER JOIN lmp_source ON lmp_measure_source.source_id = lmp_source.id
    INNER JOIN lmp_agent ON lmp_source.agent_id = lmp_agent.id
    INNER JOIN lmp_host ON lmp_agent.host_id = lmp_host.id
    INNER JOIN lmp_component ON lmp_measure_source.component_id = lmp_component.id
    WHERE lmp_source.enable = TRUE AND lmp_measure_source.component_id = %s 
    """
    filters = [componentId]
    if sourcetype:
        sql += " AND sourcetype = %s"
        filters = [componentId, sourcetype]
    
    cursor.execute(
    sql, filters
    )    
    rows = cursor.fetchall() 
    
    rowsdict = []
    for row in rows:
        rowsdict.append(
                {
                'id' : row[0], 
                'key' : row[1],
                'name' : row[2],
                'hostId' : row[3],
                'hostKey' : row[4],
                'hostName' : row[5],
                'componentId' : row[6],
                'componentKey' : row[7],
                'componentName' : row[8],
                'sourcetype' : row[9]
                }
                )
    
    return rowsdict

def findReaderSourceByKey(cursor, key):
    """ 
    ===========================================================================
    Find in DB readers with agent, host and component data
    =========================================================================== 

    **Args**:
        Key of reader
    **Returns**:
        Array of Dictionary
    """ 
    logger.debug("findReaderSourceByKey:"+key)    
    
    sql = """
    SELECT DISTINCT
    lmp_source.id,
    lmp_source.key,
    lmp_source.name,
    lmp_host.id as hostId,
    lmp_host.key as hostKey,
    lmp_host.name as hostName,
    lmp_component.id as componentId,
    lmp_component.key as componentKey,
    lmp_component.name as componentName,
    lmp_source.sourcetype as sourcetype
    FROM lmp_measure_source
    INNER JOIN lmp_source ON lmp_measure_source.source_id = lmp_source.id
    INNER JOIN lmp_agent ON lmp_source.agent_id = lmp_agent.id
    INNER JOIN lmp_host ON lmp_agent.host_id = lmp_host.id
    INNER JOIN lmp_component ON lmp_measure_source.component_id = lmp_component.id
    WHERE lmp_source.enable = TRUE AND lmp_source.sourcetype = %s
    AND lmp_source.key = %s 
    """
    
    sourceType = lmkey.SOURCE_TYPE_READER

    
    cursor.execute(
    sql, (sourceType, key)
    )    
    row = cursor.fetchone() 
    

    return {
            'id' : row[0], 
            'key' : row[1],
            'name' : row[2],
            'hostId' : row[3],
            'hostKey' : row[4],
            'hostName' : row[5],
            'componentId' : row[6],
            'componentKey' : row[7],
            'componentName' : row[8],
            'sourcetype' : row[9]
            }




def findLogPathsByComponentId(cursor, componentId):
    cursor.execute(
    """
    SELECT
    id,
    keyMaster
    FROM lmp_logpath
    WHERE
    component_id = %s   
    """, (componentId,));
    return cursor.fetchall()

def updateLogPathDurationData(cursor, pathId, count, avg, std, maxv, start, end):
    logger.debug("updateLogPathDurationData:"+str(pathId)+":"+str(count)+" -> "+str(avg))
    if avg == None:
        return
    
    if count < 3:
        return
    
    cursor.execute(
    """
    UPDATE lmp_logpath SET
    samples_count = %s,
    duration_avg = %s,
    duration_std = %s,
    duration_max = %s,
    samples_start=%s, 
    samples_end=%s    
    WHERE
    id = %s   
    """, ( count, avg, std, maxv, start, end, pathId))
            
def resetLogPathDurationData(cursor):
    logger.debug("resetLogPathDurationData:")
    
    cursor.execute(
    """
    UPDATE lmp_logpath SET
    samples_count = NULL,
    duration_avg = NULL,
    duration_std = NULL,
    duration_max = NULL,
    samples_start=NULL, 
    samples_end=NULL      
    """)

def findReferencePathMeasures(cursor, pathId, start, end, ref=None):
    sql = """
    SELECT
    COUNT(lmp_path_measure.count),  
    AVG(lmp_path_measure.duration_avg),
    AVG(lmp_path_measure.duration_std),
    AVG(lmp_path_measure.duration_max)
    FROM lmp_path_measure
    WHERE
    lmp_path_measure.path_id = %s AND
    lmp_path_measure.date BETWEEN %s AND %s
        """ 
    filters = [pathId, start, end]
    if ref != None:
        sql += "  AND ref = %s"
        filters = [pathId, start, end, ref]
    cursor.execute( sql, filters)  
    return cursor.fetchone()


def findPathMeasuresByDateAndComponentId(cursor, start, componentId):
    logger.debug("findPathMeasuresByDateAndComponentId:"+str((start, componentId)))
    cursor.execute(
    """
    SELECT 
    lmp_path_measure.count, 
    lmp_path_measure.performance,
    lmp_path_measure.performance_min
    FROM lmp_path_measure 
    INNER JOIN lmp_logpath ON lmp_path_measure.path_id = lmp_logpath.id
    WHERE
    lmp_path_measure.performance IS NOT NULL AND
    lmp_path_measure.performance_min IS NOT NULL AND
    lmp_logpath.component_id= %s AND date = %s      
    """, (componentId, start ));
    return cursor.fetchall() 

def countPathMeasuresByDateAndComponentIdAndPerformanceRange(cursor, start, componentId, vmin, vmax):
    logger.debug("countPathMeasuresByDateAndComponentIdAndPerformanceRange:")
    cursor.execute(
    """
    SELECT 
    SUM(lmp_path_measure.count)
    FROM lmp_path_measure 
    INNER JOIN lmp_logpath ON lmp_path_measure.path_id = lmp_logpath.id
    WHERE
    lmp_logpath.component_id= %s AND date = %s
    AND lmp_path_measure.performance >= %s
    AND lmp_path_measure.performance < %s       
    """, (componentId, start, vmin, vmax ));
    row = cursor.fetchone()
    if not row: return None
    return row[0]

def countPathMeasuresByDateAndComponentIdAndPerformanceMinRange(cursor, start, componentId, vmin, vmax):
    logger.debug("countPathMeasuresByDateAndComponentIdAndPerformanceMinRange:")
    cursor.execute(
    """
    SELECT 
    SUM(lmp_path_measure.count)
    FROM lmp_path_measure 
    INNER JOIN lmp_logpath ON lmp_path_measure.path_id = lmp_logpath.id
    WHERE
    lmp_logpath.component_id= %s AND date = %s
    AND lmp_path_measure.performance_min >= %s
    AND lmp_path_measure.performance_min < %s       
    """, (componentId, start, vmin, vmax ));
    row = cursor.fetchone()
    if not row: return None
    return row[0]


def findPathMeasuresAndReference(cursor, start, end):
    cursor.execute(
    """
    SELECT
    lmp_path_measure.id,
    lmp_path_measure.count,  
    lmp_path_measure.duration_avg,
    lmp_path_measure.duration_std,
    lmp_path_measure.duration_max,
    lmp_logpath.samples_count as countref,  
    lmp_logpath.duration_avg as avgref,
    lmp_logpath.duration_std as stdref,
    lmp_logpath.duration_max as maxref    
    FROM lmp_path_measure
    INNER JOIN lmp_logpath ON lmp_path_measure.path_id = lmp_logpath.id
    WHERE
    lmp_logpath.samples_count IS NOT NULL AND
    lmp_path_measure.date BETWEEN %s AND %s     
    """, (start, end ))
    return cursor.fetchall()     


def createPerfomanceMeasure(cursor, date, period, hostId, componentId, performance, performanceMin,
                            g1avg, g2avg, g3avg, g4avg, g1min, g2min, g3min, g4min,
                            update=True):
    cursor.execute(
    """
    SELECT id FROM lmp_result WHERE date = %s AND host_id=%s AND component_id = %s    
    """, (date, hostId, componentId))  
    row = cursor.fetchone()
    if row and not update:
        return
    
    if row:
        logger.debug("Update performance measure:"+str((date, hostId, componentId, performance)))
        cursor.execute(
        """
        UPDATE lmp_result SET 
        performance = %s, 
        performance_min = %s,
        g1avg = %s,
        g2avg = %s,
        g3avg = %s,
        g4avg = %s,
        g1min = %s,
        g2min = %s,
        g3min = %s,
        g4min = %s        
        WHERE id = %s   
        """, (performance, performanceMin,g1avg, g2avg, g3avg, g4avg, g1min, g2min, g3min, g4min, row[0]))        
    else:
        logger.debug("Create performance measure:"+str((date, hostId, componentId, performance)))
        cursor.execute(
        """
        INSERT INTO lmp_result(date, period, host_id, component_id, 
        performance, performance_min,
        g1avg, g2avg, g3avg, g4avg,
        g1min, g2min, g3min, g4min
        ) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)     
        """, (date, period, hostId, componentId, performance, performanceMin,g1avg, g2avg, g3avg, g4avg, g1min, g2min, g3min, g4min)) 
        
        
def createPerfomanceMeasureWithAnomaly(cursor, date, period, hostId, componentId, anomaly):
    logger.debug("createPerfomanceMeasureWithAnomaly")
    cursor.execute(
    """
    INSERT INTO lmp_result(date, period, host_id, component_id, anomaly) 
    VALUES (%s, %s, %s, %s, %s)     
    """, (date, period, hostId, componentId, anomaly))         

def updatePathMeasurePerfomance(cursor, idPathMeasure, performance, performanceMin):
    cursor.execute(
    """
    UPDATE lmp_path_measure SET performance = %s, performance_min = %s where id = %s  
    """, (performance, performanceMin, idPathMeasure))
    

def findPerfomanceMeasure(cursor, date, componentId):
#    logger.debug("findPerfomanceMeasure:"+str((date, componentId)))
    cursor.execute(
    """
    SELECT id, performance FROM lmp_result
    WHERE date = %s AND component_id = %s 
    """, (date, componentId));             
    row = cursor.fetchone()
    if not row:
        return None
    return {
            "id" : row[0],
            "performance" : row[1]
            }
    

def updateAnomalyPredicted(cursor, resultId, anomaly):
#    logger.debug("updateAnomalyPredicted:"+str(resultId)+"="+str(anomaly))
    cursor.execute(
    """
    UPDATE lmp_result SET
    anomaly = %s
    WHERE
    id = %s   
    """, (anomaly, resultId));
            
def updatePerformancePredicted(cursor, resultId, predicted):
#    logger.debug("updatePerformancePredicted:"+str(resultId)+"="+str(predicted))
    cursor.execute(
    """
    UPDATE lmp_result SET
    predicted = %s
    WHERE
    id = %s   
    """, (predicted, resultId));            

            
def findRemoteCallKeys(cursor, start, end):
    cursor.execute(
    """
    SELECT remoteCallKey, COUNT(*) AS c 
    FROM lmp_logrecord 
    WHERE exectime  BETWEEN %s AND %s 
    GROUP BY remoteCallKey 
    HAVING COUNT(*) > 1   
    """, (start, end));
    return cursor.fetchall()   

#def findRemoteCallRecords(cursor, remoteCallKey):
#    cursor.execute(
#    """
#    SELECT DISTINCT  component_id 
#    FROM lmp_appLogEventsT 
#    WHERE remoteCallKey = %s
#	ORDER BY id
#    """, (remoteCallKey, ));
#    return cursor.fetchall()  
    
def findRemoteCallRecords(cursor, remoteCallKey):
    cursor.execute(
    """
    SELECT logkey_id, component_id 
    FROM lmp_logrecord 
    WHERE remoteCallKey = %s
	ORDER BY exectime
    """, (remoteCallKey, ));
    return cursor.fetchall()  

def createRemoteCall(cursor, a, b):
    logger.debug("createRemoteCall:"+str((a,b)))
    cursor.execute(
    """
    SELECT id FROM lmp_remotecall
    WHERE logkey1_id = %s  AND logkey2_id = %s
    """, (a, b));             
    row = cursor.fetchone() 
    
    if row:
        return
    
    cursor.execute(
    """
    INSERT INTO lmp_remotecall(logkey1_id, logkey2_id)
    VALUES(%s, %s)
    """, (a, b));   



def findMeasuresTypesByComponent(cursor, componentId):
    logger.debug("findMeasuresTypesByComponent:")
    cursor.execute("""
                    SELECT lmp_measure_type.id, name, type, category, transf_type, 
                    lmp_measure_source.id AS measureSourceId, k1, k2, 
                    index_in, index_out
                    FROM lmp_measure_source
                    INNER JOIN lmp_measure_type ON type_id = lmp_measure_type.id
                    WHERE component_id = %s AND 
                    lmp_measure_type.enable = TRUE AND 
                    lmp_measure_source.enable = TRUE
                    ORDER BY index_out ASC
                   """, (componentId, ))
    rows = cursor.fetchall()
    
    rowsdict = []
    for row in rows:
        rowsdict.append(
                {
                'id' : row[0], 
                'name' : row[1],
                'type' : row[2],
                'category' : row[3],
                'transfType' : row[4],
                'measureSourceId' : row[5],
                'k1' : row[6],
                'k2' : row[7],
                'indexIn' : row[8],
                'indexOut' : row[9],
                }
                )
    
    return rowsdict

def findHostMeasuresTypesByComponent(cursor, componentId):
    logger.debug("findMeasuresTypesByComponent:")
    cursor.execute("""
                    SELECT lmp_measure_type.id, name, type, category, transf_type, 
                    lmp_measure_source.id AS measureSourceId, k1, k2, 
                    index_in, index_out
                    FROM lmp_measure_source
                    INNER JOIN lmp_measure_type ON type_id = lmp_measure_type.id
                    WHERE component_id = %s AND 
                    lmp_measure_type.enable = TRUE AND 
                    lmp_measure_source.enable = TRUE AND
                    lmp_measure_type.type = %s
                    ORDER BY index_in ASC
                   """, (componentId, lmkey.DATATYPE_MONITOR_HOST))
    rows = cursor.fetchall()
    
    rowsdict = []
    for row in rows:
        rowsdict.append(
                {
                'id' : row[0], 
                'name' : row[1],
                'type' : row[2],
                'category' : row[3],
                'transfType' : row[4],
                'measureSourceId' : row[5],
                'k1' : row[6],
                'k2' : row[7],
                'indexIn' : row[8],
                'indexOut' : row[9],
                }
                )
    
    return rowsdict

def findMeasuresTypesBySourceId(cursor, sourceId):
    logger.debug("findMeasuresTypesBySourceId:")
    cursor.execute("""
                    SELECT lmp_measure_type.id, name, type, category, transf_type, 
                    lmp_measure_source.id AS measureSourceId, k1, k2, 
                    index_in, index_out,
                    lmp_measure_type.id
                    FROM lmp_measure_source
                    INNER JOIN lmp_measure_type ON type_id = lmp_measure_type.id
                    WHERE lmp_measure_source.source_id = %s AND 
                    lmp_measure_type.enable = TRUE AND 
                    lmp_measure_source.enable = TRUE
                    ORDER BY index_out ASC
                   """, (sourceId, ))
    rows = cursor.fetchall()
    
    rowsdict = []
    for row in rows:
        rowsdict.append(
                {
                'id' : row[0], 
                'name' : row[1],
                'type' : row[2],
                'category' : row[3],
                'transfType' : row[4],
                'measureSourceId' : row[5],
                'k1' : row[6],
                'k2' : row[7],
                'indexIn' : row[8],
                'indexOut' : row[9],
                'typeId' : row[10],
                }
                )
    
    return rowsdict

def updateMeasureType(cursor, typeId, category, transfType, enable):
    logger.debug("updateMeasureType:"+str(typeId)+": "+str(category)+","+str(enable))
    cursor.execute("""
                   UPDATE lmp_measure_type SET
                   category = %s,
                   transf_type = %s,
                   enable = %s
                   WHERE id = %s
                   """, (category, transfType, enable, typeId ))  

def updateMeasureSource(cursor, measureSourceId, enable, k1, k2):
    logger.debug("updateMeasureSource:"+str(measureSourceId)+": "+str(k1)+","+str(enable))
    cursor.execute("""
                   UPDATE lmp_measure_source SET
                   k1 = %s,
                   k2 = %s,
                   enable = %s
                   WHERE id = %s
                   """, (k1, k2, enable,  measureSourceId))      

def findMeasureValue(cursor, date, typeId, ref=None):
    sql = """
          SELECT value from lmp_measure 
          WHERE date = %s AND type_id = %s
        """ 
    filters = [date, typeId]
    if ref != None:
        sql += "  AND ref = %s"
        filters = [date, typeId, ref]
    cursor.execute( sql, filters)  
    row = cursor.fetchone()
    if row:
        return row[0]
    return None

def findPerformanceValues(cursor, date, componentId):
    cursor.execute("""SELECT performance, performance_min FROM lmp_result
                       WHERE
                       date = %s AND component_id = %s
                   """, (date, componentId) )
    return cursor.fetchone()
  

def findHostMeasuresColnames(cursor):
    logger.debug("findHostMeasuresColnames:")
    cursor.execute("select * from lmp_host_measure")

    colnames = list(map(lambda x: x[0], cursor.description)) 
    return colnames[5:] # drop first 5 columns

def updateIndexMeasureType(cursor, typeId, indexOut):
    logger.debug("updateIndexMeasureType:"+str(typeId)+"->"+str(indexOut))
    cursor.execute(
    """
    UPDATE lmp_measure_type SET index_out = %s WHERE id = %s
    """, (indexOut, typeId) ) 

def updateCategoryMeasureType(cursor, typeId, category):
    logger.debug("updateCategoryMeasureType:"+str(typeId)+"->"+str(category))
    cursor.execute(
    """
    UPDATE lmp_measure_type SET category = %s WHERE id = %s
    """, (category, typeId) ) 

def updateEnableMeasureType(cursor, typeId, enable):
    logger.debug("updateEnableMeasureType:"+str(typeId)+"->"+str(enable))
    cursor.execute(
    """
    UPDATE lmp_measure_type SET enable = %s WHERE id = %s
    """, (enable, typeId) ) 

def findHostMeasureByDate(cursor, hostId, date, ref=None):
    
    sql = """
        SELECT 
              cpu,
              cpu_user,
              cpu_sys,
              cpu_idle,          
              mem,
              swap,
              diskusage,
              pids,
              cnxs,
              users,         
              
              disk_io_rate_w,
              disk_io_rate_r,         
              net_io_rate_in,
              net_io_rate_out,
              
              openfiles, 
              openfiles_rate,
              
              net_err_rate_in,
              net_err_rate_out,
              net_drop_rate_in,
              net_drop_rate_out
              
        FROM lmp_host_measure
        WHERE host_id = %s  AND date = %s
        """ 
    filters = [hostId, date]
    if ref != None:
        sql += "  AND ref = %s"
        filters = [hostId, date, ref]
    cursor.execute( sql, filters)  
    return cursor.fetchone()   
    
#==============================================================================
# METHODS FOR UI
#==============================================================================
    
def findComponents(cursor):
    """ 
    ===========================================================================
    Find in DB readers with agent, host and component data
    =========================================================================== 

    **Args**:
        Key of reader
    **Returns**:
        Array of Dictionary
    """ 
    logger.debug("findComponents:")    
    
    cursor.execute(
    """
    SELECT 
    id,
    key,
    name,
    host_id,
    x, y,
    x_label, y_label
    FROM lmp_component
    """ ) 
    rows = cursor.fetchall() 
    
    rowsdict = []
    for row in rows:
        rowsdict.append(
                    {
                    'id' : row[0], 
                    'key' : row[1],
                    'name' : row[2],
                    'hostId' : row[3],
                    'x' : row[4],
                    'y' : row[5], 
                    'xLabel' : row[6],
                    'yLabel' : row[7]                       
                    }
                )
    return rowsdict  
    
def findLogPathsWithMeasures(cursor, componentId, start, end, limit):
    cursor.execute(
    """
    SELECT DISTINCT logkey1_id, logkey2_id, M.path_id
    FROM lmp_path_measure M 
    INNER JOIN lmp_logpath P ON P.id = M.path_id	
    INNER JOIN lmp_logkey N1 ON P.logkey1_id = N1.id
    INNER JOIN lmp_logkey N2 ON P.logkey2_id = N2.id	
    INNER JOIN lmp_component C ON N1.component_id = C.id
    WHERE C.id = %s 
    AND date BETWEEN %s AND %s
    ORDER BY M.count DESC
    LIMIT %s
    """, (componentId, start, end, limit) )             
    return cursor.fetchall() 



def findLogPathsByComponent(cursor, componentId):
    cursor.execute(
    """
    SELECT DISTINCT logkey1_id AS node1Id, logkey2_id AS node2Id,  P.id AS pathId,
    N1.key AS node1, N2.key AS node2, C.name AS componentName, 
    P.duration_avg AS avgRef, P.duration_max AS maxRef	
    FROM lmp_logpath P 
    INNER JOIN lmp_logkey N1 ON P.logkey1_id = N1.id
    INNER JOIN lmp_logkey N2 ON P.logkey2_id = N2.id	
    INNER JOIN lmp_component C ON N1.component_id = C.id
    WHERE C.id = %s
    """, (componentId, ) )             
    rows = cursor.fetchall() 
    colnames = list(map(lambda x: x[0], cursor.description))
    rowsdict = []
    for row in rows:
        rowsdict.append(dict(zip(colnames, row)))
    return rowsdict


def findLogPathsOrderByLowPerformance(cursor, componentId, start, end):
    cursor.execute(
    """
    SELECT 
    path_id, N1.key AS node1, N2.key AS node2, C.name AS componentName,
    AVG(M.performance) AS pavg,
    MIN(M.performance) AS pmin,
    MAX(M.performance) AS pmax,
    SUM(M.count) AS pcount,
    (1 - AVG(M.performance)) * SUM(M.count) AS PRIORITY
    FROM lmp_path_measure M 
    INNER JOIN lmp_logpath P ON P.id = M.path_id	
    INNER JOIN lmp_logkey N1 ON P.logkey1_id = N1.id
    INNER JOIN lmp_logkey N2 ON P.logkey2_id = N2.id	
    INNER JOIN lmp_component C ON N1.component_id = C.id
    WHERE
    M.performance IS NOT NULL AND
    M.performance <> 0 AND
    M.performance < 0.7 AND
    P.component_id= %s AND M.date BETWEEN %s AND %s  
    GROUP BY path_id, N1.key, N2.key, componentName
    ORDER BY PRIORITY DESC
    """, (componentId, start, end) )             
    rows = cursor.fetchall() 
    colnames = list(map(lambda x: x[0], cursor.description))
    rowsdict = []
    for row in rows:
        rowsdict.append(dict(zip(colnames, row)))
    return rowsdict





def findLogPath(cursor, pathId):
    cursor.execute(
    """
    SELECT logkey1_id AS node1Id, logkey2_id AS node2Id,  P.id AS pathId,
    N1.key AS node1, N2.key AS node2, C.name AS componentName, 
    P.duration_avg AS avgRef, P.duration_max AS maxRef	
    FROM lmp_logpath P 
    INNER JOIN lmp_logkey N1 ON P.logkey1_id = N1.id
    INNER JOIN lmp_logkey N2 ON P.logkey2_id = N2.id	
    INNER JOIN lmp_component C ON N1.component_id = C.id
    WHERE P.id = %s
    """, (pathId, ) )             
    row = cursor.fetchone() 
    colnames = list(map(lambda x: x[0], cursor.description))
    if not row:
        return None
    return dict(zip(colnames, row))
    

def findlogPathPerformance(cursor, pathId, start, end):
    cursor.execute(
    """
    SELECT AVG(M.performance)
    FROM lmp_path_measure M 
    WHERE M.path_id	= %s
    AND date BETWEEN %s AND %s
    """, (pathId, start, end) )             
    return cursor.fetchall() 


def findRemoteCalls(cursor):
    cursor.execute(
    """
    SELECT DISTINCT C1.key AS node1, C2.key AS node2	
    FROM lmp_remotecall RC 	
    INNER JOIN lmp_logkey N1 ON RC.logkey1_id = N1.id
    INNER JOIN lmp_logkey N2 ON RC.logkey2_id = N2.id	
    INNER JOIN lmp_component C1 ON N1.component_id = C1.id
    INNER JOIN lmp_component C2 ON N2.component_id = C2.id
    """)             
    return cursor.fetchall() 

 
def findPerformanceMeasures(cursor, componentId, start, end):
    cursor.execute(
    """
    SELECT date, performance, performance_min, predicted, anomaly
    FROM lmp_result
    WHERE component_id = %s
    AND date BETWEEN %s AND %s
    ORDER BY date	
    """, (componentId, start, end) )             
    return cursor.fetchall() 

def findPerformanceAvgMeasures(cursor, componentId, start, end):
    cursor.execute(
    """
    SELECT AVG(performance), MIN(performance_min), AVG(predicted), AVG(anomaly)
    FROM lmp_result
    WHERE component_id = %s
    AND date BETWEEN %s AND %s
    """, (componentId, start, end) )             
    return cursor.fetchone() 


def findHostData(cursor, hostId, start, end):
    cursor.execute(
    """
    SELECT
          date,
          cpu,
          cpu_user,
          cpu_sys,
          cpu_idle,          
          mem,
          swap,
          diskusage,
          pids,
          cnxs,
          users,         
          openfiles, 
          openfiles_rate,         
          disk_io_rate_w,
          disk_io_rate_r,         
          net_io_rate_in,
          net_io_rate_out,
          net_err_rate_in,
          net_err_rate_out,
          net_drop_rate_in,
          net_drop_rate_out
    FROM lmp_host_measure
    WHERE host_id = %s AND
    date BETWEEN %s AND %s
    ORDER BY date	
    """, (hostId, start, end) )             
    return cursor.fetchall() 



def findMeasuresBySourceIdAndTypeId(cursor, measureTypeId, sourceId, start, end):
    #logger.debug(str((measureTypeId, sourceId, start, end)))
    cursor.execute(
    """
    SELECT date, value
    FROM public.lmp_measure M
    WHERE 
    M.type_id = %s AND M.source_id = %s AND
    M.date BETWEEN %s AND %s
    ORDER BY date	 ASC
    """, (measureTypeId, sourceId, start, end) )             
    return cursor.fetchall() 


def findlogPathDetail(cursor, pathId):
    cursor.execute(
    """
	SELECT 
	N1.key as key1,
	N2.key as key2,
	P.duration_avg,
	C.name	
    FROM lmp_logPathsT  P	
	INNER JOIN lmp_logNodesT N1 ON P.node1_id = N1.id
	INNER JOIN lmp_logNodesT N2 ON P.node2_id = N2.id	
	INNER JOIN lmp_componentsT C ON N1.component_id = C.id
	WHERE p.ID = %s
    """, (pathId, ) )             
    return cursor.fetchall()  



def updateMeasureRefFlag(cursor, start, end, ref):
    cursor.execute(
    """
	UPDATE lmp_path_measure SET
    ref =  %s
    WHERE date BETWEEN %s AND %s
    """, (ref, start, end) )   
    
    cursor.execute(
    """
	UPDATE lmp_host_measure SET
    ref =  %s
    WHERE date BETWEEN %s AND %s
    """, (ref, start, end) )  

    cursor.execute(
    """
	UPDATE lmp_measure SET
    ref =  %s
    WHERE date BETWEEN %s AND %s
    """, (ref, start, end) )    
    
    
def updatePathMeasureRefFlagByAvgRange(cursor, pathId, start, end, vmax, ref):
    cursor.execute(
    """
	UPDATE lmp_path_measure SET
    ref =  %s
    WHERE 
    path_id = %s
    AND date BETWEEN %s AND %s
    AND duration_avg > %s
    """, (ref, pathId, start, end, vmax) )   
         


#%%
    
#==============================================================================
#tablas control 
#==============================================================================

def createTablesControl(conn):
    """
    Create tables mapper logmapper
    """    
    logger.debug("createTablesBase")
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lmpcntl_logmapperlog
        (
          id SERIAL PRIMARY KEY,
          date TIMESTAMP,
          type TEXT,
          description TEXT
        )
    ''')   
        
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS lmpcntl_control
        (
          id INTEGER,
          collector_admin_state TEXT,
          collector_oper_state TEXT
        )
    ''')   
        
#    cursor.execute('''
#        INSERT INTO lmp_controlT
#        (
#          id,
#          date,
#          collector_admin_state,
#          collector_oper_state,  
#          collector_last_date 
#        ) VALUES (
#          1, %s, %s, %s, %s
#        )
#    ''', ( datetime.datetime.now(), 'DISABLE', 'IDLE', datetime.datetime(2018,3, 11, 10, 0, 0))
#    )  

    conn.commit() 
    cursor.close()
    
def getControlData(cursor):
    cursor.execute('''
        SELECT 
          date,
          collector_admin_state,
          collector_oper_state,  
          collector_last_date        
        FROM lmp_controlT WHERE id = 1
    ''') 
    row = cursor.fetchone()
    
    return {
          'date' : row[0],
          'collector_admin_state' :row[1],
          'collector_oper_state'  :row[2],  
          'collector_last_date'   :row[3]                       
            }
    
def updateControlData(cursor, values): 
    cursor.execute('''
        UPDATE lmp_controlT SET
          date = %s,
          collector_admin_state = %s,
          collector_oper_state = %s,  
          collector_last_date = %s        
    ''', (
        datetime.datetime.now(), 
        values['collector_admin_state'],
        values['collector_oper_state'],
        values['collector_last_date']    
        ) 
    )    

#%%

if __name__ == '__main__':
    print('Start module execution:')
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')       
    
#==============================================================================
    import logmappercommon.utils.postgres_util as db
    import logmappercommon.definitions.logmapperkeys as lmkey  

    conn=db.connectDb()
    cursor = conn.cursor()  
    
    agents = findAgentsEnabled(cursor)
    for agent in agents:
        logger.debug('Agent:'+str(agent))   
        sources = findSourcesByAgentIdAndType(cursor, agent['id'], lmkey.SOURCE_TYPE_READER)
        for source in sources:
            logger.debug('Source:'+str(source)) 
            
            
    logKey = findLogKey(cursor, 'device_2')
    logger.debug('logKey:'+str(logKey))
   
   
   
#    
#    
#    for row in rows:
#        logger.debug('Process:'+str(row))   
#        componentId = row['componentId']
        
#    rows = findRemoteCalls(cursor)
#    for row in rows:
#        print(str(row))        
        
        
#    rows = findlogPaths(cursor, 1, 10)
#    for row in rows:
#        print(str(row))
#        
#    start = datetime.datetime(2018,2, 26, 8, 0, 0)
#    end = datetime.datetime(2018, 2, 26, 15, 0, 0)          
#        
#    rows = findResults(cursor, 1, 2, start, end)
#    for row in rows:
#        print(str(row))              
      

    conn.close()
 
    print("End module execution") 