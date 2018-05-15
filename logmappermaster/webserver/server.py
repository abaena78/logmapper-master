# -*- coding: utf-8 -*-
"""
Created on Sun Mar  4 20:32:04 2018

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

    
import config.config as cfg
import logmappermaster.dao.master_dao as masterdao
import logmappercommon.utils.postgres_util as db
import logmappercommon.utils.logmapper_util as lmutil
import logmappercommon.utils.logging_util as logging_util
import logmappercommon.definitions.logmapperkeys as lmkey
import logmappermaster.ml.predictor as predictor


from flask import Flask
from flask import request, jsonify, abort, render_template

#%%
"""
Global Initialization. Constants definitions.
"""

logger = logging.getLogger(__name__)
app = Flask(__name__)


@app.route('/')
def index():
    app.logger.info("index")
    return """
    <h1>Logmapper</h1>
    <p>Welcome!!</p>
    """

@app.route('/help')
def helpindex():
    return 'Logmapper!'

@app.route('/help/<string:page_name>/')
def show_help_page(page_name):
    return render_template('%s.html' % page_name)


def shutdown_server():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()
    
@app.route('/shutdown', methods=['GET'])
def shutdown():
    shutdown_server()
    return 'Server shutting down...'

@app.route('/info', methods=['GET'])
def info():
    return jsonify({'logmapper-master': '0.0.1'}) 

@app.route('/state', methods=['GET'])
def state():
    app.logger.debug("state:")
    conn=db.connectDb() 
    cursor = conn.cursor()
    control = masterdao.getControlData(cursor)
    conn.close()
    return jsonify(control) 

@app.route('/set', methods=['GET', 'POST'])
def setstate():
    app.logger.info("setstate")
    conn=db.connectDb() 
    cursor = conn.cursor()
    control = masterdao.getControlData(cursor)
    
    collector_admin_state=request.args.get('collector_admin_state')
    if collector_admin_state:
        control['collector_admin_state']=collector_admin_state

    masterdao.updateControlData(cursor, control)
    conn.commit()
    conn.close()   
    
    return "OK:"+ str(control)

@app.route('/cmd/<string:command>', methods=['GET', 'POST'])
def cmd(command):
    app.logger.info("cmd:"+command)
    
    if not command:
        abort(404)    
    
    start=request.args.get('start')
    end=request.args.get('end')
    
    return jsonify({'command': command, 'start': start, 'end': end})

@app.route('/save/<string:datatype>', methods=['POST'])
def save(datatype):
    app.logger.info("save:"+datatype)
    
    if not datatype:
        abort(404)    
    
    data = request.get_json()
    print(str(data))
    
    success = False
    error = None
    
    if datatype == lmkey.DATATYPE_AGENT:
        (success, error) = saveDataAgent(data)
        
    elif datatype == lmkey.DATATYPE_PATH_METRICS:
        (success, error) = saveDataPathMetrics(data)
    elif datatype == lmkey.DATATYPE_LOG_EVENTS:
        (success, error) = saveDataLogEvents(data)
    elif datatype == lmkey.DATATYPE_LOG_METRICS:
        (success, error) = saveDataLogMetrics(data)
        
    elif datatype == lmkey.DATATYPE_MONITOR_HOST:
        (success, error) = saveDataMonitor(data)
        measureAndPredict()
    elif datatype == lmkey.DATATYPE_MONITOR_MICROSERVICE:
        (success, error) = saveDataMonitor(data)
    elif datatype == lmkey.DATATYPE_MONITOR_POSTGRES:
        (success, error) = saveDataMonitor(data)
    elif datatype == lmkey.DATATYPE_MONITOR_TOMCAT:
        (success, error) = saveDataMonitor(data)        
        
        
    return jsonify({'success': success, "detail" : error})

#%%
    
def measureAndPredict():
    try:
        start = datetime.datetime.now()
        end = lmutil.getLogMapperIntervalDate()
        start = end - datetime.timedelta(minutes = 3)
        conn=db.connectDb()
        predictor.measureAndPredict(conn, start, end)
        conn.commit() 
        conn.close()  
    except Exception as exc:
        logger.exception("exception at measureAndPredict")    
    
       
def saveDataAgent(data):
    logger.debug("saveDataAgent")
    
    agentkey =  data['agentKey'] 
    name = data['agentKey']
    ip = data['agentIp']
    port = data['agentPort']  
    hostName = data['host']
    
    conn=db.connectDb()
    cursor = conn.cursor()
        
    host = masterdao.findHostByKey(cursor, hostName)
    if not host:
        host = masterdao.createHost(conn, hostName, hostName)
            
    agent = masterdao.findAgentByKey(cursor, agentkey)
    
    if agent:
        return (False, "Agent already exist: "+str(agent))

    agent = masterdao.createAgent(conn, agentkey, name, host['id'], ip, port, True)
    return (True, str(agent))
        
def saveDataPathMetrics(data):
    logger.debug("saveDataPathMetrics")
    
    conn=db.connectDb()
    cursor = conn.cursor()
           
    key = data['readerKey']+"-"+lmkey.SOURCE_TYPE_READER   
    source = masterdao.findSourceByKey(cursor, key)
    if not source:
        logger.warning("source not found")
        return (False, "source not found") 
    
    masterdao.createSourceCounters(cursor, datetime.datetime.now(), source['id'], 
                                   data['lmstats']['count'], data['lmstats']['bytes'],
                                   data['lmstats']['records'], data['lmstats']['fails'])    
    
    hostId = source['hostId']
    monitorMeasures = data['pathMeasures']
      
    for item in monitorMeasures:     
        key = source['name']+"_"+str(item[2])
        pathId = masterdao.findLogPathIdByKeyMaster(cursor, key)
        
        if not pathId: 
            logger.warning("logPath not found")
            return (False, "path not found")

        masterdao.createPathMeasure(cursor, item[1], lmutil.calcTimeGroup(lmutil.datetimeParse(item[1])), pathId, hostId, 
                          item[3], item[4], item[5], item[6])
    
    conn.commit() 
    conn.close()
    
    return (True, None)     
    
    
    
def saveDataLogEvents(data):
    logger.debug("saveDataLogEvents")
    
    conn=db.connectDb()
    cursor = conn.cursor()
           
    key = data['readerKey']+"-"+lmkey.SOURCE_TYPE_READER   
    source = masterdao.findSourceByKey(cursor, key)
    if not source:
        logger.warning("source not found")
        return (False, "source not found") 
    
    monitorMeasures = data['logEventsCount']
      
    for item in monitorMeasures:     
#        logger.debug(item)       
        measureType = masterdao.findMeaureTypeByDataTypeAndIndex(cursor, data['datatype'], item[2])      
        if not measureType:
            logger.warning("measure type not found")
            continue         
        masterdao.createMeasure(cursor, item[1], lmutil.calcTimeGroup(lmutil.datetimeParse(item[1])), measureType['id'], source['id'], item[3])
    
    conn.commit() 
    conn.close()
    
    return (True, None) 

    
def saveDataLogMetrics(data): 
    logger.debug("saveDataMonitorHost")
    
    conn=db.connectDb()
    cursor = conn.cursor()
           
    key = data['readerKey']+"-"+lmkey.SOURCE_TYPE_READER   
    source = masterdao.findSourceByKey(cursor, key)
    if not source:
        logger.warning("source not found")
        return (False, "source not found") 
    
    monitorMeasures = data['metrics']
      
    for item in monitorMeasures:     
        date = item[1]
        period = lmutil.calcTimeGroup(lmutil.datetimeParse(date))
         
        index = 0
        for metric in item:           
            measureType = masterdao.findMeaureTypeByDataTypeAndIndex(cursor, lmkey.DATATYPE_LOG_METRICS, index)   
            index+=1
            if not measureType:
                logger.warning("measure type not found:" +str(index-1))
                continue  
            if not measureType['enable']:
                continue
            masterdao.createMeasure(cursor, date, period, measureType['id'], source['id'], metric) 
    
    conn.commit() 
    conn.close()
    
    return (True, None)     
     
    
def saveDataMonitor(data):
    logger.debug("saveDataMonitorHost")
    
    conn=db.connectDb()
    cursor = conn.cursor()
    
    key = data['monitorKey']+"-"+data['sourcetype']
    
    source = masterdao.findSourceByKey(cursor, key)
    if not source:
        logger.warning("source not found")
        return
    
    masterdao.createSourceCounters(cursor, datetime.datetime.now(), source['id'], 
                                   data['lmstats']['count'], data['lmstats']['bytes'],
                                   data['lmstats']['records'], data['lmstats']['fails'])    
    
    hostId = source['hostId']
    monitorMeasures = data['measures']
      
    for item in monitorMeasures:     
        date = item[0]
        period = lmutil.calcTimeGroup(lmutil.datetimeParse(date))
         
        if data['sourcetype'] == lmkey.SOURCE_TYPE_HOST: 
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
    conn.close()
    
    return (True, None)      

#%%  
if __name__ == '__main__':
    print('Start module execution:')
#    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')   
   
    logging_util.configureLogger('/logmapper/log/logmapper-master.log')    
    
#    ch = logging.StreamHandler(sys.stdout)
#    ch.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
#    ch.setLevel(logging.DEBUG)    
#    app.logger.addHandler(ch)
#    app.logger.info("starting")
    
  

    cfg.createDefaultConfigfile()
    config=cfg.loadConfig() 
    
    conn=db.connectDb()
    masterdao.createTablesBase(conn)
    masterdao.createTablesControl(conn)
    conn.close()

    app.run(host='0.0.0.0', port=5005, debug=True)
