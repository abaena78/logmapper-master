# -*- coding: utf-8 -*-
"""
Created on Fri Apr 20 18:02:56 2018

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


def executeAgentsCommand(conn, command):
    cursor = conn.cursor()
    agents = masterdao.findAgentsEnabled(cursor)
    for agent in agents:
        client = agentclient.LogMapperAgentClient(agent['ip'], agent['port'])
        try: 
            print("Execute in "+str(agent))
            client.open()
            r = client.executeCommand(command)
            #print(r)
            client.close()               
        except Exception as exc:
            print("Exception collecting data from "+str(exc))
        
        
        
        
#%%
    
if __name__ == '__main__':
    print('Start module execution:')
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')     

    cfg.createDefaultConfigfile()
    config=cfg.loadConfig() 

    conn=db.connectDb()    
    command = input("Enter command:")
    executeAgentsCommand(conn, command)
            
        
    conn.close()

    print("End module execution:"+str(datetime.datetime.now()))    