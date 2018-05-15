# -*- coding: utf-8 -*-
"""
Created on Wed Feb 28 22:19:34 2018

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
import logmappercommon.utils.postgres_util as db
import logmappercommon.utils.logmapper_util as lmutil
import logmappermaster.dao.master_dao as masterdao

#%%
"""
Global Initialization. Constants definitions.
"""

logger = logging.getLogger(__name__)



#%%
def mapRemoteCalls(conn, start, end):
    logger.debug('mapRemoteCalls:')
    
    cursor = conn.cursor()
    
    rows = masterdao.findRemoteCallKeys(cursor, start, end)
    
    listRc = []
    for row in rows:
#        logger.debug("remoteCall:"+str(row))
        records = masterdao.findRemoteCallRecords(cursor, row[0])
        c1 = None
        c2 = None
        n1 = None
        n2 = None
        for record in records:
            n2 = record[0]
            c2 = record[1]
            if c1 and c2 and c1 != c2:
                if not (n2, n1) in listRc:
                    masterdao.createRemoteCall(cursor, n1, n2)
                listRc.append((n1, n2))
            n1 = record[0]
            c1 = record[1]
            
    conn.commit()


#%%

if __name__ == '__main__':
    print('Start module execution:')
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')     

    cfg.createDefaultConfigfile()
    config=cfg.loadConfig()   
    
#==============================================================================
    
    start = datetime.datetime(2018, 4, 22, 20, 0, 0)
    end =   datetime.datetime(2018, 4, 27, 12, 0, 0) 

    connDbMaster=db.connectDb('master')    
    mapRemoteCalls(connDbMaster, start, end)   
    connDbMaster.close()
 
    print("End module execution") 