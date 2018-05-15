# -*- coding: utf-8 -*-
"""
Created on Thu Feb  8 11:48:11 2018

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
import logmappermaster.dao.master_dao as masterdao



#%%
"""
Global Initialization. Constants definitions.
"""

logger = logging.getLogger(__name__)


#%%

def processOutliers(connDbMaster, start, end):
    cursor = connDbMaster.cursor()  
            
    hosts = masterdao.findHosts(cursor)
#    hosts = [ masterdao.findHostByKey(cursor, "device") ]
    for host in hosts:
        components = masterdao.findComponentsByHostId(cursor, host['id'])
        for component in components:
            logger.debug('Process:'+str(component))   
            rowsPaths = masterdao.findLogPathsByComponentId(cursor, component['id'])
            
            for rowpath in rowsPaths:
                pathId = rowpath[0]
                (count, avg, std, maxv) = masterdao.findReferencePathMeasures(cursor, pathId, start, end, True)
                if avg == None or std == None: continue
                if std == 0: std =0.1
                vmax = avg + 3*std
                masterdao.updatePathMeasureRefFlagByAvgRange(cursor, pathId, start, end, vmax, False)                
                
            connDbMaster.commit()  


def calculatePerformanceReference(connDbMaster, start, end, resetReference=False):
    cursor = connDbMaster.cursor()  
        
    if resetReference:
        masterdao.resetLogPathDurationData(cursor)
        connDbMaster.commit()
    
    hosts = masterdao.findHosts(cursor)
    for host in hosts:
        components = masterdao.findComponentsByHostId(cursor, host['id'])
        for component in components:
            logger.debug('Process:'+str(component))   
            rowsPaths = masterdao.findLogPathsByComponentId(cursor, component['id'])
            
            for rowpath in rowsPaths:
                pathId = rowpath[0]
                (count, avg, std, maxv) = masterdao.findReferencePathMeasures(cursor, pathId, start, end, True)
                masterdao.updateLogPathDurationData(cursor, pathId, count, avg, std, maxv, start, end )
            connDbMaster.commit()      

    
if __name__ == '__main__':
    print('Start module execution:')
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')     

    cfg.createDefaultConfigfile()
    config=cfg.loadConfig()   
    
#==============================================================================
    
    start = datetime.datetime(2018, 4, 22, 16, 0, 0)
    end =   datetime.datetime(2018, 4, 22, 18, 30, 0)   
    
    start = datetime.datetime(2018, 4, 22, 19, 0, 0)
    end =   datetime.datetime(2018, 4, 22, 19, 38, 0)  

    start = datetime.datetime(2018, 4, 23, 7, 0, 0)
    end =   datetime.datetime(2018, 4, 23, 12, 0, 0)     
    
    start = datetime.datetime(2018, 4, 23, 15, 33, 0)
    end =   datetime.datetime(2018, 4, 23, 23, 0, 0)     
    
    start = datetime.datetime(2018, 4, 24, 20, 0, 0)
    end =   datetime.datetime(2018, 4, 25, 6, 0, 0)      

    start = datetime.datetime(2018, 4, 26, 22, 0, 0)
    end =   datetime.datetime(2018, 4, 27, 8, 0, 0)   
    
    
    #rANGO PARA CALCULAR REF
    start = datetime.datetime(2018, 4, 22, 16, 0, 0)
    end =   datetime.datetime(2018, 4, 28, 18, 0, 0)      
    

#==============================================================================
# 
#============================================================================== 

    connDbMaster=db.connectDb() 
    cursor = connDbMaster.cursor()
 
#    masterdao.updateMeasureRefFlag(cursor, start, end, True)
#    connDbMaster.commit()
#    
    processOutliers(connDbMaster, start, end)
#    
#    calculatePerformanceReference(connDbMaster, start, end, resetReference=True)
    
    connDbMaster.close()
 
    print("End module execution:"+str(datetime.datetime.now()))