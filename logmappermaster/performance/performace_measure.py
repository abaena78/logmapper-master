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
import json
import csv

import math 
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
    
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

def performanceCalculator(count, avg, std, maxv, countref, avgref, stdref, maxvref):
    """ 
    ===========================================================================
    Performance calculator function
    =========================================================================== 
    Calculate performance based on reference values.
    If some value is None return None
    
    **Args**:
        * count : actual number of samples -- (int)
        * avg   :  actual duration average -- (float)
        * std   :  actual duration standar desviation -- (float)
        * maxv  :  actual duration max value -- (float)
        * countref : reference number of samples -- (int)
        * avgref   :  reference duration average -- (float)
        * stdref   :  reference duration standar desviation -- (float)
        * maxvref  :  reference duration max value -- (float)
    **Returns**:
        performance value indicator. [0-1] -- (float)
    """  
    if avgref == None or stdref == None or maxvref == None:
        return None
  
#    f = 0
#    if maxvref == 0: maxvref = 0.1
#    if maxvref < 0.2:
#          f = avg / (maxvref * 10) 
#    if maxvref < 1:
#        f = avg / (maxvref * 5)  
#    if maxvref < 10:
#        f = avg / (maxvref * 2)
#        
#    f = 0
#    if maxvref == 0: maxvref = 0.1
#    if maxvref < 0.2:
#          f = avg / (maxvref * 5) 
#    elif maxvref < 1:
#        f = avg / (maxvref * 3)  
#    elif maxvref < 10:
#        f = avg / (maxvref * 1.5)  
#    else:
#        f = avg / maxvref
#    f = 1-f
     
    if stdref < 0.01: stdref = 0.01
    
    f = (1-((avg - avgref) / (stdref*2)))*0.9
    if f > 1: f=1
    if f < 0: f=0
    
    return f

def performanceMinCalculator(count, avg, std, maxv, countref, avgref, stdref, maxvref):
    """ 
    ===========================================================================
    Performance calculator function using max value
    This would be the worst case
    =========================================================================== 
    Calculate performance based on reference values.
    If some value is None return None
    
    **Args**:
        * count : actual number of samples -- (int)
        * avg   :  actual duration average -- (float)
        * std   :  actual duration standar desviation -- (float)
        * maxv  :  actual duration max value -- (float)
        * countref : reference number of samples -- (int)
        * avgref   :  reference duration average -- (float)
        * stdref   :  reference duration standar desviation -- (float)
        * maxvref  :  reference duration max value -- (float)
    **Returns**:
        performance value indicator. [0-1] -- (float)
    """  
    if avgref == None or stdref == None or maxvref == None:
        return None
  
    if stdref < 0.01: stdref = 0.01
       
    f = (1-((maxv - avgref) / (stdref*2)))
    if f > 1: f=1
    if f < 0: f=0    
    
    return f

#%% 

    

def calcPerformanceByComponent(connDbMaster, componentId, hostId, start, end):
    cursor = connDbMaster.cursor()
    while start < end: 
        endLoop = start + datetime.timedelta(minutes = cfg.PERIOD)
        period = lmutil.calcTimeGroup(start)
        data = masterdao.findPathMeasuresByDateAndComponentId(cursor, start, componentId)
        logger.debug(str(len(data)))
        acc_count=0
        acc_prod=0
        acc_prod_min=0
        for d in data:
            logger.debug(d)
            acc_count += d[0]
            acc_prod += d[0]*d[1]
            acc_prod_min += d[0]*d[2]
        if acc_count == 0: 
            start = endLoop 
            continue
            
        performance = acc_prod / acc_count
        performanceMin = acc_prod_min / acc_count
        logger.debug("acc_count="+str((acc_prod, acc_prod)))

                
        g1avg = masterdao.countPathMeasuresByDateAndComponentIdAndPerformanceRange(cursor, start, componentId, 0, 0.25)
        g2avg = masterdao.countPathMeasuresByDateAndComponentIdAndPerformanceRange(cursor, start, componentId, 0.25, 0.5)
        g3avg = masterdao.countPathMeasuresByDateAndComponentIdAndPerformanceRange(cursor, start, componentId, 0.5, 0.75)
        g4avg = masterdao.countPathMeasuresByDateAndComponentIdAndPerformanceRange(cursor, start, componentId, 0.75, 1.1)
        
        g1min = masterdao.countPathMeasuresByDateAndComponentIdAndPerformanceMinRange(cursor, start, componentId, 0, 0.25)
        g2min = masterdao.countPathMeasuresByDateAndComponentIdAndPerformanceMinRange(cursor, start, componentId, 0.25, 0.5)
        g3min = masterdao.countPathMeasuresByDateAndComponentIdAndPerformanceMinRange(cursor, start, componentId, 0.5, 0.75)
        g4min = masterdao.countPathMeasuresByDateAndComponentIdAndPerformanceMinRange(cursor, start, componentId, 0.75, 1.1)        

        masterdao.createPerfomanceMeasure(cursor, start, period, hostId, componentId, performance, performanceMin,
                            g1avg, g2avg, g3avg, g4avg, g1min, g2min, g3min, g4min)

        start = endLoop 
        
    connDbMaster.commit() 
    

def calcPathsPerformance(connDbMaster, start, end):
    cursor = connDbMaster.cursor()    
    rows = masterdao.findPathMeasuresAndReference(cursor, start, end)
    for row in rows:
        logger.debug('Process:'+str(row))
        (idPathMeasure, count, avg, std, maxv, countref, avgref, stdref, maxvref) =(
                row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8])
        performance = performanceCalculator(count, avg, std, maxv, countref, avgref, stdref, maxvref)
        performanceMin = performanceMinCalculator(count, avg, std, maxv, countref, avgref, stdref, maxvref)
        masterdao.updatePathMeasurePerfomance(cursor, idPathMeasure, performance, performanceMin)
    connDbMaster.commit()  

def calcComponentsPerformance(connDbMaster, start, end):  
    cursor = connDbMaster.cursor()      
    hosts = masterdao.findHosts(cursor)
    for host in hosts:
        components = masterdao.findComponentsByHostId(cursor, host['id'])
        for component in components:
            logger.debug('Process:'+str(component)) 
            calcPerformanceByComponent(connDbMaster, component['id'], component['hostId'], start, end)
            
            
def plotPerformanceCalculator():
    count = 1
    std = 1
    maxv = 6
    avg = 5
    countref = 1
    avgref=5
    stdref = 2
    maxvref=10
    
    X = np.arange(0.0, 15.0, 0.1)   
    perf = []
    for maxv in X:
        perf.append(performanceCalculator(count, avg, std, maxv, countref, avgref, stdref, maxvref))
    
    fig = plt.figure()
    ax = fig.add_subplot(111)    
    ax.plot(X, perf)
    ax.grid()
    
    ax.set_title('Performance function')
    ax.set_xlabel('actual duration')
    ax.set_ylabel('performance')    
    
    ax.plot([5], [0.9], 'o')
    ax.annotate('$perf(5)=0.9$', xy=(5, 0.9), xytext=(7, 0.9),
                arrowprops=dict(facecolor='black', shrink=0.05))    
    
    ax.text(10, 0.8, '$avgref=5, stdref=2$',
            bbox={'facecolor':'white', 'alpha':0.5, 'pad':5})    
    
    
#    fig.savefig("/tmp/performance.png")
    plt.show()
    
        
        
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
    
#    start = datetime.datetime(2018, 4, 28, 14, 30, 0)
#    end =   datetime.datetime(2018, 4, 28, 15, 30, 0)     

#==============================================================================
# calcula rendimeinto por pathMeasure
#==============================================================================
    
#    plotPerformanceCalculator()
    
    connDbMaster=db.connectDb()
    calcPathsPerformance(connDbMaster, start, end)
    calcComponentsPerformance(connDbMaster, start, end)
    connDbMaster.close()

 
    print("End module execution:"+str(datetime.datetime.now()))