# -*- coding: utf-8 -*-
"""
Created on Fri Feb  9 14:48:50 2018

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
import logmappermaster.ml.transform_data as transform
import logmappermaster.collector.arrange_data as arrange
import logmappermaster.performance.performace_measure as performance

from sklearn.externals import joblib

#%%
"""
Global Initialization. Constants definitions.
"""

logger = logging.getLogger(__name__)

EMPTY_CHARACTER = np.nan #'?'
EMPTY_VALUE=0
FOLDER_MODELS='/logmapper/models/'

#%%
def predictAnomalies(key, df, mTypes):
#    logger.debug('predictAnomalies: '+key)

    dft = transform.selectAndTransformMetricsAndEvents(df, mTypes)     
    if len(dft) == 0: 
        logger.warning("No data")
        return pd.DataFrame()
    
    if not os.path.isfile(FOLDER_MODELS+'anom_'+key):
        logger.warning("Model file not found")
        return pd.DataFrame()
        
    model = joblib.load(FOLDER_MODELS+'anom_'+key) 
    dft['anomaly'] = model.predict(dft) 
    return dft

def predictPerformance(key, df, mTypes):
#    logger.debug('predictPerformance: '+key)

    dft = transform.selectAndTransformMetrics(df, mTypes)        
    if len(dft) == 0: 
        logger.warning("No data")
        return pd.DataFrame()
    
    if not os.path.isfile(FOLDER_MODELS+'perf_'+key):
        logger.warning("Model file not found")
        return pd.DataFrame()

    model = joblib.load(FOLDER_MODELS+'perf_'+key) 
    Y = model.predict(dft) #.iloc[:, 0:-1]) 
    #cut response between [0, 1]
    alimit = np.zeros(len(dft))
    Y = np.maximum(Y, alimit)
    alimit = np.ones(len(Y))
    Y = np.minimum(Y, alimit)  
    dft['predicted'] = Y
    return dft

       

#%%
"""
*******************************************************************************
CREA DATOS TIPO PANDA CON MATRIZ PARA PROCESAMIENTO DE ARCHIVOS
*******************************************************************************
"""

def measureAndPredict(connDbMaster, start, end):
    
    performance.calcPathsPerformance(connDbMaster, start, end)
    
    cursor = connDbMaster.cursor()    
    hosts = masterdao.findHosts(cursor)
    for host in hosts:
        components = masterdao.findComponentsByHostId(cursor, host['id'])
        for component in components:
            logger.debug('Process:'+str(component)) 
             
            mTypes = masterdao.findMeasuresTypesByComponent(cursor, component['id'])
            
            performance.calcPerformanceByComponent(connDbMaster, component['id'], component['hostId'], start, end)
            
            df = arrange.arrangeComponentData(cursor, component['id'], component['hostId'], start, end, ref=None)
            logger.debug("len(df)="+str(len(df)))
            
            anomalyPredicted = predictAnomalies(component['key'], df, mTypes)
            for index, row in anomalyPredicted.iterrows():
                resultId = masterdao.findPerfomanceMeasure(cursor, index, component['id'])
                if resultId:                
                    masterdao.updateAnomalyPredicted(cursor, resultId['id'], row['anomaly'])
                else:
                    period = lmutil.calcTimeGroup(index)
                    masterdao.createPerfomanceMeasureWithAnomaly(cursor, index, period, component['hostId'], component['id'], row['anomaly'])
            connDbMaster.commit()    
            
            perfPredicted = predictPerformance(component['key'], df, mTypes)           
            for index, row in perfPredicted.iterrows():
                resultId = masterdao.findPerfomanceMeasure(cursor, index, component['id'])
                if resultId:                               
                    masterdao.updatePerformancePredicted(cursor, resultId['id'], row['predicted'])
            connDbMaster.commit()             
              
#            if len(df) > 5:
#                anomalyPredictedDf = predictAnomalies(component['key'], df, mTypes)
#                if anomalyPredictedDf == None or len(anomalyPredictedDf) == 0:
#                    logger.debug("No data, Continue")
#                    continue
            
#            for i in range(len(df)):
#                dfs = df.iloc[i:i+1]
#                anomalyPredicted = predictAnomalies(component['key'], dfs, mTypes)
#                perfPredicted = predictPerformance(component['key'], dfs, mTypes)
#                if anomalyPredicted != None:
#                    masterdao.updatePerfomanceMeasure(cursor, dfs.index[0], component['id'], perfPredicted, anomalyPredicted)
                    
                

                    
            connDbMaster.commit()     

#%%

if __name__ == '__main__':
    print('Start module execution:')
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')     

    cfg.createDefaultConfigfile()
    config=cfg.loadConfig()   
    
#==============================================================================
    
    start = datetime.datetime(2018, 4, 22, 16, 0, 0)
    end =   datetime.datetime(2018, 4, 28, 17, 0, 0) 
    
#    start = datetime.datetime(2018, 4, 25, 15, 0, 0)
#    end =   datetime.datetime(2018, 4, 25, 16, 0, 0)     

    connDbMaster=db.connectDb('master')
    measureAndPredict(connDbMaster, start, end)
    connDbMaster.close()
 
    print("End module execution:"+str(datetime.datetime.now())) 