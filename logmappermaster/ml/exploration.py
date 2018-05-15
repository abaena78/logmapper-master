# -*- coding: utf-8 -*-
"""
Created on Thu Apr 26 06:14:45 2018

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

import matplotlib.pyplot as plt

#%%
"""
Global Initialization. Constants definitions.
"""

logger = logging.getLogger(__name__)

EMPTY_CHARACTER = np.nan #'?'
EMPTY_VALUE=0

COLNAME_PERFORMANCE = 'PERFORMANCE'
COLNAME_PERFORMANCE_MIN = 'PERFORMANCE_MIN'
COLNAME_DATE = 'date'
COLNAME_PERIOD = 'period'


#%%

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

def selectData(df, mTypes):
    colnames = []
    for cType in mTypes:
        colnames.append(cType['name'])
    
    colnames.append(COLNAME_PERFORMANCE)
    colnames.append(COLNAME_PERFORMANCE_MIN)
    return df[colnames].dropna()


def describeDataComponent(df):
    df1 = pd.DataFrame()
    
    df1['mean'] = df.mean()
    df1['min'] = df.min()
    df1['max'] = df.max()
    df1['std'] = df.std()
    df1['count'] = df.count()
    
    return df1

def corrMatrix(df):
    from matplotlib import pyplot as plt
    from matplotlib import cm as cm

    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    cmap = cm.get_cmap('jet', 8)
    cax = ax1.imshow(df.corr(), interpolation="nearest", cmap=cmap)
    ax1.grid(False)
    plt.title('Correlation Matrix')
#    labels=['Sex','Length','Diam','Height','Whole','Shucked','Viscera','Shell','Rings',]
#    ax1.set_xticklabels(labels,fontsize=6)
#    ax1.set_yticklabels(labels,fontsize=6)
    # Add colorbar, make sure to specify tick locations to match desired ticklabels
    fig.colorbar(cax, ticks=[-1, -0.75, -0.5, -.25, 0, .25,.5,.75,1])
    plt.show()
    fig.savefig("/tmp/corr.png")

    columns = ('id', 'var name')
    dataTable = []
    i=0
    for colname in df.columns:
        dataTable.append([str(i), colname])
#        dataTable.append()
        i +=1
     
    
    fig = plt.figure()
    ax = fig.add_subplot(111)
    plt.table(cellText=dataTable, colLabels=columns, loc='top')
    ax.axis('off')
    ax.axis('tight')    
    plt.show()
    fig.savefig("/tmp/corrtable.png")    
        
       
#%%

if __name__ == '__main__':
    print('Start module execution:')
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')     

    cfg.createDefaultConfigfile()
    config=cfg.loadConfig() 
    
    connDbMaster=db.connectDb()
    cursor = connDbMaster.cursor()
    
#==============================================================================
#   Exploration by component
#==============================================================================
     
    hosts = masterdao.findHosts(cursor)
    hosts = [ masterdao.findHostByKey(cursor, "smsecurity") ]   
    for host in hosts:
        components = masterdao.findComponentsByHostId(cursor, host['id'])
        for component in components:
            logger.debug('Process:'+str(component))   
            
            df = pd.read_csv('/logmapper/data/arrange_'+component['key']+'.csv')
            df.date = pd.to_datetime(df.date)
            df.set_index('date', inplace=True)
            
            mTypes = masterdao.findMeasuresTypesByComponent(cursor, component['id'])
            dfs = selectData(df, mTypes)
            describeDataComponent(dfs)
            
            #add manually performance columns
            mTypes.append({'name' : COLNAME_PERFORMANCE})
            mTypes.append({'name' : COLNAME_PERFORMANCE_MIN})
            
            for mType in mTypes:
                logger.debug(str(mType))
                if mType['name'] == 'COUNT(fail)': continue
            
                fig = plt.figure()
                ax = fig.add_subplot(2, 2, 1)
                             
                x = dfs[mType['name']] 
                y = dfs[COLNAME_PERFORMANCE]
                plt.ylabel('Perf')
                ax.scatter(x, y)
                plt.title(mType['name'])
                
                ax = fig.add_subplot(2, 2, 2)
                df[mType['name']].plot(ax=ax, kind='line', style='o')
                
                ax = fig.add_subplot(2, 2, 3)
                df[[mType['name']]].boxplot(ax=ax)

                ax = fig.add_subplot(2, 2, 4)
                df[[mType['name']]].hist(ax=ax)                
                
                plt.show()
                fig.savefig("/tmp/description_"+mType['name']+".png")
                
            corrMatrix(dfs)

              
            
#==============================================================================
#   Exploration by variable 
#==============================================================================
            
#    mtypes = masterdao.findAllMeaureType(cursor)
#    
#    for mtype in mtypes: 
#        dfv = pd.DataFrame()
#        hosts = masterdao.findHosts(cursor)
#        hosts = [ masterdao.findHostByKey(cursor, "web") ]   
#        for host in hosts:
#            components = masterdao.findComponentsByHostId(cursor, host['id'])
#            for component in components:
#                logger.debug('Process:'+str(component))   
#                
#                df = pd.read_csv('/logmapper/data/arrange_'+component['key']+'.csv')
#                dfv[component['key']] = df[mtype['name']]


    connDbMaster.close()
 
    print("End module execution:"+str(datetime.datetime.now()))  