# -*- coding: utf-8 -*-
"""
Created on Fri Feb  9 14:47:50 2018

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
import matplotlib.pyplot as plt

from sklearn.externals import joblib
from sklearn.model_selection import train_test_split
from sklearn import svm
from sklearn import linear_model
from sklearn.metrics import mean_squared_error, r2_score
import sklearn.neural_network.multilayer_perceptron as nn
    
import config.config as cfg
import logmappercommon.utils.postgres_util as db
import logmappercommon.utils.logmapper_util as lmutil
import logmappermaster.dao.master_dao as masterdao
import logmappermaster.ml.transform_data as transform

#%%
"""
Global Initialization. Constants definitions.
"""

logger = logging.getLogger(__name__)

EMPTY_CHARACTER = np.nan #'?'
EMPTY_VALUE=0
FOLDER_MODELS='/logmapper/models/'


    
    
#==============================================================================
# NUEVO ***********************************************************
#============================================================================== 

def findBestSvmParameters(dft):
    
    colnamesAnom = ['error', 'errorTrain', 'description']
    errorTestList = [] 
    errorTrainList = [] 
    descriptionAnomList = []
    
    
    errorMin = 100
    nuBest = None
    gammaBest = None
    
    nu = 0.01
    gamma=0.01    
    while nu < 1:
        while gamma < 1:
            model, errorTest, errorTrain, description = learnAnomalies(dft, nu, gamma)
            errorTestList.append(errorTest)
            errorTrainList.append(errorTrain)
            descriptionAnomList.append(description)  
            if errorTest != None and errorTest < errorMin:
                errorMin = errorTest
                nuBest = nu
                gammaBest = gamma
            gamma += 0.01
        nu += 0.01
        
    dataList= [ errorTestList, errorTrainList, descriptionAnomList]
    zipped = list(zip(colnamesAnom, dataList))
    dfsvm = pd.DataFrame(dict(zipped))
    dfsvm = dfsvm[colnamesAnom]
    
    return dfsvm, nuBest, gammaBest, errorMin
            

    
    
def learnAnomalies(df, valNu=0.1, valGamma=0.1):
    if len(df) < 10: return None, None, None, "No data"
    
    X_train, X_test = train_test_split(df, test_size=0.3, random_state=0)
    
    
#==============================================================================
# ejecuta algoritmos de SVM
#==============================================================================  
# http://scikit-learn.org/stable/auto_examples/svm/plot_oneclass.html#sphx-glr-auto-examples-svm-plot-oneclass-py    

    # fit the model kernel=RBF, POLYNOMIAL, LINEAR, SIGMOID
    model = svm.OneClassSVM(nu=valNu, kernel="rbf", gamma=valGamma)
    #model = svm.OneClassSVM(nu=0.1, kernel="linear")
    #model = svm.OneClassSVM(nu=0.1, kernel="poly", degree=2, coef0=0.0)
    #model = svm.OneClassSVM(nu=0.1, kernel="sigmoid ", coef0=0.0)
    model.fit(X_train)
    y_pred_train = model.predict(X_train)
    n_error_train = y_pred_train[y_pred_train == -1].size    
    error_train = (n_error_train / y_pred_train.size) *100
     
    y_pred_test = model.predict(X_test)
    n_error_test = y_pred_test[y_pred_test == -1].size  
    error_test = (n_error_test / y_pred_train.size)*100  

    description = 'nu,gamma,ntrain,ntest: {:.2f},{:.2f},{:d},{:d}'.format(valNu, valGamma, y_pred_train.size, y_pred_test.size)
         

    return model, error_test, error_train, description
    
 
# ==============================================================================
# ejecuta algoritmos de REGRESION LINEAL
#============================================================================== 
#http://scikit-learn.org/stable/modules/linear_model.html#ordinary-least-squares
# split data into train and test  
def learnPerformance(df):
    if len(df) < 1: return None, None, None, "No data"
    X_train, X_test, y_train, y_test = train_test_split(df.iloc[:, 0:-1], df['PERFORMANCE'], test_size=0.3, random_state=0)
    
     
    model = linear_model.LinearRegression()
    
    # Train the model using the training sets
    model.fit (X_train, y_train)
    
    # Make predictions using the testing set
    y_test_pred = model.predict(X_test)
    #cut response between [0, 1]
    alimit = np.zeros(len(X_test))
    y_test_pred = np.maximum(y_test_pred, alimit)
    alimit = np.ones(len(X_test))
    y_test_pred = np.minimum(y_test_pred, alimit)
    
    # The coefficients
#    logger.info('Coefficients: \n', model.coef_)
    # The mean squared error
    mse = mean_squared_error(y_test, y_test_pred)
    # Explained variance score: 1 is perfect prediction
    r2 = r2_score(y_test, y_test_pred)
    logger.info("ML:Mean squared error: %.2f" % mse)
    
    logger.info('ML:Variance score: %.2f' % r2)    
    description = "REGR: samples: "+str(len(X_train))+", "+str(len(X_test))

    return model, mse, r2, description     



if __name__ == '__main__':
    print('Start module execution:')
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')     

    cfg.createDefaultConfigfile()
    config=cfg.loadConfig()   
    
#==============================================================================
    

    
#==============================================================================
    connDbMaster=db.connectDb()
     
    cursor = connDbMaster.cursor() 
    hosts = masterdao.findHosts(cursor)
#    hosts = [ masterdao.findHostByKey(cursor, "device") ]
    
    componentList = []
    
    colnamesAnom = ['component', 'error', 'description']
    errorTestList = [] 
    errorTrainList = []
    descriptionAnomList = []
    
    colnamesRegr = ['component', 'mse', 'r2', 'description']
    mseList = []
    r2List = []
    descriptionRegrList = []
    
    for host in hosts:
        components = masterdao.findComponentsByHostId(cursor, host['id'])
        for component in components:
            logger.debug('Process:'+str(component))   
            
            mTypes = masterdao.findMeasuresTypesByComponent(cursor, component['id'])           
            df = pd.read_csv('/logmapper/data/arrange_'+component['key']+'.csv')
            df.set_index('date', inplace=True)
            
            componentList.append(component['name'])
           
            dft = transform.selectAndTransformMetricsAndEvents(df, mTypes)
            dfsvm, nuBest, gammaBest, errorMin = findBestSvmParameters(dft)
            if nuBest:
                logger.debug("Best: nu={:f} gamma={:f} errorMin={:f}".format(nuBest, gammaBest, errorMin))            
                model, errorTest, errorTrain, description = learnAnomalies(dft, nuBest, gammaBest)
                joblib.dump(model, FOLDER_MODELS+'anom_'+component['key'])
            else:
                logger.debug("No best found:"+str(nuBest)+","+str(gammaBest))
                model, errorTest, errorTrain, description = (None, None, None, "Error")
                
                
            errorTestList.append(errorTrain)
            errorTrainList.append(errorTest)
            descriptionAnomList.append(description)
            
            
#            dft = transform.selectAndTransformAll(df, mTypes)
            dft = transform.selectAndTransformMetricsAndPerformance(df, mTypes)    
            model, mse, r2, description = learnPerformance(dft)
            Y = model.predict(dft.iloc[:, 0:-1])
            #cut response between [0, 1]
            alimit = np.zeros(len(dft))
            Y = np.maximum(Y, alimit)
            alimit = np.ones(len(Y))
            Y = np.minimum(Y, alimit)            
            dft['predicted'] = Y
            dft[['PERFORMANCE', 'predicted']].plot()
            mseList.append(mse)
            r2List.append(r2)
            descriptionRegrList.append(description) 
            joblib.dump(model, FOLDER_MODELS+'perf_'+component['key'])
            
    connDbMaster.close()  
    
    dataList= [componentList, errorTestList, descriptionAnomList]
    zipped = list(zip(colnamesAnom, dataList))
    dfanomalies = pd.DataFrame(dict(zipped))
    dfanomalies = dfanomalies[colnamesAnom]
    
    print(dfanomalies)
    
    dataList= [componentList, mseList, r2List, descriptionRegrList]
    zipped = list(zip(colnamesRegr, dataList))
    dfregr = pd.DataFrame(dict(zipped))
    dfregr = dfregr[colnamesRegr]  
    print(dfregr)


 
    print("End module execution:"+str(datetime.datetime.now())) 