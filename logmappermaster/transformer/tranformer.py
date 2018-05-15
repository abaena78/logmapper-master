# -*- coding: utf-8 -*-
"""
Created on Sat Jan 20 09:03:36 2018

@author: abaena
"""

#******************************************************************************
#Add logmapper-agent directory to python path for module execution
#******************************************************************************
if __name__ == '__main__':    
    import os, sys
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname( __file__ ), '..','..'))) 
#******************************************************************************

import logmappermaster.transformer.fuzzysystems as fuzzysystems


import logging

logger = logging.getLogger(__name__)

#%%

cnxs_fuzzysim = fuzzysystems.cnxs.buildFuzzySystem()  
counter_fuzzysim = fuzzysystems.counter.buildFuzzySystem() 
cpu_fuzzysim = fuzzysystems.cpu.buildFuzzySystem() 
disk_io_rate_r_fuzzysim = fuzzysystems.disk_io_rate_r.buildFuzzySystem() 
disk_io_rate_w_fuzzysim = fuzzysystems.disk_io_rate_w.buildFuzzySystem() 
diskusage_fuzzysim = fuzzysystems.diskusage.buildFuzzySystem() 
memory_fuzzysim = fuzzysystems.memory.buildFuzzySystem() 
net_err_rate_fuzzysim = fuzzysystems.net_err_rate.buildFuzzySystem()  
net_io_rate_in_fuzzysim = fuzzysystems.net_io_rate_in.buildFuzzySystem() 
net_io_rate_out_fuzzysim = fuzzysystems.net_io_rate_out.buildFuzzySystem() 
openfiles_fuzzysim = fuzzysystems.openfiles.buildFuzzySystem() 
pids_fuzzysim = fuzzysystems.pids.buildFuzzySystem() 
swap_fuzzysim = fuzzysystems.swap.buildFuzzySystem()
users_fuzzysim = fuzzysystems.users.buildFuzzySystem()


#******************************************************************************
#%%

def transformFuzzy(var_name, inputs):
    """ 
    ===========================================================================
    Transform numerical input data using a fuzzyy system  
    ===========================================================================   
    
    **Args**:
        var_name: (str) 
                  Name of the output variable 
                  Name must match with a fuzzy control created
        inputs: Array of Tuples(str, double)
                [('memory', 58.9), ('memory2', 5228.9),] 
    **Returns**:
        None
    """      
    fuzzysim = globals()[var_name+'_fuzzysim']
    
    if not fuzzysim:
        raise Exception('Fuzzy ControlSystem not found: '+var_name+'_fuzzysim')
    
    for var in inputs:
        value = var[1] 
        if value < 0: value = 0
        fuzzysim.input[var[0]] = value
    fuzzysim.compute()
    #print("Resultado="+str(fuzzysim.output['out'])) 
    return fuzzysim.output['out']


def transformStandar(value, mean, std):
    """ 
    ===========================================================================
    Transform numerical input data using 
    Xnormalized = (X - Xmean) / Xstd
    ===========================================================================   
    
    **Args**:

    **Returns**:
        None
    """
    return (value - mean) / std 

def transformMaxMin(value, vmax, vmin=0):
    """ 
    ===========================================================================
    Transform numerical input data using 
    Xnormalized = (X - Xmin) / (Xmax - Xmin)
    ===========================================================================   
    
    **Args**:

    **Returns**:
        None
    """  
    return (value - vmin) / ( vmax - vmin)

def transformPercentage(value):
    """ 
    ===========================================================================
    Transform numerical input data using 
    X/100
    ===========================================================================   
    
    **Args**:

    **Returns**:
        None
    """  
    r = value/100
    if r>1: r=1.0
    return r

def transformBigValue(value):
    """ 
    ===========================================================================
    Transform numerical input data using 
    X/100
    ===========================================================================   
    
    **Args**:

    **Returns**:
        None
    """  
    r = value/100000
    if r>1: r=1.0
    return r

#******************************************************************************
#%%

if __name__ == '__main__':
    print('Start module execution:')
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S') 
    
    inputs = [('net_io_rate_in', 95.4)]
#    print('transformFuzzy('+str(inputs[0])+') = ' + str(transformFuzzy('cnxs', inputs)))
    
    print('transformFuzzy('+str(inputs[0])+') = ' + str(transformFuzzy('net_io_rate_in', inputs)))
    
    
    
    
    