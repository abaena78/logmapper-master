# -*- coding: utf-8 -*-
"""
Created on Sat Jan 20 16:30:39 2018

@author: abaena
"""

import numpy as np
import skfuzzy as fuzz
from skfuzzy import control as ctrl
import matplotlib.pyplot as plt

import logging

logger = logging.getLogger(__name__)


def buildFuzzySystem(showDescription = False):
    """ 
    ===========================================================================
    Build Fuzzy Sistem for variable: cnxs: network connections  
    ===========================================================================   
    
    **Args**:
        showDescription: (boolean)
    **Returns**:
        None
    """ 
    #==========================================================================
    # Set labels of inputs and outputs
    #==========================================================================      
 
    in1_max = 100
    
    var_in1_label = 'users'
    var_out_label = 'out'
    
    logger.info("buildFuzzySystem:" + var_in1_label)
    
    #==========================================================================
    # Set numerical range of inputs and outputs
    #==========================================================================          
    
    var_in1_universe = np.arange(0, in1_max, in1_max/1000)
    var_out_universe = np.arange(0, 1, 0.01)
    
    #==========================================================================
    # Set inputs(Antecedent) and outputs (Consequent)
    #==========================================================================     
    
    var_in1 = ctrl.Antecedent(var_in1_universe, var_in1_label)
    var_out = ctrl.Consequent(var_out_universe, var_out_label)

    #==========================================================================
    # Set membership functions of fuzzy set
    #==========================================================================  
    
    var_in1.automf(number=3, variable_type='quant')
    var_in1['low'] = fuzz.trimf(var_in1.universe, [0, 0, in1_max*0.2])
    var_in1['average'] = fuzz.trapmf(var_in1.universe, [0, 0.2*in1_max, 0.8*in1_max, in1_max])
    var_in1['high'] = fuzz.trimf(var_in1.universe, [0.8*in1_max, in1_max, in1_max])
    
    var_out.automf(number=3, variable_type='quant')
    
    #==========================================================================
    # Set fuzzy rules
    #========================================================================== 
    
    rule1 = ctrl.Rule(var_in1['high'], var_out['high'])
    rule2 = ctrl.Rule(var_in1['average']  , var_out['average'])
    rule3 = ctrl.Rule(var_in1['low']  , var_out['low'])
    
    #==========================================================================
    # Build fuzzy control system and simulation
    #==========================================================================
    
    var_ctrl = ctrl.ControlSystem([rule1, rule2, rule3])
    var_fuzzysim = ctrl.ControlSystemSimulation(var_ctrl)
    
    #==========================================================================
    # Set fuzzy rules
    #==========================================================================    
    
    if showDescription:
        fig = plt.figure(figsize=(12, 12))
        plt.subplot(3, 1, 1)
        plt.title('Input: '+var_in1_label)
        plt.plot(var_in1_universe, var_in1['low'].mf, label='low')
        plt.plot(var_in1_universe, var_in1['average'].mf, label='average')
        plt.plot(var_in1_universe, var_in1['high'].mf, label='high')
        plt.legend()
        
        plt.subplot(3, 1, 2)
        plt.title('Output: '+var_out_label)
        plt.plot(var_out_universe, var_out['low'].mf, label='low')
        plt.plot(var_out_universe, var_out['high'].mf, label='high')
        plt.legend()
        
        var_fuzzysim.input[var_in1_label] = var_in1_universe
        var_fuzzysim.compute()
        y = var_fuzzysim.output[var_out_label]
        plt.subplot(3, 1, 3)
        plt.plot(var_in1_universe, y, label='Fuzzy transform of '+var_in1_label)
        
        #plt.show()
        plt.savefig('/tmp/fuzzy_'+var_in1_label+'.png')
        
    return var_fuzzysim


if __name__ == '__main__':
    print('Start module execution:')
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S') 
  
    showDescription = True
    fuzzysim = buildFuzzySystem(showDescription) 
    if not showDescription:
        fuzzysim.input['users'] = 15.0
        fuzzysim.compute()
        print("Resultado="+str(fuzzysim.output['out']))   

    print("End module execution") 
