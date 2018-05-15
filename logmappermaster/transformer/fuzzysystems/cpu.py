# -*- coding: utf-8 -*-
"""
Created on Sat Jan 20 16:26:23 2018

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
    Build Fuzzy Sistem for variable: memory  
    ===========================================================================   
    
    **Args**:
        showDescription: (boolean)
    **Returns**:
        None
    """ 
    #==========================================================================
    # Set labels of inputs and outputs
    #==========================================================================      
    
    var_in1_label = 'cpu'
    var_in2_label = 'cpu_idle'
    var_out_label = 'out'
    
    logger.info("buildFuzzySystem:" + var_in1_label)
    
    #==========================================================================
    # Set numerical range of inputs and outputs
    #==========================================================================

    in1_max = 100.0  
    in2_max = 100.0       
    
    var_in1_universe = np.arange(0, in1_max, in1_max/1000.0)
    var_in2_universe = np.arange(0, in2_max, in2_max/1000.0)
    var_out_universe = np.arange(0, 1, 0.01)
    
    #==========================================================================
    # Set inputs(Antecedent) and outputs (Consequent)
    #==========================================================================     
    
    var_in1 = ctrl.Antecedent(var_in1_universe, var_in1_label)
    var_in2 = ctrl.Antecedent(var_in2_universe, var_in2_label)
    var_out = ctrl.Consequent(var_out_universe, var_out_label)

    #==========================================================================
    # Set membership functions of fuzzy set
    #==========================================================================  
    
    var_in1.automf(number=3, variable_type='quant')
    var_in2.automf(number=3, variable_type='quant')
    var_out.automf(number=3, variable_type='quant')

    
    #==========================================================================
    # Set fuzzy rules
    #========================================================================== 
    
    rule1 = ctrl.Rule(var_in1['high'] | var_in2['high'], var_out['high'])
    rule2 = ctrl.Rule(var_in1['average']  | var_in2['average'], var_out['high'])
    rule3 = ctrl.Rule(var_in1['low']  | var_in2['low'], var_out['low'])
    rule4 = ctrl.Rule(var_in1['low']  | var_in2['average'], var_out['average'])
    rule5 = ctrl.Rule(var_in1['average']  | var_in2['low'], var_out['average'])
    
    #==========================================================================
    # Build fuzzy control system and simulation
    #==========================================================================
    
    var_ctrl = ctrl.ControlSystem([rule1, rule2, rule3, rule4, rule5])
    var_fuzzysim = ctrl.ControlSystemSimulation(var_ctrl)
    
    #==========================================================================
    # Set fuzzy rules
    #==========================================================================    
    
    if showDescription: 
        fig = plt.figure(figsize=(12, 12))
        plt.subplot(3, 1, 1)
        plt.title('Input: '+var_in2_label)
        plt.plot(var_in1_universe, var_in1['low'].mf, label='low')
        plt.plot(var_in1_universe, var_in1['average'].mf, label='average')
        plt.plot(var_in1_universe, var_in1['high'].mf, label='high')
        plt.legend()
        
        plt.subplot(3, 1, 2)
        plt.title('Input: '+var_in2_label)
        plt.plot(var_in2_universe, var_in2['low'].mf, label='low')
        plt.plot(var_in2_universe, var_in2['average'].mf, label='average')
        plt.plot(var_in2_universe, var_in2['high'].mf, label='high')
        plt.legend()        
        
        plt.subplot(3, 1, 3)
        plt.title('Output: '+var_out_label)
        plt.plot(var_out_universe, var_out['low'].mf, label='low')
        plt.plot(var_out_universe, var_out['average'].mf, label='average')
        plt.plot(var_out_universe, var_out['high'].mf, label='high')
        plt.legend()
        
#        var_fuzzysim.input[var_in1_label] = var_in1_universe
#        var_fuzzysim.input[var_in2_label] = var_in2_universe
#        var_fuzzysim.compute()
#        y = var_fuzzysim.output[var_out_label]
#        plt.subplot(4, 1, 4)
#        plt.plot(var_in1_universe, y, label='Fuzzy transform of '+var_in1_label)
        
        #plt.show()
        plt.savefig('/tmp/fuzzy_'+var_in1_label+'.png')
        

        upsampled = np.linspace(0, 100, 25)
        x, y = np.meshgrid(upsampled, upsampled)
        z = np.zeros_like(x)        
        
        for i in range(25):
            for j in range(25):
                var_fuzzysim.input[var_in1_label] = x[i, j]
                var_fuzzysim.input[var_in2_label] = y[i, j]
                var_fuzzysim.compute()
                z[i, j] = var_fuzzysim.output['out'] 
                
        from mpl_toolkits.mplot3d import Axes3D  
        
        fig = plt.figure(figsize=(12, 12))
        ax = fig.add_subplot(111, projection='3d')
        
        surf = ax.plot_surface(x, y, z, rstride=1, cstride=1, cmap='viridis',
                               linewidth=0.4, antialiased=True)
        
        cset = ax.contourf(x, y, z, zdir='z', offset=-2.5, cmap='viridis', alpha=0.5)
        cset = ax.contourf(x, y, z, zdir='x', offset=3, cmap='viridis', alpha=0.5)
        cset = ax.contourf(x, y, z, zdir='y', offset=3, cmap='viridis', alpha=0.5)
        
        ax.view_init(30, 200)
        fig.savefig('/tmp/fuzzy_'+var_in1_label+'3d.png')
        
    return var_fuzzysim


if __name__ == '__main__':
    print('Start module execution:')
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S') 
  
    showDescription = True
    fuzzysim = buildFuzzySystem(showDescription) 
    if not showDescription:
        fuzzysim.input['cpu'] = 15.0
        fuzzysim.compute()
        print("Resultado="+str(fuzzysim.output['out']))
        
    print("End module execution") 