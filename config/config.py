# -*- coding: utf-8 -*-

# XXXXXX.py : part of LogMapper
# 
# Copyright (c) 2018, Jorge Andres Baena abaena78@gmail.com
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of the <organization> nor the
#       names of its contributors may be used to endorse or promote products
#       derived from this software without specific prior written permission.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""
===============================================================================
Configuration defintions for Logmapper

@author: abaena
===============================================================================
"""

import os
import configparser

#==============================================================================
# Constants definitions
#==============================================================================

__version__ = "0.0.4"

CONFIGFILE_NAME = "conf.ini"


PERIOD = 1 #1 MINUTE

#==============================================================================
# Properties keys
#==============================================================================


SECTION_LOGMAPPER = "LOGMAPPER"
PROP_DIR_BASE = "dirbase"
PROP_DIR_LOG = "dirlog"
PROP_DIR_DATA = "dirdata"
PROP_LOGFILENAME = "logger.file"
PROP_DEBUGLEVEL = "logger.debug.level"

PROP_MASTERPORT = "master.port" 



#==============================================================================
# Classes definitions
#==============================================================================



#==============================================================================
# Functions
#==============================================================================  

def loadConfig(filepath=CONFIGFILE_NAME):
    """ 
    ===========================================================================
    Load configuration from file
    If the file not exist create a new one.
    ===========================================================================   
    
    **Args**:
        filepath: path of the file -- str       
    **Returns**:
        ConfigParser class. Query setting with: ``config.get(section, key)``   
    """    
    if not filepath:
       filepath=CONFIGFILE_NAME 
    
    # Check if there is already a configurtion file
    if not os.path.isfile(filepath):
        createDefaultConfigfile()
        
    config = configparser.ConfigParser()
    config.read(filepath)
    return config 


def printConfig(config):
    """ 
    ===========================================================================
    Print in logger config data
    ===========================================================================   
    
    **Args**:
        None        
    **Returns**:
        None  
    """
    for section in config.sections():
        for key in config[section]: 
            print(section+'.'+key+'='+config.get(section, key))
       

def saveConfig(config):
    """ 
    ===========================================================================
    Print in logger config data
    ===========================================================================   
    
    **Args**:
        None        
    **Returns**:
        None  
    """
    # Create the configuration file as it doesn't exist yet
    cfgfile = open(CONFIGFILE_NAME, 'w')
    config.write(cfgfile)
    cfgfile.close()  
    

  

def createDefaultConfigfile():
    """ 
    ===========================================================================
    Create configfile with Default Data
    ===========================================================================   
    
    **Args**:
        None        
    **Returns**:
        None  
    """    
    # Add content to the file
    config = configparser.ConfigParser()
    config.add_section(SECTION_LOGMAPPER)
    config.set(SECTION_LOGMAPPER, PROP_DIR_BASE, "/logmapper/")
    config.set(SECTION_LOGMAPPER, PROP_DIR_LOG, "/logmapper/log/")
    config.set(SECTION_LOGMAPPER, PROP_DIR_DATA, "/logmapper/data/")
    config.set(SECTION_LOGMAPPER, PROP_LOGFILENAME, "logmapper-master.log")
    config.set(SECTION_LOGMAPPER, PROP_DEBUGLEVEL, 'INFO')

    config.set(SECTION_LOGMAPPER, PROP_MASTERPORT, '5005')
 
    saveConfig(config)

if __name__ == '__main__':
    createDefaultConfigfile()
    config=loadConfig()     
    printConfig(config)