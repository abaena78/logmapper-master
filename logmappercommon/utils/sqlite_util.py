# -*- coding: utf-8 -*-
"""
Created on Sat Sep 23 09:47:20 2017

@author: abaena
"""

import logging
import sqlite3
import os
import shutil
import threading
import datetime



logger = logging.getLogger(__name__)


def connectDb(dbname):
    """
    Creates or opens a file called mydb with a SQLite3 DB
    """
    logger.debug("connect:"+dbname)
    try:
        conn = sqlite3.connect(dbname)
        return conn
    except ValueError as e:
        logger.error("Unable to connect to the database:"+e)
        return None

    
def connectDbOnlyRead(dbname):
    """
    Creates or opens a file called mydb with a SQLite3 DB
    """
    logger.debug("connect:"+dbname)
    try:
        conn = sqlite3.connect(dbname, uri=True)
        return conn
    except ValueError as e:
        logger.error("Unable to connect to the database:"+e)
        return None
    
  
def connectDbMemory():
    """
    Create a Memory DB
    """    
    logger.debug("connectDbMemory:")
    try:
        # Create a database in RAM
        conn = sqlite3.connect(':memory:')
        return conn
    except ValueError as e:
        logger.error("Unable to connect to the memory database"+e)
        return None 
    
def dropTable(conn, table):
    logger.debug("dropTable:"+table)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS " + table)
    cursor.close()
    conn.commit()    
    
    
def copyDbData(sourceConn, targetConn):
    """
    Copy DB data using SQL commands
    Previous data is keeped. 
    """      
    logger.debug("copyDbData:")
    query = "".join(line for line in sourceConn.iterdump())   
    targetConn.executescript(query)  
    
def copyDbDataSelection(sourceConn, targetConn, selection):
    """
    Copy DB data using SQL commands
    Previous data is keeped. 
    selection=["table1", "table2"]
    """      
    logger.debug("copyDbDataSelection:")
    
    cursor = targetConn.cursor()
    for table in selection:
        query = "DROP TABLE IF EXISTS " + table
        cursor.execute(query)
    cursor.close()
    targetConn.commit()
    
    
    query = ''
    for line in sourceConn.iterdump():
        if any(item in line for item in selection):
            query = query + line
    targetConn.executescript(query)
    
def copyDbFile(source, target):
    """
    Copy DB File using filesystem functions
    All previous data in target DB is lost.
    """      
    if not os.path.isfile(source):
        logger.error("source file not exist:"+source)
        return
    shutil.copy(source, target)
    logger.debug("copyDbFile:" + source +" -> " + target)
    
def deleteDbFile(dbfile):
    """
    Delete DB File using filesystem functions.
    DB File must be unlocked
    """     
    logger.info("deleteDbFile:"+dbfile)
    if not os.path.isfile(dbfile):
        logger.error("source file not exist:"+dbfile)
        return 
    os.remove(dbfile)
    
def dbFileExist(dbfile):
    return os.path.isfile(dbfile)

class saveDbFileThread(threading.Thread):    
    """ 
    ***************************************************************************
    Thread for long command executing
    Execute one commant at time    
    ***************************************************************************
    """  
    def __init__(self, nameBase, connDbSource, dbNameTarget, selection):
        threading.Thread.__init__(self)
        self.setName(nameBase+"_"+dbNameTarget)
        self.dbNameTarget = dbNameTarget
        
        query = ''
        for line in connDbSource.iterdump():
            if any(item in line for item in selection):
                query = query + line 
        self.query = query
        self.startDate = datetime.datetime.now()
        
        
    def run(self):
        logger.info("Start Thread:" + self.name + ":"+self.dbNameTarget) 
        try:
            deleteDbFile(self.dbNameTarget)  
        except Exception as e:
            logger.warning("Fail Db File erase:"+self.dbNameTarget+":"+str(e))
            return
        connDbTarget = connectDb(self.dbNameTarget)
        connDbTarget.executescript(self.query)          
        connDbTarget.close() 
        logger.info("Finish Thread:" + self.name)           


#%%
"""
*******************************************************************************
Module Execution
This code helps to developer to know the usage of the module
*******************************************************************************
"""
if __name__ == '__main__':    
    print('Start module execution:')
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')     

    logger.info('Test DAO')
#    connDb=connectDb("logmapper")
#    for line in connDb.iterdump(): 
#        print("#####="+line)
#              
#    copyDbFile("logmapper", "logmapper2")
    
#    deleteDbFile("logmapper2")
 
    
    print("End module execution")              
              
    