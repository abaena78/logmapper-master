# -*- coding: utf-8 -*-
"""
Created on Sat Apr  7 22:39:24 2018

@author: abaena
"""

import logging
import psycopg2 as ps

    
logger = logging.getLogger(__name__)


#TODO PASAR A ARCHIVO DE CONFIGURACION
DATABASE_NAME = "logmapper4"
DATABASE_HOST = "127.0.0.1"
DATABASE_USER = "sm"
DATABASE_PWD  = "1234"


def connectDb(dbname=DATABASE_NAME, host=DATABASE_HOST, user=DATABASE_USER, pwd=DATABASE_PWD):
    """
    Creates or opens a file called mydb with a SQLite3 DB
    """
    logger.debug("connect:"+dbname)
    dbname2=DATABASE_NAME
    try:
        connString = "dbname='{}' host='{}' user='{}' password='{}'".format(dbname2, host, user, pwd)
        conn = ps.connect(connString)
        conn.set_isolation_level(0) #Not require transactions
        return conn
    except ValueError as e:
        logger.error("Unable to connect to the database:"+e)
        return None 
    
def getDbCountTransactions(cursor):
    cursor.execute('''
    SELECT 
    count(*)
    FROM pg_stat_activity
    ''') 
    row = cursor.fetchone()
    return row[0]

def getDbLocksExclusive(cursor):
    cursor.execute('''
    SELECT 
    count(*) 
    FROM pg_locks bl JOIN pg_catalog.pg_stat_activity a  ON a.pid = bl.pid 
    WHERE mode = 'ExclusiveLock'
    ''') 
    row = cursor.fetchone()
    return row[0]



    
    
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
    connDb=connectDb("postgres", "127.0.0.1", "sm", "1234")
    cursor = connDb.cursor()
    dbconns = getDbCountTransactions(cursor)
    dblocks = getDbLocksExclusive(cursor)
    
    print(str(dbconns)+","+str(dblocks))
 
    connDb.close()
    print("End module execution")  