# -*- coding: utf-8 -*-
"""
Created on Sun Mar  4 10:06:18 2018

@author: abaena
"""

import logging
import telnetlib
import datetime
import time

#%%
"""
Global Initialization. Constants definitions.
"""

logger = logging.getLogger(__name__)


#%%


class LogMapperAgentClient:
    
    PROMPT = b">>"
    NEWLINE = b"\n"
    
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.conn = None       
        
    def open(self):
        logger.debug("open:"+self.host+":"+str(self.port))
        self.conn = telnetlib.Telnet(self.host, self.port, timeout=10)
        self.conn.read_until(self.PROMPT, timeout=10)
        
    def close(self):
        self.conn.write(b"q"+self.NEWLINE)
        self.conn.read_all().decode('ascii')
        self.conn.close()
        
    def send(self, command, timeout=10):
        logger.debug("send:"+command)
        self.conn.write(command.encode('ascii') + self.NEWLINE)
        response=self.conn.read_until(self.PROMPT, timeout=timeout)
        response = response.decode('ascii')
        logger.debug("response:"+response)
        return response
    
    def sendCommand(self, command, timeout=60):
        tries=0
        while(tries < 5):
            response = self.send(command, timeout)
            response = self.droplmheader(response) 
            if response.startswith('ERROR'):
                logger.warn("ERROR EXECUTING:"+command+": "+response)
                time.sleep(2)  
                tries += 1
                continue
            return response
    
    def droplmheader(self, response):
        if not response: return None
        start = response.find('LMData:')
        end = response.find('LMEndData')
        start = start + len('LMData:')
        return response[start:end].strip()
    
    def executeCommand(self, command):
        return self.sendCommand(command)    
    
    def getAgent(self):
        return self.sendCommand("get agent")
    
    def getReaders(self):
        return self.sendCommand("get readers")
    
    def getMonitors(self):
        return self.sendCommand("get monitors")    
    
    def getLogMetricsColnames(self, reader):       
        return self.sendCommand("get logMetricsColnames {}".format(reader)) 
    
    def getLogMonMeasuresColnames(self, monitorKey):       
        return self.sendCommand("get monMeasuresColnames {}".format(monitorKey))      

    def getLogItemsTrace(self, reader):
        return self.sendCommand("get logkeys {}".format(reader))

    def getPaths(self, reader):
        return self.sendCommand("get paths {}".format(reader))   
    
    def getPathMeasures(self, reader, start, end):
        start = start.strftime("%Y-%m-%dT%H:%M")
        end = end.strftime("%Y-%m-%dT%H:%M")
        return self.sendCommand("get pathMeasures {} {} {}".format(reader, start, end))  
    
    def getLogEventsCount(self, reader, start, end):
        start = start.strftime("%Y-%m-%dT%H:%M")
        end = end.strftime("%Y-%m-%dT%H:%M")        
        return self.sendCommand("get logEventsCount {} {} {}".format(reader, start, end))  

    def getLogMetrics(self, reader, start, end):
        start = start.strftime("%Y-%m-%dT%H:%M")
        end = end.strftime("%Y-%m-%dT%H:%M")        
        return self.sendCommand("get logMetrics {} {} {}".format(reader, start, end))  
    
    def getMonMeasures(self, monitorKey, start, end):
        start = start.strftime("%Y-%m-%dT%H:%M")
        end = end.strftime("%Y-%m-%dT%H:%M")        
        return self.sendCommand("get monMeasures {} {} {}".format(monitorKey, start, end))      

    def getLogRecords(self, reader, start, end):
        start = start.strftime("%Y-%m-%dT%H:%M")
        end = end.strftime("%Y-%m-%dT%H:%M")        
        return self.sendCommand("get logRecords {} {} {}".format(reader, start, end))  
     
        

if __name__ == '__main__':
    print('Start module execution:')
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')   
    
    start = datetime.datetime(2018,2, 26, 8, 0, 0)
    end = datetime.datetime(2018, 2, 26, 15, 0, 0)
    reader = "device"    

    client = LogMapperAgentClient("127.0.0.1", 5001)
    
    
    client.open()
#    print(client.send("sh"))
#    print(client.getAgent()) 
#    r = client.getPathMeasures(reader, start, end)
    r = client.getHostMeasures(start, end)
    client.close() 
    
    import json
    
    if not r.startswith("ERROR"):   
        j=json.loads(r)
        print(j)


    print("End module execution") 