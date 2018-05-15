# -*- coding: utf-8 -*-
"""
Created on Wed Feb  7 11:50:54 2018

@author: abaena
"""

import datetime


PERIOD = 1


def calcTimeGroup(dt):
    minuteOfTheDay = dt.hour*60+dt.minute
    group = minuteOfTheDay / 1
    return int(group)

def calcTimeGrouptoDatetime(date, period):
    minuteOfTheDay = period * 1
    hour = minuteOfTheDay // 60
    minute = minuteOfTheDay % 60
    return datetime.datetime(date.year,date.month,date.day,hour, minute, 0)

def datetimeParse(datetimestr):
    return datetime.datetime.strptime(datetimestr, '%Y-%m-%d %H:%M:%S')

def getLogMapperIntervalDate(date=datetime.datetime.now()):
    return datetime.datetime(date.year, date.month, date.day, date.hour, date.minute, 0)

def getNextLogMapperIntervalDate(date=datetime.datetime.now()):
    date = getLogMapperIntervalDate(date)
    return date + datetime.timedelta(minutes = 1) 

def getBeforeMapperIntervalDate(date=datetime.datetime.now()):
    date = getLogMapperIntervalDate(date)
    return date - datetime.timedelta(minutes = 1) 
    

if __name__ == '__main__':
    print('Start module execution:')
    
    
    stamp = datetime.datetime(2018,2, 6, 8, 0, 0)
    period = calcTimeGroup(stamp)
    print('period='+str(period))
    
    print(stamp.date())
    
    print(calcTimeGrouptoDatetime(stamp.date(), period))
    
    stamp = datetime.datetime(2018,2, 6, 8, 48, 20)
    print(getLogMapperIntervalDate(stamp))
    print(getNextLogMapperIntervalDate(stamp))
    print(getBeforeMapperIntervalDate())