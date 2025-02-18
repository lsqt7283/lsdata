# -*- coding: utf-8 -*-
"""
Created on Fri Feb  4 08:08:38 2022

Run Data Query via LS XLerate API


@author: TQiu
"""


import os
import pandas as pd
from datetime import date 
from datetime import timedelta 



def getXDataFromTo(xid, entity_ids, start_date, end_date, outFPath, outFName=None):
    
    # report with date range
    if outFName == None:
        outFName = 'XData_'+xid
    
    for eid in entity_ids:
        df = pd.read_json('http://in2apps/xlerate/api/XLerate?templateId='+ \
                          xid+'&contentFormat=tablejson&entityId='+ \
                          eid+'&startdate='+start_date+'&enddate='+end_date)
        try:
            os.chdir(outFPath)
        except FileNotFoundError:
            print("Warning: Folder not exists. Creating folder... "+outFPath)
            os.makedirs(outFPath)
            os.chdir(outFPath)
        df.to_csv(outFName+'_'+eid+'.csv',index=True,index_label='Date')
    return df



def getXDataAsOf(xid, outFPath, entity_ids=None, outFName=None, as_of_date=None):
    
    # report as of one day
    if entity_ids == None:
        entity_ids = ['500420010', '418015010', '455552010', '554215010']
    
    if as_of_date == None:
        yesterday= date.today() - timedelta(days = 1)
        yesterday = yesterday.strftime("%m/%d/%Y")
        as_of_date=yesterday
    
    if outFName == None:
        outFName = 'XData_'+xid
    
    for eid in entity_ids:
        df = pd.read_json('http://in2apps/xlerate/api/XLerate?templateId='+ \
                          xid+'&contentFormat=tablejson&entityId='+ \
                          eid+'&startdate='+as_of_date)
        try:
            os.chdir(outFPath)
        except FileNotFoundError:
            print("Warning: Folder not exists. Creating folder... "+outFPath)
            os.makedirs(outFPath)
            os.chdir(outFPath)
        df.to_csv(outFName+'_'+eid+'.csv',index=True,index_label='Date')
    return df


__all__ = ["getXDataFromTo", "getXDataAsOf"]