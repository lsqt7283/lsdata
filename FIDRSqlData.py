# -*- coding: utf-8 -*-
"""
Created on Mon Mar 16 13:39:57 2020

Run Data Query via FIDR SQL Database

Based on Sqlquery.py script by AShariq 20190625

Examples:
mySQL = '''select * from acid_entity_map'''
result = pyFIDR_read(mySQL)

Reference:
https://www.tutorialspoint.com/sqlalchemy/index.htm

@author: TQiu
"""


import pandas as pd
from sqlalchemy import create_engine, text


def getFIDRSqlData(strSQL):
    """
    strSQL: String input of SQL statement
    dfData: Pandas Dataframe output of query result
    """
    dbuser = 'fidr_read'
    dbpass = 'fidr_read'
    dbserver = 'fidrprd'    
    dbname = '?service_name=fidrprd'
    dbport = '1522'
    #dbpoolsize = 6
    #dboverflow = 10
    #dbtimeout = 300    
    writeEngine = create_engine('oracle+cx_oracle://' + dbuser + ':' + dbpass + '@' + dbserver + ':' + dbport + '/' + dbname)
    conn = writeEngine.connect()    
    q = text(strSQL)
    dfData = pd.read_sql_query(q, conn)
    conn.close()
    return dfData


__all__ = ["getFIDRSqlData"]