#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 19 13:38:55 2021

@author: pranayrastogi
"""
import os
import pandas as pd
import sqlite3

def refineFunctionNames(col):
    return col.replace('<','').replace('>','').strip()

def insertCSVtoDB():
    
    sub_dirs = [f for f in os.listdir(os.getcwd()) if os.path.isdir(f)]
    
    for dirs in sub_dirs:
        files = os.listdir(dirs)
        
        for f in files:
            df = pd.read_csv(os.path.join(os.path.join(os.getcwd(),dirs),f))
            df.columns = list(map(refineFunctionNames,df.columns))
            conn = sqlite3.connect(os.path.join(os.getcwd(),"file.db" ))
            df.to_sql('stock_data', conn, if_exists='append', index = False, chunksize=100000)
            

def readDBIntoCSV(query):
    conn = sqlite3.connect(os.path.join(os.getcwd(),"file.db" ))
    df = pd.read_sql(query, conn)
    return df


#insertCSVtoDB()
query = 'SELECT date,time,close FROM stock_data WHERE ticker="WHEELS"'
print(readDBIntoCSV(query).head())

# ['ticker', 'date', 'time', 'open', 'high', 'low', 'close', 'volume']