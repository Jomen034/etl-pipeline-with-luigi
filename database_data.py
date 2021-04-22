# -*- coding: utf-8 -*-
"""
Created on Thu Apr 15 22:38:55 2021

@author: jomen
"""

import sqlite3 as sql
import os
import csv
from sqlite3 import Error
import pandas as pd

try:

    # Connect to database
    conn = sql.connect(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\data\database.sqlite')
    cursor = conn.cursor()
    
    database_columns = [
        'artists', 
        'content', 
        'genres',
        'labels', 
        'reviews', 
        'years'
        ]
    
    for column in database_columns:
        results = pd.read_sql_query("SELECT * FROM {}".format(column), conn)
        results.to_csv(r"C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\database_{}.csv".format(column),
                       encoding='utf-8',
                       index=False,
                       header=True,
                       quoting=2)
        

except Error as e:
  print(e)

# Close database connection
finally:
  conn.close()