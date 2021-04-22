# -*- coding: utf-8 -*-
"""
Created on Wed Apr 14 23:17:31 2021

@author: jomen
"""

import pandas as pd

df = pd.read_json(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\data\tweet_data.json', lines=True)
df.to_csv(r"C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\tweet_data.csv", index = None, header=True)