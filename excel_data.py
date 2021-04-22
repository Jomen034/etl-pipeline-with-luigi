# -*- coding: utf-8 -*-
"""
Created on Wed Apr 14 23:48:06 2021

@author: jomen
"""


import pandas as pd

#def excel_data():
def file_1000_s1():
    
    file_1000_s1 = pd.read_excel(r"C:\Users\jomen\Documents\self_project\ETL_with_Luigi\data\file_1000.xls",
                           sheet_name = 'Sheet1')
    df = file_1000_s1.to_csv(r"C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\file_1000_s1.csv", 
                  index = None,
                  header=True)
    return df


def file_1000_s2():
    
    file_1000_s2 = pd.read_excel(r"C:\Users\jomen\Documents\self_project\ETL_with_Luigi\data\file_1000.xls",
                           sheet_name = 'Sheet2')
    df = file_1000_s2.to_csv(r"C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\file_1000_s2.csv", 
                  index = None,
                  header=True)
    return df


def disaster_data():
    disaster = pd.read_csv(r"C:\Users\jomen\Documents\self_project\ETL_with_Luigi\data\disaster_data.csv")
    df = disaster.to_csv(r"C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\disaster_data.csv", 
                  index = None,
                  header=True)
    return df


def reviews():
    
    reviews_q1_xlsx = pd.read_excel(r"C:\Users\jomen\Documents\self_project\ETL_with_Luigi\data\reviews_q1.xlsx")
    reviews_q1_csv = pd.read_csv(r"C:\Users\jomen\Documents\self_project\ETL_with_Luigi\data\reviews_q1.csv")
    reviews_q2 = pd.read_csv(r"C:\Users\jomen\Documents\self_project\ETL_with_Luigi\data\reviews_q2.csv")
    reviews_q3 = pd.read_csv(r"C:\Users\jomen\Documents\self_project\ETL_with_Luigi\data\reviews_q3.csv")
    reviews_q4 = pd.read_csv(r"C:\Users\jomen\Documents\self_project\ETL_with_Luigi\data\reviews_q4.csv")
    
    dframes = [
        reviews_q1_xlsx,
        reviews_q1_csv,
        reviews_q2,
        reviews_q3,
        reviews_q4
        ]
    
    df = pd.concat(dframes)
    df.drop_duplicates(subset=['id'], inplace=True)
    
    df.to_csv(r"C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\reviews.csv", 
                  index = None,
                  header=True)
    return df


file_1000_s1()
file_1000_s2()
disaster_data()
reviews()