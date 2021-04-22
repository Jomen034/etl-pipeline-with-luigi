import sqlite3 as sql
import os
import csv
from sqlite3 import Error
import pandas as pd

try:

    # Connect to database
    conn = sql.connect(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\data\chinook.db')
    cursor = conn.cursor()
    
    chinook_columns = [
        'albums', 
        'artists', 
        'customers', 
        'employees',
        'genres', 
        'invoice_items', 
        'invoices', 
        'media_types', 
        'playlist_track', 
        'playlists', 
        'sqlite_sequence',
        'sqlite_stat1', 
        'tracks'
        ]
    
    for column in chinook_columns:
        results = pd.read_sql_query("SELECT * FROM {}".format(column), conn)
        results.to_csv(r"C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\chinook_{}.csv".format(column),
                       encoding='utf-8',
                       index=False,
                       header=True,
                       quoting=2)
        #cursor.execute("SELECT * FROM {}".format(column))
        #with open(r"C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\chinook_{}.csv".format(column), "w", encoding='utf-8') as csv_file:
        #    csv_writer = csv.writer(csv_file, delimiter=",")
        #    csv_writer.writerow([i[0] for i in cursor.description])
        #    csv_writer.writerows(cursor)
        #    dirpath = os.getcwd() + "/chinook_{}.csv".format(column)

except Error as e:
  print(e)

# Close database connection
finally:
  conn.close()