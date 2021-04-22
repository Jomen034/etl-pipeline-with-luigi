# -*- coding: utf-8 -*-
"""
Created on Thu Apr 15 23:19:56 2021

@author: jomen
"""

import pandas as pd
import luigi
import sqlite3
import chinook_data as cd
import database_data as dbd
import excel_data as xld
import tweet_data as twd


class ExtractData(luigi.Task):

    def run(self):
        chinook_data = cd
        database_data = dbd
        excel_data = xld
        tweet_data = twd

    def output(self):
        return(luigi.LocalTarget(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data'))
    

class TransformData(luigi.Task):

    def output(self):
        return luigi.LocalTarget(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi')

    def run(self):

        def join_ArtistId():
            albums = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\chinook_albums.csv')
            artists = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\chinook_artists.csv')
            chinook_albums = pd.merge(albums, artists, how='inner', on=['ArtistId'])
            return chinook_albums
            
        
            
class LoadData(luigi.Task):
    def requires(self):
        return [TransformData()]

    def run(self):
        conn = sqlite3.connect(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\data_warehouse.db')
        cursor = conn.cursor()
        
        """
        This is to create chinook_albums' table 
        contains merged df of albums & artists on ArtistId 
        """
        cursor.execute('CREATE TABLE chinook_albums (AlbumId int, Title varchar(100), ArtistId int)')
        albums = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\chinook_albums.csv')
        artists = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\chinook_artists.csv')
        chinook_albums = pd.merge(albums, artists, how='inner', on=['ArtistId'])
        chinook_albums.to_sql('chinook_albums', 
                      conn, 
                      if_exists='replace',
                      index=False
                      )
        
        
        """
        This it to create chinook_customers' table
        contains FullName of employees
        """
        cursor.execute("""CREATE TABLE chinook_customers (CustomerId int, 
               Company nvarchar(100),
               Address nvarchar(100),
               City nvarchar(100),
               State nvarchar(100),
               Country nvarchar(100),
               PostalCode nvarchar(100),
               Phone nvarchar(100),
               Fax nvarchar(100),
               Email nvarchar(100),
               SupportdRepId int,
               FullName nvarchar(100)
               )""")
        chinook_customers = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\chinook_customers.csv')
        chinook_customers['FullName'] = chinook_customers['FirstName'] + " " + chinook_customers['LastName']
        chinook_customers = chinook_customers.drop(columns=['FirstName', 'LastName'])
        chinook_customers.to_sql('chinook_customers', 
              conn, 
              if_exists='replace',
              index=False
              )
        
        """
        This it to create chinook_employees' table
        contains FullName of employees
        """
        cursor.execute("""CREATE TABLE chinook_employees (EmployeedId int, 
               Title nvarchar(100),
               ReportsTo int,
               BirthDate datetime,
               HireData datetime,
               Address nvarchar(100),
               City nvarchar(100),
               State nvarchar(100),
               Country nvarchar(100),
               PostalCode nvarchar(100),
               Phone nvarchar(100),
               Fax nvarchar(100),
               Email nvarchar(100),
               FullName nvarchar(100)
               )""")
        chinook_employees = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\chinook_employees.csv')
        chinook_employees['FullName'] = chinook_employees['FirstName'] + " " + chinook_employees['LastName']
        chinook_employees = chinook_employees.drop(columns=['FirstName', 'LastName'])
        chinook_employees.to_sql('chinook_employees', 
              conn, 
              if_exists='replace',
              index=False
              )
        
        """
        This is to create chinook_full_invoices' table 
        contains merged df of invoices_items & invoices on InvoiceId 
        """
        cursor.execute("""CREATE TABLE chinook_full_invoices (InvoiceId int,
                       TrackId int,
                       UnitPrice numeric(10,2),
                       Quantityt int,
                       CustomerId int,
                       InvoiceDate datetime,
                       BillingAddress nvarchar(100),
                       BillingCity nvarchar(100),
                       BillingState nvarchar(100),
                       BillingCountry nvarchar(100),
                       BililngPostalCode nvarchar(100),
                       Total numeric(10,2)
                       )""")
        invoices_items = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\chinook_invoice_items.csv')
        invoices = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\chinook_invoices.csv')
        chinook_full_invoices = pd.merge(invoices_items, invoices, how='inner', on=['InvoiceId'])
        chinook_full_invoices = chinook_full_invoices.drop(columns=['InvoiceLineId'])
        chinook_full_invoices.to_sql('chinook_full_invoices', 
                      conn, 
                      if_exists='replace',
                      index=False
                      )
        
        """
        This is to create chinook_full_playlists' table 
        contains merged df of playlist_track & playlists on PlaylistId 
        """
        cursor.execute("""CREATE TABLE chinook_full_playlists (PlaylistId int,
                       TrackId int,
                       Name nvarchar(100)
                       )""")
        playlist_track = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\chinook_playlist_track.csv')
        playlists = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\chinook_playlists.csv')
        chinook_full_playlists = pd.merge(playlist_track, playlists, how='inner', on=['PlaylistId'])
        chinook_full_playlists.to_sql('chinook_full_playlists', 
                      conn, 
                      if_exists='replace',
                      index=False
                      )
        
        """
        This is to create chinook_full_tracks' table 
        contains merged df of albums & artists on ArtistId 
        """
        cursor.execute("""CREATE TABLE chinook_full_tracks (PlaylistId int,
                       TrackId int,
                       Name nvarchar(100)
                       )""")
        albums = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\chinook_albums.csv')
        media_types = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\chinook_media_types.csv')
        genres = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\chinook_genres.csv')
        tracks = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\chinook_tracks.csv')
        chinook_full_tracks = pd.merge(albums,
                                        pd.merge(media_types,
                                                 pd.merge(tracks, genres, suffixes=(None, '_of_Genre'), how="inner", on=["GenreId"]),
                                                 suffixes=('_of_MediaType', None),
                                                 how="inner", on=["MediaTypeId"]),
                                        how="inner", on=["AlbumId"], sort=True)
        
        chinook_full_tracks.to_sql('chinook_full_tracks', 
                      conn, 
                      if_exists='replace',
                      index=False
                      )
        """
        This is to create database_full_reviews' table 
        contains merged df of all tables on reviewid 
        """
        cursor.execute("""CREATE TABLE database_full_reviews (reviewid int,
                       artist1 text,
                       content text,
                       genre text,
                       label text,
                       title text,
                       artist2 text,
                       url text,
                       score real,
                       best_new_music int,
                       author text,
                       author_type text,
                       pub_date text,
                       pub_weekday int,
                       pub_day int,
                       pub_month int,
                       pub_year int
                       )""")
        artists = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\database_artists.csv')
        content = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\database_content.csv')
        genres = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\database_genres.csv')
        labels = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\database_labels.csv')
        reviews = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\database_reviews.csv')
        years = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\database_years.csv')
        database_full_reviews = pd.merge(artists,
                                         pd.merge(content,
                                                  pd.merge(genres,
                                                           pd.merge(labels,
                                                                    pd.merge(reviews, years, how='inner', on=['reviewid']),
                                                                    how='inner', on=['reviewid']),
                                                           how='inner', on=['reviewid']),
                                                  how='inner', on=['reviewid']),
                                         how='inner', on=['reviewid'])
        
        database_full_reviews.to_sql('database_full_reviews', 
                      conn, 
                      if_exists='replace',
                      index=False
                      )
        
        """
        This is to create disaster_data's table
        """
        cursor.execute("""CREATE TABLE disaster_data (id int,
                       keyword text,
                       location text,
                       text text,
                       target text
                       )""")
        disaster_data = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\disaster_data.csv')
        disaster_data.to_sql('disaster_data',
                             conn,
                             if_exists='replace',
                             index=False)
        
        """
        This is to create file_1000_s2's table
        """
        cursor.execute("""CREATE TABLE file_1000_s2 (id int,
                       FullName text,
                       Gender text,
                       Country text,
                       Age int,
                       Date datetime
                       )""")
        file_1000_s2 = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\file_1000_s2.csv')
        file_1000_s2['FullName'] = file_1000_s2['First Name'] + " " + file_1000_s2['Last Name']
        file_1000_s2 = file_1000_s2.drop(columns=['First Name', "Last Name", 'Unnamed: 0'])
        file_1000_s2.to_sql('file_1000_s2',
                             conn,
                             if_exists='replace',
                             index=False)
        
        
        """
        This is to create reviews' table
        """
        cursor.execute("""CREATE TABLE reviews (listing_id text,
                       id int,
                       date datetime,
                       reviewer_id text,
                       reviewer_name text,
                       comments text
                       )""")
        reviews = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\reviews.csv')
        reviews.to_sql('reviews',
                             conn,
                             if_exists='replace',
                             index=False)
        
        """
        This is to create tweet_data's table
        """
        cursor.execute("""CREATE TABLE tweet_data (
                       id int,
                       text text,
                       user text,
                       lang text,
                       place text
                       )""")
        tweet_data = pd.read_csv(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\extracted_data\tweet_data.csv')
        tweet_data = tweet_data.drop(columns=['contributors', 'truncated', 'is_quote_status',
                                               'in_reply_to_status_id', 'in_reply_to_user_id', 'favorite_count',
                                               'entities', 'retweeted', 'coordinates', 'source',
                                               'in_reply_to_screen_name', 'id_str', 'retweet_count', 'metadata',
                                               'favorited', 'retweeted_status', 'geo',
                                               'in_reply_to_user_id_str', 'created_at',
                                               'in_reply_to_status_id_str', 'quoted_status_id',
                                               'quoted_status', 'possibly_sensitive', 'quoted_status_id_str',
                                               'extended_entities'])
        tweet_data.to_sql('tweet_data',
                             conn,
                             if_exists='replace',
                             index=False)
        
        conn.close()
    
    
    def output(self):
        return luigi.LocalTarget(r'C:\Users\jomen\Documents\self_project\ETL_with_Luigi\data_warehouse.db')
        #return [LoadData().run()]