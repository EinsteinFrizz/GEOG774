# -*- coding: utf-8 -*-
"""
Created on Wed Sep 30 12:43:06 2020

@author: apee461
"""
#currently up to sending the data to PostGIS database - IP changed from localost to actual address however error due to not accepting TCP/IP on the port
# %% IMPORTING PACKAGES
import requests #internet data requests
import psycopg2 #connect to PostGIS database
import gensim #JSON data
import csv
import ast
import time
from pprint import pprint as pp
import re
from gensim import corpora, models
import logging
logging.basicConfig(filename='lab3.log',level=logging.DEBUG,filemode='w')
import warnings
warnings.filterwarnings("ignore",category=DeprecationWarning)
import json #was getting error 'name json is not defined' for dump

# %% SETTING UP YELP API
endpoint = 'https://api.yelp.com/v3/businesses/search'
headers = {'Authorization':'Bearer l_zg3FKea0qKiF8ql_cEcSzTJe_keKB2S_O15T2ZV-BitVeLQHnfjkC3Z7Ukkk2n9h61KDb04iw3CDPhjLIxdALd85B4Fp9f5_nA47Br5oDmlC9Zjd-yo1Kg6Ql0X3Yx'} #this is my API key

params = {'latitude':'-36.872' , 'longitude':'174.74' , 'limit':'50' , 'radius':'500'}

payload = requests.get(endpoint,params=params,headers=headers).json()

# %% READING FROM YELP API
with open('yelp.json','w') as f:
    json.dump(payload,f)

#payload_good = [['name','rating','url','price','lat','long','category']]
payload_review = [['Name','Bus_ID','Rating','Url','Price','Lat','Long','Category','Review_1','Review_2','Review_3']]

for business in payload['businesses']: #for loop iterates through each entry in the array
    lat = str(business['coordinates']['latitude']) #set variable to the string of latitude value
    lon = str(business['coordinates']['longitude']) #set variable to the string of longitude value
    name = str(business['name']) #set variable to the string of name of the business
    url = str(business['url']) #set variable to the string of URL of the business
    price = business['price'] if 'price' in business else "null" #set variable to the priciness of the business if it is defined, otherwise set it to 'null' string
    rating = str(business['rating']) #set variable to the string of rating of the business
    category = str(business['categories'][0]['title']) #set variable to the string of the first category that the business fits in, using the 'title' value
    busID = business['id']
    
    r = requests.get('https://api.yelp.com/v3/businesses/'+busID+'/reviews',params={},headers=headers).json()
    
    n = len(r['reviews'])
    review1,review2,review3 = 'null','null','null' #initialises variables
    if n > 0: review1 = r['reviews'][0]['text'] #adds 'text' value to the list below if it exists
    if n > 1: review2 = r['reviews'][1]['text'] #adds 'text' value to the list below if it exists
    if n > 2: review3 = r['reviews'][2]['text'] #adds 'text' value to the list below if it exists
    
    l = [name,busID,rating,url,price,lat,lon,category,review1,review2,review3] #list of attributes read from the json file, including the first three reviews where they exist
    
    #print(l) #prints out the list of information - this is a lot
    
    payload_review.append(l) #adds the list to the above array

with open('yelp.csv','w') as csv_file:
    writer = csv.writer(csv_file) #defines writer function
    writer.writerows(payload_review) #writes the information to the csv

csv_file.close()

# %% CONNECTING TO POSTGIS DATABASE
dbname = 'apee461' #name of the PostGIS database
user = 'postgres' #credentials to log in
host = '130.216.217.56' #host of the database
port = '5432' #port of the database on the host
password = 'bobby2Tables' #very secure credentials in plaintext

connString = "dbname=\'" + dbname + "\' user=\'" + user + "\' host=\'" + host + "\' port=\'" + port + "\' password=\'" + password + "\'" #turns credentials etc into a string instruction for the database connection

conn = psycopg2.connect(connString) #connects to the database

# %% DOING SQL THINGS
#creating a cursor
conn.close() #close any open connections to the database
conn = psycopg2.connect(connString) #reopen that connection

curr = conn.cursor()

#formatting stuffs
sql = "CREATE TABLE public.yelp_data \
( \
    geom geometry, \
    oid serial NOT NULL, \
    busid character varying, \
    name character varying, \
    url character varying, \
    price character varying, \
    category character varying, \
    review1 character varying, \
    review2 character varying, \
    review3 character varying, \
    PRIMARY KEY (oid) \
)"

curr.execute(sql)
conn.commit()

# %% MORE SQL THINGS
#creating a cursor
conn.close()
conn = psycopg2.connect(connString)

curr = conn.cursor()

sql = "CREATE INDEX geom_index ON public.yelp_data USING gist (geom) TABLESPACE pg_default; " #pretty sure this is where the line ends but check weird arrows in pdf

curr.execute(sql)
conn.commit()
conn.close()

# %% EVEN MORE SQL THINGS
#creating a cursor
conn.close()
conn = psycopg2.connect(connString)

curr = conn.cursor()

sqlString = "ST_GeomFromText(\'POINT("+str(b[6])+" "+str(b[5])+")\',4326),\' "+b[1]+" \' ,$$"+b[0]+" \
        $$,\'"+b[3]+"\',\'"+b[4]+"\', \'"+b[2]+"\','"+b[7]+"\',$$"+b[8]+"$$,$$"+b[9]+"$$,$$"+b[10]+"$$"

for b in payload_review[1:] :
    sql = "INSERT INTO yelp_data(geom,busid,name,url,price,rating,category,review1,review2,review3) \
        VALUES(sqlString)"
    curr.execute(sql)
    
    conn.commit()