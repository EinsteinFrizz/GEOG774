# -*- coding: utf-8 -*-
"""
Created on Wed Sep 30 12:43:06 2020

@author: apee461
"""
#currently up to running the model
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

## %% READING FROM YELP API
#with open('yelp.json','w') as f:
#    json.dump(payload,f)
#
##payload_good = [['name','rating','url','price','lat','long','category']]
#payload_review = [['Name','Bus_ID','Rating','Url','Price','Lat','Long','Category','Review_1','Review_2','Review_3']]
#
#for business in payload['businesses']: #for loop iterates through each entry in the array
#    lat = str(business['coordinates']['latitude']) #set variable to the string of latitude value
#    lon = str(business['coordinates']['longitude']) #set variable to the string of longitude value
#    name = str(business['name']) #set variable to the string of name of the business
#    url = str(business['url']) #set variable to the string of URL of the business
#    price = business['price'] if 'price' in business else "null" #set variable to the priciness of the business if it is defined, otherwise set it to 'null' string
#    rating = str(business['rating']) #set variable to the string of rating of the business
#    category = str(business['categories'][0]['title']) #set variable to the string of the first category that the business fits in, using the 'title' value
#    busID = business['id']
#    
#    r = requests.get('https://api.yelp.com/v3/businesses/'+busID+'/reviews',params={},headers=headers).json()
#    
#    n = len(r['reviews'])
#    review1,review2,review3 = 'null','null','null' #initialises variables
#    if n > 0: review1 = r['reviews'][0]['text'] #adds 'text' value to the list below if it exists
#    if n > 1: review2 = r['reviews'][1]['text'] #adds 'text' value to the list below if it exists
#    if n > 2: review3 = r['reviews'][2]['text'] #adds 'text' value to the list below if it exists
#    
#    l = [name,busID,rating,url,price,lat,lon,category,review1,review2,review3] #list of attributes read from the json file, including the first three reviews where they exist
#    
#    #print(l) #prints out the list of information - this is a lot
#    
#    payload_review.append(l) #adds the list to the above array
#
#with open('yelp.csv','w') as csv_file:
#    writer = csv.writer(csv_file) #defines writer function
#    writer.writerows(payload_review) #writes the information to the csv
#
#csv_file.close()

## %% CONNECTING TO POSTGIS DATABASE
#dbname = 'apee461' #name of the PostGIS database
#user = 'postgres' #credentials to log in
#host = '130.216.217.56' #host of the database
#port = '5432' #port of the database on the host
#password = 'bobby2Tables' #very secure credentials in plaintext
#
#connString = "dbname=\'" + dbname + "\' user=\'" + user + "\' host=\'" + host + "\' port=\'" + port + "\' password=\'" + password + "\'" #turns credentials etc into a string instruction for the database connection
#
#conn = psycopg2.connect(connString) #connects to the database

## %% DOING SQL THINGS
##creating a cursor
#conn.close() #close any open connections to the database
#conn = psycopg2.connect(connString) #reopen that connection
#
#curr = conn.cursor()
#
##formatting stuffs
#sql = "DROP TABLE IF EXISTS public.yelp_data; CREATE TABLE public.yelp_data \
#( \
#    geom geometry, \
#    oid serial NOT NULL, \
#    busid character varying, \
#    name character varying, \
#    url character varying, \
#    price character varying, \
#    rating character varying, \
#    category character varying, \
#    review1 character varying, \
#    review2 character varying, \
#    review3 character varying, \
#    PRIMARY KEY (oid) \
#)"
#
#curr.execute(sql)
#conn.commit()

## %% MORE SQL THINGS
##creating a cursor
#conn.close()
#conn = psycopg2.connect(connString)
#
#curr = conn.cursor()
#
#sql = "CREATE INDEX geom_index ON public.yelp_data USING gist (geom) TABLESPACE pg_default; " #pretty sure this is where the line ends but check weird arrows in pdf
#
#curr.execute(sql)
#conn.commit()
#conn.close()

## %% EVEN MORE SQL THINGS
##creating a cursor
#conn.close()
#conn = psycopg2.connect(connString)
#
#curr = conn.cursor()
#
#for b in payload_review[1:] :
#    sqlString = "ST_GeomFromText(\'POINT("+str(b[6])+" "+str(b[5])+")\',4326),\' "+b[1]+" \' ,$$"+b[0]+" \
#        $$,\'"+b[3]+"\',\'"+b[4]+"\', \'"+b[2]+"\','"+b[7]+"\',$$"+b[8]+"$$,$$"+b[9]+"$$,$$"+b[10]+"$$"
#    sql = "INSERT INTO yelp_data(geom,busid,name,url,price,rating,category,review1,review2,review3) \
#        VALUES("+str(sqlString)+")"
#    curr.execute(sql)
#    
#    conn.commit()
    
## %% IMPORTING DATA
#logging.info("Starting GENSIM code")
#documents = []
#conn = psycopg2.connect(connString)
#curr = conn.cursor()
#curr.execute("SELECT review1,review2,review3 FROM yelp_data")
#review_data = []
#for i in curr:
#    if i[0] != 'null': review_data.append(i[0])
#    if i[1] != 'null': review_data.append(i[1])
#    if i[2] != 'null': review_data.append(i[2])
#
#logging.info("%s reviews received" , len(review_data))
#conn.close()

## %% STOPLIST STUFF
#for review in review_data:
#    documents.append(' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|(gt)"," ",review).split()).lower())
#    
#logging.info("CORPUS SIZE AFTER REGEX: "+str(len(documents)))
#
#stoplist = set("a about above after again against all am an and any are aren\'t as at be because been before being below between both but by can\'t cannot could couldn\'t did didn\'t do does doesn\'t doing don\'t down during each few for from further had hadn\'t has hasn\'t have haven\'t having he he'd he\'ll he\'s her here here\'s hers herself him himself his how how\'s i i\'d i\'ll i\'m i\'ve if in into is isn\'t it it\'s its itself let\'s me more most mustn't my myself no nor not of off on once only or other ought our ours ourselves out over own same shan\'t she she\'d she\'ll she\'s should shouldn\'t so some something such than that that\'s the their theirs them themselves then there there\'s these they they\'d they\'ll they're they\'ve this those through to too under until up very was wasn\'t we we\'d we\'ll we\'re we\'ve were weren\'t what what\'s when when\'s where where\'s which while who who\'s whom why why\'s with won\'t would wouldn\'t you you\'d you\'ll you\'re you\'ve your yours yourself yourselves a b c d e f g h i j k l m n o p q r s t u v w x y z don que con en de le sus el re ll rt si go can la ve hi ur dis ain es wanna couldn thx je te ese rn tu ya lo como por pm ca amp como me je oye mi del tho un une da los doin yo nah im lt da se su thru vs una mas uno imma didn ni para tira pa las nos esto dm say know like ima just thought tx way whats say get said dem esta going dont get san qu bien even mf yea good seems knew thing except san yay sabes really yes mis soy vaz em wasn xo got goes need never il ah hey doesn vos keep already telling keeps people much think talk will estar cuando telling shouldno ida llevar much talk feel every someone oh haha miss cause ser tiempo now told come back one al watching thank cant back looks great much mean plase seb dormir ser plzz thanks new literally soon take must time try still end join tbt see las right look anything anymore better tag make makes sure start okay aren give hard pretty let finally start many ever na ng ko stop looking seeing actually things ha probably tonight nice today says ready without done everyone nothing tilltell meet coming others next absolutely hoy bye ma made tug yeah enjoy lil late day side piece find shout dude dudes appearently favourite definitely 0 1 2 3 4 5 6 7 8 9 tell find words want met gea leave please guys guy us sounds otherwise big name amazing missing biyi isn happened besides donde via vamos sleep bed morning put hours finna af phil saying amor est mine iight put joe fuck fucking shit stay stand row wear via hours aqui hay monday tuesday wednesday thursday friday saturday sunday remember close long jerry centro last omg lol lmfao rofl place seen early gotta whole ones stand ok wait lmao year trippin hasta messing lame ugh yet wtf idk act bae away anyone bring damn ig pues alright tf might xd wrong starting little maybe gets sometimes known getting whatever later together left gonna else tf anybody nobodyana starting whatever needs casa happiest bout lefttil eso almost everybody till swear yall around excited best wrong follow far annoying pls gonna favorite babe maybe wants".split())
#s = ""
#for w in stoplist:
#    s += w + " "
#    stoplist = set(s.split())
#
##tokenise
#texts = [[word for word in document.lower().split() if word not in stoplist] for document in documents]
#logging.info("CORPUS SIZE AFTER STOPLIST: "+str(len(texts)))
#
##remove words that appear once
#all_tokens = sum(texts, [])
#logging.info("beginning tokenisation")
#tokens_once = set(word for word in set(all_tokens) if all_tokens.count(word) == 1)
#logging.info("words tokenised, starting single mentioned word reduction")
#texts = [[word for word in text if word not in tokens_once] for text in texts]
#logging.info("words mentioned only once removed")
#
#logging.info("CORPUS SIZE AFTER EMPTY ROWS REMOVED: "+str(len(texts)))
#dictionary = corpora.Dictionary(texts)
#
#corpus = [dictionary.doc2bow(text) for text in texts]
#
#tfidf = models.TfidfModel(corpus) #initialise the model
#
#corpus_tfidf = tfidf[corpus] #apply TFIDF transform to the entire corpus
#
##actually run the model
#logging.info(len(corpus_tfidf))
#logging.info("starting LDA model")
#
#model = models. ldamodel.LdaModel(corpus_tfidf, id2word=dictionary, alpha=0.001, num_topics=10, update_every=0, passes=50)
#
#pp(model.show_topics())

# %% LOAD FILE INTO PYTHON
with open('rev_subset_50k.json',encoding='utf8') as f:
        lines = f.readlines()

documents = []

for line in lines:
        d = ast.literal_eval(str(line)[:-1])
        documents.append(d['text'])
#from here just do the same as earlier

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

#formatting stuffs NOTE THAT YELP_DATA HAS BEEN CHANGED TO SUPPLIED_YELP
sql = "DROP TABLE IF EXISTS public.supplied_yelp; CREATE TABLE public.supplied_yelp \
( \
    geom geometry, \
    oid serial NOT NULL, \
    busid character varying, \
    name character varying, \
    url character varying, \
    price character varying, \
    rating character varying, \
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

#added IF NOT EXISTS to stop an error however this may need to be redone
sql = "CREATE INDEX IF NOT EXISTS geom_index ON public.supplied_yelp USING gist (geom) TABLESPACE pg_default; " #pretty sure this is where the line ends but check weird arrows in pdf

curr.execute(sql)
conn.commit()
conn.close()

# %% EVEN MORE SQL THINGS
#creating a cursor
conn.close()
conn = psycopg2.connect(connString)

curr = conn.cursor()

for b in payload_review[1:] :
    sqlString = "ST_GeomFromText(\'POINT("+str(b[6])+" "+str(b[5])+")\',4326),\' "+b[1]+" \' ,$$"+b[0]+" \
        $$,\'"+b[3]+"\',\'"+b[4]+"\', \'"+b[2]+"\','"+b[7]+"\',$$"+b[8]+"$$,$$"+b[9]+"$$,$$"+b[10]+"$$"
    sql = "INSERT INTO supplied_yelp(geom,busid,name,url,price,rating,category,review1,review2,review3) \
        VALUES("+str(sqlString)+")"
    curr.execute(sql)
    
    conn.commit()
    
# %% IMPORTING DATA
logging.info("Starting GENSIM code")
documents = []
conn = psycopg2.connect(connString)
curr = conn.cursor()
curr.execute("SELECT review1,review2,review3 FROM yelp_data")
review_data = []
for i in curr:
    if i[0] != 'null': review_data.append(i[0])
    if i[1] != 'null': review_data.append(i[1])
    if i[2] != 'null': review_data.append(i[2])

logging.info("%s reviews received" , len(review_data))
conn.close()

# %% STOPLIST STUFF
for review in review_data:
    documents.append(' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|(gt)"," ",review).split()).lower())
    
logging.info("CORPUS SIZE AFTER REGEX: "+str(len(documents)))

stoplist = set("a about above after again against all am an and any are aren\'t as at be because been before being below between both but by can\'t cannot could couldn\'t did didn\'t do does doesn\'t doing don\'t down during each few for from further had hadn\'t has hasn\'t have haven\'t having he he'd he\'ll he\'s her here here\'s hers herself him himself his how how\'s i i\'d i\'ll i\'m i\'ve if in into is isn\'t it it\'s its itself let\'s me more most mustn't my myself no nor not of off on once only or other ought our ours ourselves out over own same shan\'t she she\'d she\'ll she\'s should shouldn\'t so some something such than that that\'s the their theirs them themselves then there there\'s these they they\'d they\'ll they're they\'ve this those through to too under until up very was wasn\'t we we\'d we\'ll we\'re we\'ve were weren\'t what what\'s when when\'s where where\'s which while who who\'s whom why why\'s with won\'t would wouldn\'t you you\'d you\'ll you\'re you\'ve your yours yourself yourselves a b c d e f g h i j k l m n o p q r s t u v w x y z don que con en de le sus el re ll rt si go can la ve hi ur dis ain es wanna couldn thx je te ese rn tu ya lo como por pm ca amp como me je oye mi del tho un une da los doin yo nah im lt da se su thru vs una mas uno imma didn ni para tira pa las nos esto dm say know like ima just thought tx way whats say get said dem esta going dont get san qu bien even mf yea good seems knew thing except san yay sabes really yes mis soy vaz em wasn xo got goes need never il ah hey doesn vos keep already telling keeps people much think talk will estar cuando telling shouldno ida llevar much talk feel every someone oh haha miss cause ser tiempo now told come back one al watching thank cant back looks great much mean plase seb dormir ser plzz thanks new literally soon take must time try still end join tbt see las right look anything anymore better tag make makes sure start okay aren give hard pretty let finally start many ever na ng ko stop looking seeing actually things ha probably tonight nice today says ready without done everyone nothing tilltell meet coming others next absolutely hoy bye ma made tug yeah enjoy lil late day side piece find shout dude dudes appearently favourite definitely 0 1 2 3 4 5 6 7 8 9 tell find words want met gea leave please guys guy us sounds otherwise big name amazing missing biyi isn happened besides donde via vamos sleep bed morning put hours finna af phil saying amor est mine iight put joe fuck fucking shit stay stand row wear via hours aqui hay monday tuesday wednesday thursday friday saturday sunday remember close long jerry centro last omg lol lmfao rofl place seen early gotta whole ones stand ok wait lmao year trippin hasta messing lame ugh yet wtf idk act bae away anyone bring damn ig pues alright tf might xd wrong starting little maybe gets sometimes known getting whatever later together left gonna else tf anybody nobodyana starting whatever needs casa happiest bout lefttil eso almost everybody till swear yall around excited best wrong follow far annoying pls gonna favorite babe maybe wants".split())
s = ""
for w in stoplist:
    s += w + " "
    stoplist = set(s.split())

#tokenise
texts = [[word for word in document.lower().split() if word not in stoplist] for document in documents]
logging.info("CORPUS SIZE AFTER STOPLIST: "+str(len(texts)))

#remove words that appear once
all_tokens = sum(texts, [])
logging.info("beginning tokenisation")
tokens_once = set(word for word in set(all_tokens) if all_tokens.count(word) == 1)
logging.info("words tokenised, starting single mentioned word reduction")
texts = [[word for word in text if word not in tokens_once] for text in texts]
logging.info("words mentioned only once removed")

logging.info("CORPUS SIZE AFTER EMPTY ROWS REMOVED: "+str(len(texts)))
dictionary = corpora.Dictionary(texts)

corpus = [dictionary.doc2bow(text) for text in texts]

tfidf = models.TfidfModel(corpus) #initialise the model

corpus_tfidf = tfidf[corpus] #apply TFIDF transform to the entire corpus

#actually run the model
logging.info(len(corpus_tfidf))
logging.info("starting LDA model")

model = models. ldamodel.LdaModel(corpus_tfidf, id2word=dictionary, alpha=0.001, num_topics=10, update_every=0, passes=50)

pp(model.show_topics())

# %% OUTPUT TO CSV
with open('output_supplied.csv','w') as csv_file:
    writer = csv.writer(csv_file) #defines writer function
    writer.writerows(model.show_topics(formatted=False)) #writes the information to the csv
    #current error is that the stuff inside writerows() needs to be iterable

csv_file.close()