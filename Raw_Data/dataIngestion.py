# coding: utf-8

# In[1]:

import json
import urllib.request
import boto3
from botocore.client import Config
import datetime as dt
import arrow
import pandas as pd
import glob
import logging
import os

cwd = os.getcwd()
print(cwd)


d = dt.date.today()
p1 = cwd + '/Initial_csv'
p2 = cwd + '/Newdata'

if not os.path.exists(p1):
    os.mkdir('Initial_csv')
if not os.path.exists(p2):
    os.mkdir('Newdata')

logfile = 'logger.log'

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',filename=logfile,level=logging.INFO)

with open('config.json') as json_data_file:
    data = json.load(json_data_file)
    accesskey = data['AWSAccess']
    secretkey = data['AWSSecret']
    
#Create a connection
s3 = boto3.resource('s3',
                    aws_access_key_id =  accesskey, 
                    aws_secret_access_key =  secretkey , 
                    config = Config(signature_version='s3v4')
                   )
#boto3.set_stream_logger('boto3.resources', logging.INFO)
#Create a bucket
#logging.info('Connection created')
#s3.create_bucket(Bucket='team7pa_assignment1')
logging.info('Bucket created')
bucket = s3.Bucket('team7pa_assignment1')  
bucketlen = len(list(bucket.objects.all()))
print(bucketlen)
if bucketlen == 0 :
    print(cwd)
    #initial data uploading get files from configinitial
    with open('configinitial.json') as json_data_file:
        data = json.load(json_data_file)
        for i in data["result"]:
            url = i["link"]
            fname = url[37:]
            os.chdir(p1)
            cwd1 = os.getcwd()
            urllib.request.urlretrieve(url, fname)
    
#initial data uploading get merge csv  
    allFiles = glob.glob(cwd1 + "/*.csv")
    frame = pd.DataFrame([])
    for file_ in allFiles:
        df = pd.read_csv(file_,index_col=False, header=0)
        frame = frame.append(df)
    initialfile = "PA_" + '{:%d%m%y}'.format(d) + "_WBAN_14737.csv"
    frame.to_csv(initialfile, index=False)
    #upload initial data
    data1 = open(initialfile, 'rb')
    s3.Bucket('team7pa_assignment1').put_object(Key=initialfile, Body=data1)
    logging.info('Bucket empty: Initial data uploaded')    
   
    
else:   

    os.chdir(cwd)
    d = dt.date.today()
    print(d)
    #upload latest data
    with open('config.json') as json_data_file:
        data = json.load(json_data_file)
    url = data['link']
    os.chdir(p2)
    #get data from config file
    urllib.request.urlretrieve(url, 'configfiledata.csv')
    logging.info('Data downloaded fron config file path')

    df2 = pd.read_csv('configfiledata.csv',index_col=False, header=0,dtype=object)
    #download latest file from bucket

    b = list(bucket.objects.all())
    l = [(k, k.last_modified) for k in b]
    l1 = [k for k, v in sorted(l, key=lambda p: p[1], reverse=True)]
    key_to_download = l1[0].key

    s3.Bucket('team7pa_assignment1').download_file(key_to_download, key_to_download)
    df1 = pd.read_csv(key_to_download,index_col=False, header=0,dtype=object)
    logging.info('Latest Data From bucket downloaded')
    df3 = df1.append(df2)


    #upload latest data
    newfile = 'PA1_' +'{:%d%m%y}'.format(d) +'_WBAN_14737.csv'
    df3.to_csv(newfile, index=False)
    key = newfile
    objs = list(bucket.objects.filter(Prefix=key))
    if len(objs) > 0 and objs[0].key == key:
        print("File already exists in your bucket!!")
        logging.error('File already exists in your bucket!!Please remove duplicate file.')
    else:
        data2 = open(newfile, 'rb')
        s3.Bucket('team7pa_assignment1').put_object(Key=newfile, Body=data2)
        logging.info('New file uploaded')
os.chdir(cwd)
