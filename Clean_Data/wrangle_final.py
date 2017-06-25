
# coding: utf-8

# In[3]:

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
import numpy as np

cwd = os.getcwd()
print(cwd)

d = dt.date.today()

logfile = 'logger.log'

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',filename=logfile,level=logging.INFO)

with open('configWrangle.json') as json_data_file:
	data = json.load(json_data_file)
	accesskey = data['AWSAccess']
	secretkey = data['AWSSecret']
	
#Create a connection
s3 = boto3.resource('s3',
                    aws_access_key_id = accesskey , 
                    aws_secret_access_key =  secretkey , 
                    config = Config(signature_version='s3v4')
                   )
#boto3.set_stream_logger('boto3.resources', logging.INFO)
#Create a bucket
logging.info('Connection created')
#s3.create_bucket(Bucket='team7pa_assignment1')
#logging.info('Bucket created')

bucket = s3.Bucket('team7pa_assignment1')  
bucketlen = len(list(bucket.objects.all()))
print(bucketlen)

if bucketlen == 0 :
    print(cwd)
    logging.error('No raw data!!')
else: 
    #initial data uploading get files from configinitial
    with open('configWrangle.json') as json_data_file:
        data = json.load(json_data_file)
        rawurl = data['rawData']
        fname = 'raw_data.csv'
        urllib.request.urlretrieve(rawurl, fname)
        data = pd.read_csv(fname)
        #data.describe()
        # ###### Clean data
        reporttype = data[(data["REPORTTPYE"] == 'FM-15') & (data["HOURLYVISIBILITY"] == '10.00')]
        #print(reporttype)
# ###### Clean HOURLYDRYBULBTEMPC column to get in degree celsius from in tengths of  degree celsius
        data['HOURLYDRYBULBTEMPC']=data['HOURLYDRYBULBTEMPC'].astype(str)
        data['HOURLYDRYBULBTEMPC']=data['HOURLYDRYBULBTEMPC'].map(lambda x: x.rstrip('s'))
        #data['HOURLYDRYBULBTEMPC']=data['HOURLYDRYBULBTEMPC'].astype(float)
        data['HOURLYDRYBULBTEMPC'] = (data['HOURLYDRYBULBTEMPC']*10)
        #data['HOURLYDRYBULBTEMPC']


        data['HOURLYWETBULBTEMPC'] = data['HOURLYWETBULBTEMPC'].astype(str)
        data['HOURLYWETBULBTEMPC']=data['HOURLYWETBULBTEMPC'].map(lambda x: x.rstrip('s'))
        #data['HOURLYWETBULBTEMPC'] = data['HOURLYWETBULBTEMPC'].astype(float)
        data['HOURLYWETBULBTEMPC'] = (data['HOURLYWETBULBTEMPC']*10)
        #data['HOURLYWETBULBTEMPC']
# ###### Get the missing values by row and column
        def missing_values(df):
            return sum(df.isnull())

        #print(data.apply(missing_values, axis=0))

        #print(data.apply(missing_values, axis=1).head())


    # ###### Get datatypes of columns

        #print(data.dtypes)
        #print(data)

        newfile = 'PA_' +'{:%d%m%y}'.format(d) +'_WBAN_14737_clean.csv'
        data.to_csv(newfile, index=False)
        key = newfile
        objs = list(bucket.objects.filter(Prefix=key))
        if len(objs) > 0 and objs[0].key == key:
            print("File already exists in your bucket!!")
            logging.error('File already exists in your bucket!!Please remove duplicate file.')
        else:
            data1 = open(newfile, 'rb')
            s3.Bucket('team7pa_assignment1').put_object(Key=newfile, Body=data1)
            logging.info('Clean file uploaded')

    os.chdir(cwd)


# In[ ]:



