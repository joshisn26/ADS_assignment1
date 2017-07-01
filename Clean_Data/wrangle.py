
# coding: utf-8

# In[ ]:




# In[ ]:

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
#Create a bucket
logging.info('Connection created')
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
        data.drop(['HOURLYSKYCONDITIONS','HOURLYPRSENTWEATHERTYPE', 'DAILYMaximumDryBulbTemp','DAILYMinimumDryBulbTemp', 'DAILYAverageDryBulbTemp', 'DAILYDeptFromNormalAverageTemp', 'DAILYAverageRelativeHumidity', 'DAILYAverageDewPointTemp','DAILYAverageWetBulbTemp','DAILYHeatingDegreeDays', 'DAILYCoolingDegreeDays', 'DAILYWeather', 'DAILYPrecip', 'DAILYSnowfall','DAILYSnowDepth', 'DAILYAverageStationPressure', 'DAILYAverageSeaLevelPressure', 'DAILYAverageWindSpeed', 'DAILYPeakWindSpeed' , 'PeakWindDirection','DAILYSustainedWindSpeed','DAILYSustainedWindDirection', 'MonthlyMaxSeaLevelPressureDate', 'MonthlyMaxSeaLevelPressureTime', 'MonthlyMinSeaLevelPressureDate', 'MonthlyMinSeaLevelPressureTime', 'MonthlyTotalHeatingDegreeDays', 'MonthlyTotalCoolingDegreeDays', 'MonthlyDeptFromNormalHeatingDD', 'MonthlyDeptFromNormalCoolingDD','MonthlyTotalSeasonToDateHeatingDD','MonthlyTotalSeasonToDateCoolingDD','MonthlyAverageRH','MonthlyDewpointTemp','MonthlyWetBulbTemp','MonthlyAverageRH','MonthlyAvgHeatingDegreeDays','MonthlyAvgCoolingDegreeDays','MonthlyStationPressure','MonthlySeaLevelPressure','MonthlyAverageWindSpeed','MonthlyTotalSnowfall','MonthlyDeptFromNormalMaximumTemp','MonthlyDeptFromNormalMinimumTemp','MonthlyDeptFromNormalAverageTemp','MonthlyDeptFromNormalPrecip','MonthlyTotalLiquidPrecip','MonthlyGreatestPrecip','MonthlyGreatestPrecipDate','MonthlyGreatestSnowfall','MonthlyGreatestSnowfallDate','MonthlyGreatestSnowDepth', 'MonthlyGreatestSnowDepthDate','MonthlyDaysWithGT90Temp','MonthlyDaysWithLT32Temp','MonthlyDaysWithGT32Temp','MonthlyDaysWithLT0Temp','MonthlyDaysWithGT001Precip','MonthlyDaysWithGT010Precip','MonthlyDaysWithGT1Snow','MonthlyMaxSeaLevelPressureValue','MonthlyMinSeaLevelPressureValue'],inplace=True,axis=1,errors='ignore')                                     
        data.update(data[['HOURLYDRYBULBTEMPF','HOURLYDRYBULBTEMPC','HOURLYWETBULBTEMPF','HOURLYWETBULBTEMPC','HOURLYWindGustSpeed','HOURLYPressureTendency', 'HOURLYPressureChange', 'HOURLYVISIBILITY','MonthlyMaximumTemp','MonthlyMinimumTemp','MonthlyMeanTemp']].fillna(0))
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






