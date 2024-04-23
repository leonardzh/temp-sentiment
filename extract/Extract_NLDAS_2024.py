import json
import itertools
import ee
import os
from datetime import datetime, timedelta, time
import pandas as pd
import numpy as np
import time as sleeptime #otherwise interferes with datetime.time
from functools import reduce
import math
from multiprocessing import Pool
import warnings
warnings.filterwarnings("ignore")

ee.Authenticate()
ee.Initialize(project='ee-liuliuzheng1208')

os.chdir('/gpfs/data1/oshangp/liuz/sesync')
input_folder = './data/processed/pre_weather'
output_folder = './data/processed/nldas_weather'

def getFiles():
    #Get list of all files that have not yet been processed
    fs = os.listdir(input_folder)
    fs = [f for f in fs if f[-3:] == 'csv']
    
    done = os.listdir(output_folder)
    
    fs = [f for f in fs if f not in done]
    
    fs.sort(reverse=True)
   
    return(fs)

def chunker(iter, size):
    chunks = []
    if size < 1:
        raise ValueError('Chunk size must be greater than 0.')
    for i in range(0, len(iter), size):
        chunks.append(iter[i:(i+size)])
    return chunks

def getDataValues(feats, img, var):
    #First, break feats into lists of lists, each with len < 4500
    feats = chunker(feats, 4500)
    
    allres = []
    for feat in feats:
        res = img.reduceRegions(ee.FeatureCollection(feat), reducer=ee.Reducer.first()).getInfo()
        allres = allres + res['features']
    
    df = pd.DataFrame([x['properties'] for x in allres])
    return(df['first'].values if 'first' in df.columns else np.array([np.NaN]*df.shape[0]))

todos = getFiles()



def processNLDAS(f):
    print(datetime.now(), f)
    
    dat = pd.read_csv(os.path.join(input_folder,f),dtype = {'yyyymmdd':str,'hour':str})
    
    print(dat.shape[0])
    if dat.shape[0] == 0:
        with open(os.path.join(output_folder,f), 'w') as file:
            pass
        print("Skipping " + f)
        return
    
    daydat = pd.DataFrame()
    for hour in dat.hour.unique().tolist():
        sel = dat[dat.hour == hour] 
        img = ee.Image('NASA/NLDAS/FORA0125_H002/A' + f[7:11] + f[11:13] + f[13:15] + '_' + hour.zfill(2) + '00')
        temp = img.select('temperature')
        speh = img.select('specific_humidity')
        pres = img.select('pressure')
        prcp = img.select('total_precipitation')
        srad = img.select('shortwave_radiation')
        lrad = img.select('longwave_radiation')
        wndu = img.select('wind_u')
        wndv = img.select('wind_v')
         
        feats = []
        for i,v in sel.iterrows():
            lon = v['lon']
            lat = v['lat']
            pt = ee.Geometry.Point(lon, lat)
            feat = ee.Feature(pt, {'tweet_created_at_int': v['tweet_created_at_int'], 'id': str(v['id'])})
            feats.append(feat)
         
        for var, img in [('temp',temp),
                 ('speh',speh),
                 ('pres',pres),
                 ('prcp',prcp),
                 ('srad',srad),
                 ('lrad',lrad),
                 ('wndu',wndu),
                 ('wndv',wndv)
                ]:
            sel.loc[:,var] = getDataValues(feats, img, var)

        res = sel[['id','tweet_created_at_int','temp',
            'speh', 'pres', 'prcp', 'srad', 'lrad', 'wndu', 'wndv']]
        daydat = pd.concat([daydat, res])
        
    daydat.to_csv(os.path.join(output_folder,f), index=False)
    sleeptime.sleep(10)

def main():
    todolist = getFiles()
    print(len(todolist))
    # Create a pool of 4 worker processes
    with Pool(1) as p:
        # Map process_item function to each item in the todolist
        p.map(processNLDAS, todolist)

main()

# for f in todos:  
#     print(datetime.now(), f)
    
#     dat = pd.read_csv(os.path.join(input_folder,f),dtype = {'yyyymmdd':str,'hour':str})
    
#     print(dat.shape[0])
#     if dat.shape[0] == 0:
#         with open(os.path.join(output_folder,f), 'w') as file:
#             pass
#         print("Skipping " + f)
#         continue
    
#     daydat = pd.DataFrame()
#     for hour in dat.hour.unique().tolist():
#         sel = dat[dat.hour == hour] 
#         img = ee.Image('NASA/NLDAS/FORA0125_H002/A' + f[7:11] + f[11:13] + f[13:15] + '_' + hour.zfill(2) + '00')
#         temp = img.select('temperature')
#         speh = img.select('specific_humidity')
#         pres = img.select('pressure')
#         prcp = img.select('total_precipitation')
#         srad = img.select('shortwave_radiation')
#         lrad = img.select('longwave_radiation')
#         wndu = img.select('wind_u')
#         wndv = img.select('wind_v')
         
#         feats = []
#         for i,v in sel.iterrows():
#             lon = v['lon']
#             lat = v['lat']
#             pt = ee.Geometry.Point(lon, lat)
#             feat = ee.Feature(pt, {'tweet_created_at_int': v['tweet_created_at_int'], 'id': str(v['id'])})
#             feats.append(feat)
         
#         for var, img in [('temp',temp),
#                  ('speh',speh),
#                  ('pres',pres),
#                  ('prcp',prcp),
#                  ('srad',srad),
#                  ('lrad',lrad),
#                  ('wndu',wndu),
#                  ('wndv',wndv)
#                 ]:
#             sel.loc[:,var] = getDataValues(feats, img, var)

#         res = sel[['id','tweet_created_at_int','temp',
#             'speh', 'pres', 'prcp', 'srad', 'lrad', 'wndu', 'wndv']]
#         daydat = pd.concat([daydat, res])
        
#     daydat.to_csv(os.path.join(output_folder,f), index=False)
#     sleeptime.sleep(10)