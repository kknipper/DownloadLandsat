#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Sep 26 06:35:14 2017

@author: kyle.knipper
"""
import os
import pandas as pd
from datetime import datetime
import json
import requests
from time import sleep
import logging
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.DEBUG)
import urllib

import wget
import tarfile
import gzip
import zipfile

#########################################################################################
#%% DEFINE PARAMETERS IN THIS SECTION - This is the only section that needs to be altered
###########################################n##############################################
base = '/stor/array01/Kyle/ETFusion_1_5/Ripperdan/' #define your base/root directory. This is the same as when running DisALEXI/Fusion. Line 214 finishes defining where the data will be downloaded
collection = 7 #collection=5 for Landsat 5, collection=7 for landsat 7, collection=8 for Landsat 8
path = 43
row = 34
start_date = '2015-02-09'
end_date = '2015-02-11'
cloud = 100 #keep at 100% if you'd like to make sure every scene available is downloaded (will need to check for cloud cover after download)
auth = [('username'),('password')] #put in your username and password for Landsat Earth Explorer

#########################################################################################
### You should not have to change anything below this line ###
#########################################################################################

#%% Define functions used to gather data
def espa_api(endpoint, verb='get', body=None, uauth=None):
    """ Suggested simple way to interact with the ESPA JSON REST API """  
    host = 'https://espa.cr.usgs.gov/api/v1/'
    auth_tup = uauth if uauth else (username, password)
    response = getattr(requests, verb)(host + endpoint, auth=auth_tup, json=body)
    print('{} {}'.format(response.status_code, response.reason))
    data = response.json()
    if isinstance(data, dict):
        messages = data.pop("messages", None)
        if messages:
            print(json.dumps(messages, indent=4))
    try:
        response.raise_for_status()
    except Exception as e:
        print(e)
        return None
    else:
        return data
    
def search(collection,path,row,start_date,end_date,cloud):
   
    end = datetime.strptime(end_date, '%Y-%m-%d')
    
    if collection == 5:
        metadataUrl = 'https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/LANDSAT_TM_C1.csv'
    if collection == 7:
        metadataUrl = 'https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/LANDSAT_ETM_C1.csv'
    if collection == 8:
        metadataUrl = 'https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/LANDSAT_8_C1.csv'
    
    fn = metadataUrl.split(os.sep)[-1]
    
    # looking to see if metadata CSV is available and if its up to the date needed
    #if os.path.exists(fn):
    #    d = datetime.fromtimestamp(os.path.getmtime(fn))
    #    if ((end.year>d.year) and (end.month>d.month) and (end.day>d.day)):
    #        wget.download(metadataUrl, bar=None)
    #else:
    wget.download(metadataUrl, bar=None)
        
    metadata = pd.read_csv(fn)
    
    if collection == 7:
        output = metadata[(metadata.acquisitionDate >= start_date) & (metadata.acquisitionDate < end_date) & 
                          (metadata.path == path) & (metadata.row == row) & 
                          (metadata.cloudCoverFull <= cloud) & 
                          (metadata.DATA_TYPE_L1 == 'L1TP') &
                          ((metadata.COLLECTION_CATEGORY == 'T1') | (metadata.COLLECTION_CATEGORY == 'A1'))].LANDSAT_PRODUCT_ID
                              
    if collection == 8 or collection == 5:
        output = metadata[(metadata.acquisitionDate >= start_date) & (metadata.acquisitionDate < end_date) &
                          (metadata.path == path) & (metadata.row == row) &
                          (metadata.cloudCoverFull <= cloud) &
                          (metadata.DATA_TYPE_L1 == 'L1TP') &
                          (metadata.COLLECTION_CATEGORY == 'T1')].LANDSAT_PRODUCT_ID
    
    os.system("rm " + fn)
    
    return output.values     
 
def extract_archive(source_path, destination_path=None, delete_originals=False):
    """
    Attempts to decompress the following formats for input filepath
    Support formats include `.tar.gz`, `.tar`, `.gz`, `.zip`.
    :param source_path:         a file path to an archive
    :param destination_path:    path to unzip, will be same name with dropped extension if left None
    :param delete_originals:    Set to "True" if archives may be deleted after
                                their contents is successful extracted.
    """

    head, tail = os.path.split(source_path)

    def set_destpath(destpath, file_ext):
        if destpath is not None:
            return destpath
        else:
            return os.path.join(head, tail.replace(file_ext, ""))

    if source_path.endswith(".tar.gz"):
        with tarfile.open(source_path, 'r:gz') as tfile:
            tfile.extractall(set_destpath(destination_path, ".tar.gz"))
            ret = destination_path

    # gzip only compresses single files
    elif source_path.endswith(".gz"):
        with gzip.open(source_path, 'rb') as gzfile:
            content = gzfile.read()
            with open(set_destpath(destination_path, ".gz"), 'wb') as of:
                of.write(content)
            ret = destination_path

    elif source_path.endswith(".tar"):
        with tarfile.open(source_path, 'r') as tfile:
            tfile.extractall(set_destpath(destination_path, ".tar"))
            ret = destination_path

    elif source_path.endswith(".zip"):
        with zipfile.ZipFile(source_path, "r") as zipf:
            zipf.extractall(set_destpath(destination_path, ".zip"))
            ret = destination_path

    else:
        raise Exception("supported types are tar.gz, gz, tar, zip")

    print("Extracted {0}".format(source_path))
    if delete_originals:
        os.remove(source_path)

    return ret    

class BaseDownloader(object):
    """ basic downloader class with general/universal download utils """

    def __init__(self, local_dir):
        self.local_dir = local_dir
        self.queue = []

        if not os.path.exists(local_dir):
            os.mkdir(local_dir)

    @staticmethod
    def _download(source, dest, retries=2):
        trynum = 0
        while trynum < retries:
            try:
                wget.download(url=source, out=dest)
                return dest
            except:
                sleep(1)

    @staticmethod
    def _extract(source, dest):
        """ extracts a file to destination"""
        return extract_archive(source, dest, delete_originals=False)

    def _raw_destination_mapper(self, source):
        """ returns raw download destination from source url"""
        filename = os.path.basename(source)
        return os.path.join(self.local_dir, filename)

    def _ext_destination_mapper(self, source):
        """ maps a raw destination into an extracted directory dest """
        filename = os.path.basename(source).replace(".tar.gz", "")
        tilename = filename
        return os.path.join(self.local_dir, tilename)
                
    def download(self, source, mode='w', cleanup=True):
        """
        Downloads the source url and extracts it to a folder. Returns
        a tuple with the extract destination, and a bool to indicate if it is a
        fresh download or if it was already found at that location.

        :param source:  url from which to download data
        :param mode:    either 'w' or 'w+' to write or overwrite
        :param cleanup: use True to delete intermediate files (the tar.gz's)
        :return: tuple(destination path (str), new_download? (bool))
        """
        raw_dest = self._raw_destination_mapper(source)
        ext_dest = self._ext_destination_mapper(raw_dest)
         
        if not os.path.exists(ext_dest) or mode == 'w+':
            self._download(source, raw_dest)
            self._extract(raw_dest, ext_dest)
            fresh = True                     
        else:
            print("Found: {0}, Use mode='w+' to force rewrite".format(ext_dest))
            fresh = False
        
        if cleanup and os.path.exists(raw_dest):
            os.remove(raw_dest)
        
        return ext_dest, fresh
    
def org_destination_mapper(source, base):
        tilename = 'download/landsat/org_tar'
        filename = os.path.join(base,tilename)
        if not os.path.exists(filename):
            print('org_tar path does not exists')
            exit
        else:
            wget.download(url=source,out=filename)
            return filename

#%% MAIN BODY
host = 'https://espa.cr.usgs.gov/api/v1/'
TIMEOUT=86400

# check on dates and collection
#if collection == 5 and start_date[0:4] >= 2012:
#    print("wrong date range for Landsat5")
#    exit
    
#if collection == 7 and start_date[0:4] <= 1999:
#    print("wrong date range for Landsat7, no data available before 1999")
#    exit
    
#if collection == 8 and start_date[0:4] <= 2012:
#    print("wrong date range for Landsat8, no data available before 2013")
#    exit

username = auth[0]
password = auth[1]
  
print("Searching for Landsat scenes that fit criteria...this might take a few seconds")
sceneIDs = search(collection,path,row,start_date,end_date,cloud)
    
l_tiles=[]
    
for sceneID in sceneIDs:
    l_tiles.append(sceneID)

print("Done! These are the scenes that will be downloaded: ")

for k in range(len(l_tiles)) :
    print(l_tiles[k])
    
#%% Check to make sure the ESPA System is up and not down for maintenance
link1 = 'https://espa.cr.usgs.gov/'
link2 = 'https://espa.cr.usgs.gov/static/message_content.html'

f = urllib.urlopen(link1)
myfile = f.read()

f2 = urllib.urlopen(link2)
main_str = f2.read()

words = 'ESPA - System Maintenance'

if words in myfile:
    print('The ESPA site is likely down for maintenance, here is what the website is saying:')
    print(main_str)
    exit()
#%%    
# set order data    
order = espa_api('available-products', body=dict(inputs=l_tiles), uauth=(username,password))
print('Submitting order request now...')
print(json.dumps(order, indent=4))
order['format']='gtiff'

# check for "date_restricted"
ddd = json.loads(json.dumps(order))

for item in ddd:
    if item == "date_restricted":
        print('There is a date restriction error in the dataset...finding the scene and deleting now...')
        date_with_error = str(ddd.get(item).get('sr')[0])
        print('date with error = ' + str(date_with_error))
        
        l_tiles.remove(date_with_error)
                
        print("new tiles for download created")

           
# reorder data
order = espa_api('available-products', body=dict(inputs=l_tiles), uauth=(username,password))
print('Submitting order request now...')
print(json.dumps(order, indent=4))
order['format']='gtiff'       
            
#place the order
print('POST /api/v1/order')
resp = espa_api('order', verb='post', body=order, uauth=(username,password))
print(json.dumps(resp, indent=4))

orderid = resp['orderid']
print('orderid = ' + str(orderid))
        
#%% wait for order, once ready, download        
print('waiting for order...')
           
complete = False
reached_TIMEOUT = False
starttime = datetime.now()
        
while not complete and not reached_TIMEOUT:
    resp = espa_api('item-status/{0}'.format(orderid), uauth=(username,password))
    
    if orderid not in resp:
        raise Exception('Order ID {} not found'.format(orderid))

    urls = [_.get('product_dload_url') for _ in resp[orderid]]
    
    elapsed_time = (datetime.now() - starttime).seconds
    reached_TIMEOUT = elapsed_time > TIMEOUT
    print('elapsed time is {0}m'.format(elapsed_time / 60.0))
    
    if all(urls) == True:
        
        for s in range(len(urls)):
        #for s in range(9,len(urls)):
            print('File {0} of {1} for order: {2}'.format(s + 1,len(urls), orderid))
            print(urls[s])
            
            downloader = BaseDownloader(base +'download/landsat/')
                       
            downloader.download(urls[s])
            org_destination_mapper(urls[s],base)
                       
            
            del downloader
            
        complete = True
        print('Download Complete!')
    
    if not complete:
        sleep(300)
        

