import sys, wget, os
import glob
from pathlib import Path
import pandas as pd
from datetime import datetime as dt
from zipfile import ZipFile
import re, time, requests
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool
import threading
import subprocess

ext_dict = {'export': 'CSV',
            'mentions': 'CSV',
            'gkg': 'csv'}

dir_data = './data/raw'

def create_urls(date_range, type, trans):
    extension = ext_dict[type]
    #if trans:
    urls_trans = [f'http://data.gdeltproject.org/gdeltv2/{date.strftime("%Y%m%d%H%M%S")}.translation.{type}.{extension}.zip' for date in date_range]
    #else:
    urls = [f'http://data.gdeltproject.org/gdeltv2/{date.strftime("%Y%m%d%H%M%S")}.{type}.{extension}.zip' for date in date_range]
    return urls+urls_trans

def create_fns(date_range, type, trans):
    extension = ext_dict[type]
    #if trans:
    fns_trans = [f'{date.strftime("%Y%m%d%H%M%S")}.translation.{type}.{extension}.zip' for date in date_range]
    #else:
    fns = [f'{date.strftime("%Y%m%d%H%M%S")}.{type}.{extension}.zip' for date in date_range]
    return fns+fns_trans

def download_url(args):
    t0 = time.time()
    url, fn = args[0], args[1]
    try:
        r = requests.get(url)
        with open(fn, 'wb') as f:
            f.write(r.content)
        return(url, time.time() - t0)
    except Exception as e:
        print('Exception in download_url():', e)

def download_zip(args):
    t0 = time.time()
    url, fn = args[0], args[1]
    filepath = f'{dir_data}/{fn}'
    try:
        wget.download(url, filepath)
    except Exception as e:
        print(f'Error for {fn}')
        print(e)
    # return filepath, fn
    return(filepath, time.time() - t0)

def unzip(args):
    # loading the temp.zip and creating a zip object
    filepath= args[0]
    with ZipFile(filepath, 'r') as zObject:
        # Extracting all the members of the zip 
        # into a specific location.
        zObject.extractall(path=dir_data)
    csv_path = filepath[:-4]
    os.remove(filepath)
    os.rename(csv_path, csv_path.replace('CSV', 'csv'))

def download_parallel(args):
    cpus = cpu_count()
    pool = ThreadPool(cpus - 1)
    results = pool.imap_unordered(download_zip, args, chunksize=10)
    pool.close()
    pool.join()
    # for result in results:
    #     print('url:', result[0], 'time (s):', result[1])
    return results

def unzip_parallel(args):
    cpus = cpu_count()
    pool = ThreadPool(cpus - 1)
    results = pool.imap_unordered(unzip, args, chunksize=5)
    pool.close()
    pool.join()

def extract(inputs):
    for input in inputs:
        result = download_zip(input)
        unzip(result)
        #print('url:', result[0], 'time (s):', result[1])

#def sequential():
#    for input in inputs:
#        result = download_zip(input)
#        unzip(result)
#        #print('url:', result[0], 'time (s):', result[1])
def transform(dir_data, file, start, end, type):
    #path = Path(dir_data)
    #files = list(path.glob(f'*translation.{type}.csv'))
    if type=='gkg':
        headers = ['GKGRecordID', 'DATE', 'SourceCollectionIdentifier', 'SourceCommonName', 'DocumentIdentifier',
                   'Counts', 'V2Counts', 'Themes', 'V2Themes', 'Locations', 'V2Locations', 'Persons', 'V2Persons',
                   'Organizations', 'V2Organizations', 'V2Tone', 'Dates', 'GCAM', 'SharingImage', 'RelatedImages',
                   'SocialImageEmbeds', 'SocialVideoEmbeds', 'Quotations', 'AllNames', 'Amounts', 'TranslationInfo','Extras']
        df = pd.read_csv(file, delimiter="\t",
                                    header=None,
                                    on_bad_lines=None,
                                    encoding='ISO-8859-1',
                                    names=headers,
                                    dtype={'DATE': "object", 'GKGRecordID': 'object', 'SourceCollectionIdentifier':'object'}
                                    )
        df['TranslationInfo'] = df['TranslationInfo'].astype(str).apply(
            lambda x: re.sub(r'(srclc:)([a-z]+)(.*)', r'\2', x))  # extracting language information
    elif type=='mentions':
        headers = ['GlobalEventID', 'EventTimeDate', 'MentionTimeDate', 'MentionType', 'MentionSourceName',
                   'MentionIdentifier', 'SentenceID', 'Actor1CharOffset', 'Actor2CharOffset', 'ActionCharOffset',
                   'InRawText', 'Confidence', 'MentionDocLen', 'MentionDocTone', 'MentionDocTranslationInfo','Extras']
        df = pd.read_csv(file,delimiter="\t",
                                    header=None,
                                    on_bad_lines=None,
                                    encoding='ISO-8859-1',
                                    names=headers,
                                    dtype={'GlobalEventID':'object', 'EventTimeDate':'object', 'MentionTimeDate':'object',
       'MentionType':'object'}
                         )


        df['MentionDocTranslationInfo'] = df['MentionDocTranslationInfo'].astype(str).apply(
            lambda x: re.sub(r'(srclc:)([a-z]+)(.*)', r'\2', x))  # extracting language information
    else:
        headers= ['GlobalEventID','Day',	'MonthYear',	'Year',	'FractionDate',	'Actor1Code',
                  'Actor1Name',	'Actor1CountryCode',	'Actor1KnownGroupCode',	'Actor1EthnicCode',
                  'Actor1Religion1Code',	'Actor1Religion2Code',	'Actor1Type1Code',	'Actor1Type2Code',
                  'Actor1Type3Code',	'Actor2Code',	'Actor2Name',	'Actor2CountryCode',
                  'Actor2KnownGroupCode',	'Actor2EthnicCode',	'Actor2Religion1Code',	'Actor2Religion2Code',
                  'Actor2Type1Code',	'Actor2Type2Code',	'Actor2Type3Code',	'IsRootEvent',	'EventCode',
                  'EventBaseCode',	'EventRootCode',	'QuadClass',	'GoldsteinScale',	'NumMentions',
                  'NumSources',	'NumArticles',	'AvgTone',	'Actor1Geo_Type',	'Actor1Geo_FullName',
                  'Actor1Geo_CountryCode',	'Actor1Geo_ADM1Code',	'Actor1Geo_ADM2Code',	'Actor1Geo_Lat',
                  'Actor1Geo_Long',	'Actor1Geo_FeatureID',	'Actor2Geo_Type',	'Actor2Geo_FullName',
                  'Actor2Geo_CountryCode',	'Actor2Geo_ADM1Code',	'Actor2Geo_ADM2Code',	'Actor2Geo_Lat',
                  'Actor2Geo_Long',	'Actor2Geo_FeatureID',	'ActionGeo_Type',	'ActionGeo_FullName',
                  'ActionGeo_CountryCode',	'ActionGeo_ADM1Code',	'ActionGeo_ADM2Code',	'ActionGeo_Lat',
                  'ActionGeo_Long',	'ActionGeo_FeatureID',	'DATEADDED',	'SOURCEURL']
        df = pd.read_csv(file,delimiter="\t",
                                    header=None,
                                    on_bad_lines=None,
                                    encoding='ISO-8859-1',
                                    names=headers,
                                    dtype={'GlobalEventID':'object', 'Day':'object', 'MonthYear':'object', 'Year':'object','FractionDate': 'object','IsRootEvent':'object','EventCode':'object', 'EventBaseCode':'object', 'EventRootCode':'object', 'QuadClass':'object','Actor1Geo_Type':'object','Actor2Geo_Type':'object','ActionGeo_Type':'object'}
                                    )
    dir_dest = dir_data+'/headers_'+str(file)[13:-3]+'pkl'
    print(dir_dest)
    df.to_pickle(dir_dest)

    return None

def download_zip_2(urls, fns):
    t0 = time.time()
    url, fn = urls, fns
    filepath = f'{dir_data}/{fn}'
    try:
        # Download the zip files, subprocess is used to run a function (wget here) while letting the principal threads working
        subprocess.call(['wget', '-O', filepath, url])
    except Exception as e:
        print(f'Error for {fn}')
        print(e)
    return (filepath, time.time() - t0)



def unzip_transform(filepath, dir_data, start, end, type):
    with ZipFile(filepath, 'r') as zObject:
        # Extracting all the members of the zip
        # into a specific location.
        zObject.extractall(path=dir_data)
    csv_path = filepath[:-4]
    os.remove(filepath)
    os.rename(csv_path, csv_path.replace('CSV', 'csv'))
    print('csv_path = {}'.format(csv_path))

    #with open(csv_path, 'r') as file:
    #print('open csv path = {}'.format(file))
    transform(dir_data, csv_path, start, end, type)

    os.remove(csv_path)


def load():
    pass

def main():
    start = dt.strptime(sys.argv[1], '%Y%m%d%H')
    end = dt.strptime(sys.argv[2], '%Y%m%d%H')
    type = sys.argv[3]
    trans = sys.argv[4] == True
    date_range = pd.date_range(start, end, freq='15T').to_pydatetime()
    urls=create_urls(date_range, type, trans)
    fns= create_fns(date_range, type, trans)
    inputs = zip(urls, fns)

    t0 = time.time()
    results = download_parallel(inputs)
    unzip_parallel(results)
    # extract(inputs)
    print(f"Total time: {time.time() - t0}")
    # extract(start, end, type, dir_data, trans)

def sequential():
    start = dt.strptime(sys.argv[1], '%Y%m%d%H')
    end = dt.strptime(sys.argv[2], '%Y%m%d%H')
    type = sys.argv[3]
    trans = sys.argv[4] == 'True'
    date_range = pd.date_range(start, end, freq='15T').to_pydatetime()
    urls= create_urls(date_range, type, trans)
    fns= create_fns(date_range, type, trans)

    inputs = zip(urls, fns)
    print(inputs)

    t0 = time.time()
    #results = download_parallel(inputs)
    #unzip_parallel(results)
    # extract(inputs)
    cpus = cpu_count()
    with ThreadPool(processes=cpus-1) as pool:
        for url, file_name in inputs:
            pool.apply_async(download_zip_2(url, file_name))
            dir = dir_data+'/'+file_name
            pool.apply_async(unzip_transform(dir_data+'/'+file_name, dir_data,start, end, type))

    print(f"Total time: {time.time() - t0}")


def main_test():
    start = dt.strptime(sys.argv[1], '%Y%m%d%H')
    end = dt.strptime(sys.argv[2], '%Y%m%d%H')
    type = sys.argv[3]
    trans = sys.argv[4] == 'True'
    date_range = pd.date_range(start, end, freq='15T').to_pydatetime()
    urls= create_urls(date_range, type, trans)
    fns= create_fns(date_range, type, trans)

    inputs = zip(urls, fns)

    t0 = time.time()
    results = download_parallel(inputs)
    unzip_parallel(results)
    # extract(inputs)
    print(f"Total time: {time.time() - t0}")
    # extract(start, end, type, dir_data, trans)

    path = Path(dir_data)
    files = list(path.glob(f'*.{type}.csv'))
    for file in files:
        transform(dir_data,file, start, end, type)
        os.remove(file)
if __name__=='__main__':
    sequential()