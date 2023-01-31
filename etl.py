import sys, wget, os
import glob
from pathlib import Path
import pandas as pd
from datetime import datetime as dt
from zipfile import ZipFile
import re, time, requests
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool

ext_dict = {'export': 'CSV',
            'mentions': 'CSV',
            'gkg': 'csv'}

dir_data = './data/raw'

def create_urls(date_range, type, trans):
    extension = ext_dict[type]
    if trans:
        urls = [f'http://data.gdeltproject.org/gdeltv2/{date.strftime("%Y%m%d%H%M%S")}.translation.{type}.{extension}.zip' for date in date_range]
    else:
        urls = [f'http://data.gdeltproject.org/gdeltv2/{date.strftime("%Y%m%d%H%M%S")}.{type}.{extension}.zip' for date in date_range]
    return urls

def create_fns(date_range, type, trans):
    extension = ext_dict[type]
    if trans:
        fns = [f'{date.strftime("%Y%m%d%H%M%S")}.translation.{type}.{extension}.zip' for date in date_range]
    else:
        fns = [f'{date.strftime("%Y%m%d%H%M%S")}.{type}.{extension}.zip' for date in date_range]
    return fns

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

def transform(dir_data, start, end, type, trans):
    path = Path(dir_data)
    if trans==True:
        files = list(path.glob(f'*translation.{type}.csv'))
    else:
        files = list(path.glob(f'*0.{type}.csv'))
    header_gkg = ['GKGRecordID', 'DATE', 'SourceCollectionIdentifier', 'SourceCommonName', 'DocumentIdentifier','Counts', 'V2Counts', 'Themes', 'V2Themes', 'Locations', 'V2Locations', 'Persons', 'V2Persons','Organizations', 'V2Organizations', 'V2Tone', 'Dates', 'GCAM', 'SharingImage', 'RelatedImages','SocialImageEmbeds', 'SocialVideoEmbeds', 'Quotations', 'AllNames', 'Amounts', 'TranslationInfo','Extras']
    df = pd.concat([pd.read_csv(f,delimiter="\t",
                                header=None,
                                on_bad_lines=None,
                                encoding='ISO-8859-1',
                                names=header_gkg,
                                dtype={'DATE':"object", 'GKGRecordID':'object'}
                                )
                    for f in files
                    ])

    if trans==True:
        df.to_csv(dir_data+f'/dataset_trans_{type}_from_{start}_to_{end}.csv')
        df['TranslationInfo'] = df['TranslationInfo'].astype(str).apply(
            lambda x: re.sub(r'(srclc:)([a-z]+)(.*)', r'\2', x))  # extracting language information
    else:
        df.to_csv(dir_data + f'/dataset_{type}_from_{start}_to_{end}.csv')#


def load():
    pass

def main():
    start = dt.strptime(sys.argv[1], '%Y%m%d%H')
    end = dt.strptime(sys.argv[2], '%Y%m%d%H')
    type = sys.argv[3]
    trans = sys.argv[4] == 'True'

    date_range = pd.date_range(start, end, freq='15T').to_pydatetime()
    urls = create_urls(date_range, type, trans)
    fns = create_fns(date_range, type, trans)
    inputs = zip(urls, fns)

    t0 = time.time()
    results = download_parallel(inputs)
    unzip_parallel(results)
    # extract(inputs)
    print(f"Total time: {time.time() - t0}")
    # extract(start, end, type, dir_data, trans)
    # transform(dir_data, start, end, type, trans)

if __name__=='__main__':
    main()