import sys, wget, os
import pandas as pd
from datetime import datetime as dt
from zipfile import ZipFile

ext_dict = {'export': 'CSV',
            'mentions': 'CSV',
            'gkg': 'csv'}

def download_zip(date, type, extension):
    str_date = date.strftime("%Y%m%d%H%M%S")
    url = f'http://data.gdeltproject.org/gdeltv2/{str_date}.{type}.{extension}.zip'
    filename = f'{str_date}.{type}.{extension}.zip'
    filepath = f'data/raw/{filename}'
    try:
        if not os.path.exists(filepath):
            wget.download(url, filepath)
    except Exception as e:
        print(f'Error for {filename}')
        print(e)
    return filepath, filename

def unzip(filepath, filename):
    # loading the temp.zip and creating a zip object
    with ZipFile(filepath, 'r') as zObject:
        # Extracting all the members of the zip 
        # into a specific location.
        path = zObject.extract(member=filename.replace('.zip', ''), path='data/raw')
    if os.path.exists(filepath):
        os.remove(filepath)
    os.rename(path, path.replace('CSV', 'csv'))

def extract(start, end, type):
    date_range = pd.date_range(start, end, freq='15T').to_pydatetime()
    for date in date_range:
        filepath, filename = download_zip(date, type, ext_dict[type])
        unzip(filepath, filename)

def transform():
    pass

def load():
    pass

def main():
    start = dt.strptime(sys.argv[1], '%Y%m%d%H')
    end = dt.strptime(sys.argv[2], '%Y%m%d%H')
    type = sys.argv[3]
    
    extract(start, end, type)

if __name__=='__main__':
    main()