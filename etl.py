import sys, wget, os
import glob
from pathlib import Path
import pandas as pd
from datetime import datetime as dt
from zipfile import ZipFile
import re

ext_dict = {'export': 'CSV',
            'mentions': 'CSV',
            'gkg': 'csv'}

def download_zip(date, type, extension, dir_data, trans):
    str_date = date.strftime("%Y%m%d%H%M%S")
    if trans==True:

        url = f'http://data.gdeltproject.org/gdeltv2/{str_date}.translation.{type}.{extension}.zip'
        filename = f'{str_date}.translation.{type}.{extension}.zip'
    else:
        url = f'http://data.gdeltproject.org/gdeltv2/{str_date}.{type}.{extension}.zip'
        filename = f'{str_date}.{type}.{extension}.zip'
    filepath = f'{dir_data}/{filename}'
    try:
        if not os.path.exists(filepath):
            wget.download(url, filepath)
    except Exception as e:
        print(f'Error for {filename}')
        print(e)
    return filepath, filename

def unzip(filepath, filename, dir_data):
    # loading the temp.zip and creating a zip object
    with ZipFile(filepath, 'r') as zObject:
        # Extracting all the members of the zip 
        # into a specific location.
        path = zObject.extract(member=filename.replace('.zip', ''), path=dir_data)
    if os.path.exists(filepath):
        os.remove(filepath)
    os.rename(path, path.replace('CSV', 'csv'))

def extract(start, end, type, dir_data, trans):
    date_range = pd.date_range(start, end, freq='15T').to_pydatetime()
    for date in date_range:
        filepath, filename = download_zip(date, type, ext_dict[type], dir_data, trans)
        unzip(filepath, filename, dir_data)

def transform(dir_data, start, end, type, trans):
    path = Path(dir_data)
    print(trans)
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
        df.to_csv(dir_data + f'/dataset_{type}_from_{start}_to_{end}.csv')


def load():
    pass

def main():
    start = dt.strptime(sys.argv[1], '%Y%m%d%H')
    end = dt.strptime(sys.argv[2], '%Y%m%d%H')
    type = sys.argv[3]
    if len(sys.argv) == 5:
        trans = True
    else:
        trans = False
    print(trans)
    dir_data = '/Users/dorianfitton/Documents/Cours_Télécom/NoSQL_project_GDELT.nosync/data/raw'

    extract(start, end, type, dir_data, trans)
    transform(dir_data, start, end, type, trans)

if __name__=='__main__':
    main()