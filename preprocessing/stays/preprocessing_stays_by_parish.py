"""
This is a preprocessing script that transforms the JSON stays data into 
tables of stays with a parish for each stay.

Output files have columns:
imsi, mcc, s, e, n, n_4G, lat, lon, parish

Files are processed for the given dates.

Usage:
python python/preprocessing_stays_by_parish.py \
    --start_date=yyyy-mm-dd \
    --end_date=yyyy-mm-dd \
    --stays_datapath=/path/to/data/ \
    --shapefilepath=/path/to/data/shapefile.shp

Example usage:
python python/preprocessing_stays_by_parish.py --start_date=2020-03-01 --end_date=2020-04-18 \
    --stays_datapath=/home/data_commons/andorra_data_2020/stays/ \
    --shapefilepath=/home/data_commons/andorra_data_2020/datafiles/shapefiles/andorra_parish.shp \
    > preprocessing_stays_by_parish_2020-03-01_2020-04-18.out


The input files are saved in filepaths named  by 
    /YYYY_MM/stays_YYYY_MM_DD.json
The output  files  are saved to 
    /YYYY_MM/stays_YYYY_MM_DD.csv

"""


import datetime
import json
from pathlib import Path

import numpy as np
import pandas as pd

import geopandas as gpd


default_stays_datapath = '/home/data_commons/andorra_data_2020/stays/'
default_andorra_parish_shps_filepath = '/home/data_commons/andorra_data_2020/datafiles/shapefiles/andorra_parish.shp'

date_fmt = '%Y-%m-%d'

IMSI = 'imsi'
MCC = 'mcc'
START = 's'
END = 'e'
N = 'n'
N4G =  'n_4G'
LAT = 'lat'
LON = 'lon'

PARISH_NAME = 'parish'
GEOMETRY = 'geometry'

CRS ='epsg:4269'


def get_stays_day_filepath(datapath, day, month, year=2020):
    return '{}{}_{}/stays_{}_{}_{}.json'.format(datapath, year, month, year, month, day)

def get_stays_by_parish_filepath(datapath, day, month, year=2020):
    return '{}{}_{}/stays_{}_{}_{}.csv'.format(datapath, year, month, year, month, day)


def daterange(start_datetime, end_datetime):
    for n in range(int((end_datetime - start_datetime).days) + 1):
        yield start_datetime + datetime.timedelta(n)


def get_stays_df(json_persons_data):
    """
    Returns dataframe with stays.
    Columns: imsi, mcc, s, e, n, n_4G, lat, lon
    """
    stays_df = None
    for i, p_data in enumerate(json_persons_data):
        if i % 10000 == 0:
            print('processing imsi %s/%s : %s' % (i, len(json_persons_data),datetime.datetime.now()))
        stay_points = p_data['stay_points']
        if not stay_points:
            continue
        p_stays_df = pd.DataFrame.from_records(stay_points)
        p_stays_df[LON] = p_stays_df['p'].apply(lambda p: p[0])
        p_stays_df[LAT] = p_stays_df['p'].apply(lambda p: p[1])
        p_stays_df.drop('p', axis=1, inplace=True)
        p_stays_df[IMSI] = p_data[IMSI]
        p_stays_df[MCC] = p_data[MCC]

        if stays_df is None:
            stays_df = p_stays_df
        else:
            stays_df = stays_df.append(p_stays_df)
            
    return stays_df
        

def get_stays_by_parish_df(json_persons_data, parish_shps):
    """
    Returns dataframe with stays and parish data.
    Columns: imsi, mcc, s, e, n, n_4G, lat, lon, parish
    """
    stays_df = get_stays_df(json_persons_data)
    # join with parishes geodata
    gdfp = gpd.GeoDataFrame(stays_df,
                            geometry=gpd.points_from_xy(stays_df.lon, stays_df.lat))
    gdfp.crs = {'init':CRS}
    stays_by_parish_df = gpd.sjoin(gdfp, parish_shps, how='left')
    stays_by_parish_df = stays_by_parish_df.drop(columns=[GEOMETRY,'index_right'])
    return stays_by_parish_df.set_index(IMSI)


def process_date_files(dates, stays_datapath, shapefilepath):
    # Read in the Andorra parish shapefile
    andorra_parish_shps = gpd.read_file(shapefilepath)
    andorra_parish_shps = andorra_parish_shps.to_crs({'init':CRS})
    andorra_parish_shps.rename(columns={'NAME_1':PARISH_NAME}, inplace=True)
    andorra_parish_shps = andorra_parish_shps[[PARISH_NAME, GEOMETRY]]
    assert(len(andorra_parish_shps) == 7) # There  are  7 parishes
    
    for i, d in enumerate(dates):
        date_str =  d.strftime("%Y-%m-%d")
        print('%s/%s: processing %s -- %s' % (i+1, len(dates), date_str, datetime.datetime.now()))
        
        stays_json_filepath = get_stays_day_filepath(stays_datapath, d.day, d.month, d.year)
        stays_by_parish_filepath = get_stays_by_parish_filepath(stays_datapath, d.day, d.month, d.year)

        if not Path(stays_json_filepath).is_file():
            print('skipping %s -- file not found: %s' % (date_str, stays_json_filepath))
            continue
        stays_json_data = json.load(open(stays_json_filepath))
        stays_by_parish_df = get_stays_by_parish_df(stays_json_data, andorra_parish_shps)
        print('%s/%s: saving data for %s to %s' % (i+1, len(dates), date_str, stays_by_parish_filepath))
        stays_by_parish_df.to_csv(stays_by_parish_filepath)
        

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Preprocess JSON stays files to produce CSV tables of stays with parishes.')
    parser.add_argument('--start_date', required=True,
                        help='yyyy-mm-dd start date for files to process')
    parser.add_argument('--end_date', required=True,
                        help='yyyy-mm-dd end date for files to process')
    parser.add_argument('--stays_datapath', default=default_stays_datapath,
                        help='/path/to/data/')
    parser.add_argument('--shapefilepath', default=default_andorra_parish_shps_filepath,
                        help='/path/to/data/')
    args = parser.parse_args()
    
    start_datetime = datetime.datetime.strptime(args.start_date, date_fmt)
    end_datetime  = datetime.datetime.strptime(args.end_date, date_fmt)
    
    process_dates = [d for d in daterange(start_datetime, end_datetime)]
    process_date_files(process_dates, args.stays_datapath, args.shapefilepath)
    
    