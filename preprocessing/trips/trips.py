"""
Trips
-------------
Computes daily number of total trips by mobile subscribers, based on precomputed stays.


Usage:
python trips.py \
    --start_date=yyyy-mm-dd \
    --end_date=yyyy-mm-dd \
    --data_filepath=PATH \
    --outputs_filepath=PATH
   
Example usage:
nohup python trips.py \
  --start_date=2020-01-01 \
  --end_date=2020-10-31 \
  --data_filepath=/home/data_commons/andorra_data_2020/  \
  --outputs_filepath=./outputs/metrics/ > nohup_trips_2020.out &



Saves aggregate daily tripscounts to 
outputs_filepath/YEAR/trips.csv:
-------------
date, users making trips, total trips, mean trips, median trips

"""
from datetime import datetime, timedelta
import pathlib

import numpy as np
import pandas as pd

IMSI = 'imsi'
DATE = 'date'

TOTAL_TRIPS = 'total trips'
USERS_MAKING_TRIPS = 'users making trips'
# per user metrics
TRIPS_MEAN = 'mean trips'
TRIPS_MEDIAN = 'median trips'

date_fmt = '%Y-%m-%d'


def get_stays_filepath(data_filepath, day, month, year):
    return '{}stays/{}_{}/stays_{}_{}_{}.csv'.format(data_filepath, year, month, year, month, day)

def daterange(start_datetime, end_datetime):
    for n in range(int((end_datetime - start_datetime).days) + 1):
        yield start_datetime + timedelta(n)

def get_trips_df(data_filepath, dates):
    df = None
    records = []
    missing_dates = []
    for i, d in enumerate(dates):
        stays_filepath = get_stays_filepath(data_filepath, d.day, d.month, d.year)
        date_str =  d.strftime("%Y-%m-%d")
        if i%20==0:
            print('%s/%s %s : %s' %  (i, len(dates), date_str, datetime.now()))
        if not pathlib.Path(stays_filepath).is_file():
            missing_dates += [d]
            print('%s\nfile not found: %s' % (date_str, stays_filepath))
            continue
        df = pd.read_csv(stays_filepath).dropna()
        trips = (df[IMSI].value_counts() - 1)
        records +=  [{
            DATE: d,
            USERS_MAKING_TRIPS: len(trips[trips>0]),
            TOTAL_TRIPS: trips.sum(),
            TRIPS_MEAN: trips.mean(),
            TRIPS_MEDIAN: trips.median(),
        }]
    trips_df = pd.DataFrame.from_records(records).set_index(DATE)
    return trips_df, missing_dates


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Computes likely dates of presence, entrances, departures \
                    for each user in the data, based on the dates they are \
                    observed in the data.')
    parser.add_argument('--start_date', required=True)
    parser.add_argument('--end_date', required=True)
    parser.add_argument('--data_filepath', required=True)
    parser.add_argument('--outputs_filepath', required=True)
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, date_fmt)
    end_date = datetime.strptime(args.end_date, date_fmt)
    datetimes = [d for d in daterange(start_date, end_date)]
    print('--- get trips ---')
    print('datetimes: %s - %s' % (datetimes[0], datetimes[-1]))
    year = start_date.year
    data_filepath, outputs_filepath = args.data_filepath, args.outputs_filepath
    trips_df, missing_dates = get_trips_df(data_filepath, datetimes)
    print('computed trips. %s/%s missing dates' % (len(missing_dates), len(datetimes)))
    trips_filepath = ('%s%s/trips.csv' % (outputs_filepath, year))
    print('saving trips data to %s' % trips_filepath)
    trips_df.to_csv(trips_filepath, index=True, index_label=DATE)
    print('saved')
    