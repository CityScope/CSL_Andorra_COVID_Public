"""
Infer homes
-------------
Infers a home parish for  each  user in the stays data and saves a table.  
Home parish is inferred based  on where  the user stayed the most in the 
nighttime hours over a given period of data. 
The criteria for inferring home parish is described below.


Usage:
python python/infer_homes.py --month=yyyy-mm \
    --month MONTH is the Year-Month for which to infer homes.
    [--stays_path STAYS_PATH] \
    [--homes_path HOMES_PATH]
    
optional arguments
    --stays_path=STRING is a path  to the stays data used for home inference.
    --homes_path=STRING is a path to where output inferred homes data is saved.
    
Example usage:
python python/infer_homes.py \
    --month=2020-3 \
    --stays_path=/home/data_commons/andorra_data_2020/stays/ \
    --homes_path=/home/data_commons/andorra_data_2020/homes/ \
    > infer_homes_2020_03.out


Data is  saved  to homes_path/yyyy_m_homes.csv

The saved  table has columns:
--------
imsi, mcc, parish, days, nights, datafiles
--------

parish: The inferred home parish  for the imsi/user.

days: The function that  computes the data takes data from a given time period. 
Days is the  number of  days in  this time  period where the user reported stays data.

nights: The number of nights the user  reported data and  that were used to infer
the home parish.

datafiles:
The number of files available when computing days  and nights. This varies across months. 
It is consistent across users per month.

"""
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


date_fmt = '%Y-%m'


IMSI = 'imsi'
MCC = 'mcc'
START = 's'
END = 'e'

PARISH = 'parish'
DAYS = 'days'
NIGHTS = 'nights'
DATAFILES = 'datafiles'


# nighttime start: 12am; nighttime end: 6am
# data 's','e' are seconds since 12am
S_12AM = 0
S_6AM = 60*60*6

S_NIGHT_STAY_DURATION = 'night stay duration'


default_homes_path = '/home/data_commons/andorra_data_2020/homes/'
default_stays_path = '/home/data_commons/andorra_data_2020/stays/'


def get_homes_filepath(datapath, year, month):
    return '{}{}_{}_homes.csv'.format(datapath, year, month)

def get_stays_filepath(datapath, year, month, day):
    return '{}{}_{}/stays_{}_{}_{}.csv'.format(datapath, year, month, year, month, day)


def get_stays_filepaths(year, month, stays_path):
    stays_fpaths = []
    d = date(year, month, 1)
    while d.month == month:
        stays_fpaths += [get_stays_filepath(stays_path, d.year, d.month, d.day)]
        d += timedelta(days=1)
    return stays_fpaths


def infer_homes(year, month, homes_path, stays_path):
    print('getting stays filepaths for %s/%s' % (year, month))
    stays_fpaths = get_stays_filepaths(year, month, stays_path)
    missing_stays_fpaths = [fp for fp in stays_fpaths if not Path(fp).is_file()]
    filtered_stays_fpaths = [fp for fp in stays_fpaths if Path(fp).is_file()]
    print('missing %s/%s stays files: %s' % (len(missing_stays_fpaths),
                                             len(stays_fpaths), missing_stays_fpaths))
    if len(filtered_stays_fpaths) < 1:
        print('no stay files to  process')
        return None
    print('handling %s stays files' % len(filtered_stays_fpaths))
    inferred_homes_df = process_stays_data(filtered_stays_fpaths)
    # save homes data
    homes_fpath = get_homes_filepath(homes_path, year, month)
    print('saving inferred homes data to %s' % homes_fpath)
    inferred_homes_df.reset_index().to_csv(homes_fpath, index=False)
    print('saved %s' % homes_fpath)
    return inferred_homes_df


def process_stays_data(stays_fpaths):
    # for each imsi, accumulate record of mcc, and of tally days, nights, 
    # and cumulative stay duration in each pairsh
    imsi_mcc = None
    imsi_days = None
    imsi_nights = None
    imsi_nights_parish_cum_duration = None # indexed by imsi, parish

    for i, fpath in enumerate(stays_fpaths):
        print('(%s/%s) %s handling %s' % (i+1, len(stays_fpaths), datetime.now(), fpath))
        stays_df = pd.read_csv(fpath)
        print('%s stays'  % len(stays_df))
        # compute duration of each nighttime stay
        stays_df[S_NIGHT_STAY_DURATION] = stays_df.apply(night_stay_duration, axis=1)
        # assert data integrity
        assert stays_df[S_NIGHT_STAY_DURATION].apply(lambda nsd: nsd <= S_6AM).all()
        nights_stays_df = stays_df[stays_df[S_NIGHT_STAY_DURATION] > 0]
        # make a series of imsis from each day  --> will later count days with value counts
        # make a series of imsis from each night  --> will later count days with value counts
        # either make new or combine with previous
        if imsi_mcc is None:
            imsi_mcc = stays_df[[IMSI,MCC]].drop_duplicates(subset=IMSI, keep='first')
            imsi_days = pd.Series(stays_df[IMSI].unique())
            imsi_nights = pd.Series(nights_stays_df[IMSI].unique())
        else:
            imsi_days = imsi_days.append(pd.Series(stays_df[IMSI].unique()))
            imsi_nights = imsi_nights.append(pd.Series(nights_stays_df[IMSI].unique()))
            imsi_mcc = imsi_mcc.append(stays_df[[IMSI,MCC]]).drop_duplicates(
                subset=IMSI, keep='first')

        #  accumulate the aggregate night stay time for  each imsi and parish
        imsi_nsp_cum_duration = nights_stays_df.groupby(
            [IMSI, PARISH])[S_NIGHT_STAY_DURATION].sum()
        if imsi_nights_parish_cum_duration is None:
            imsi_nights_parish_cum_duration = imsi_nsp_cum_duration
        else:
            imsi_nights_parish_cum_duration = imsi_nights_parish_cum_duration.add(
                imsi_nsp_cum_duration, fill_value=0)

    # map imsi to parish with  the greatest cumulative night stay duration
    # get the index for the total max stay by parish for each imsi
    imsi_max_night_stay_idx = imsi_nights_parish_cum_duration.reset_index().groupby(
        [IMSI]).idxmax()[S_NIGHT_STAY_DURATION].values
    # select rows by index and make a dataframe indexed by imsi
    inferred_home_parish_df = imsi_nights_parish_cum_duration.iloc[imsi_max_night_stay_idx].reset_index().set_index(IMSI)[[PARISH]]
    # add in the number  of days and nights of data observed for each IMSI   
    inferred_home_parish_df['days'] = imsi_days.value_counts()
    inferred_home_parish_df['nights'] = imsi_nights.value_counts()
    inferred_home_parish_df[MCC] = imsi_mcc.set_index(IMSI)
    inferred_home_parish_df['datafiles'] = len(stays_fpaths)
    
    return inferred_home_parish_df


def night_stay_duration(row, night_start=S_12AM, night_end=S_6AM):
    stay_start, stay_end = row[START], row[END]
    if stay_start > night_end:
        return 0
    return min(stay_end, night_end) - max(stay_start, night_start)



if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Infers a home parish for each user in the stays data and saves a table.')
    parser.add_argument('--month', required=True,
                        help='yyyy-mm the Year/Month for which to infer homes.')
    parser.add_argument('--stays_path', default=default_stays_path,
                        help='/path/to/stays/data/')
    parser.add_argument('--homes_path', default=default_homes_path,
                        help='/path/to/save/homes/data/')
    args = parser.parse_args()
    
    month_datetime = datetime.strptime(args.month, date_fmt)
    infer_homes(month_datetime.year, month_datetime.month, args.homes_path, args.stays_path)

