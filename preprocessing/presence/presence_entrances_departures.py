"""
Presence, entrances, departures
-------------
Computes likely dates of presence, entrances, departures for each
user in the data, based on the dates they are observed in the data.


Usage:
python presence_entrances_departures.py \
    --start_date=yyyy-mm-dd \
    --end_date=yyyy-mm-dd \
    --data_filepath=PATH \
    --outputs_filepath=PATH
   
Example usage:
nohup python presence_entrances_departures.py \
   --start_date=2019-03-01 \
   --end_date=2019-06-30 \
   --data_filepath=/home/data_commons/andorra_data_2020/  \
   --outputs_filepath=./outputs/metrics/ > nohup_presence_2019.out &


Saves aggregate daily presence counts to 
outputs_filepath/YEAR/presence.csv:
-------------
date, all, non-tourists, tourists

Saves aggregate daily entrances and departures counts to 
outputs_filepath/YEAR/entrance_departure.csv:
-------------
date, entrance_{nationality}, ..., departure_{nationality}

"""
from datetime import datetime, timedelta
import pathlib

import numpy as np
import pandas as pd

IMSI = 'imsi'
MCC ='mcc'
DATE = 'date'

ENTRANCES = 'entrances'

date_fmt = '%Y-%m-%d'

WINDOW = 13

# MCC
ALL = 'All'
OTHER_MCC = 'other'
NON_ANDORRAN = 'Non-Andorran'
ANDORRAN_MCC = '213'
mcc_names_dict = {
    '213':'Andorran',
    '214':'Spanish',
    '208':'French',
    '234':'British',
    # otherwise nationality is 'other'
    OTHER_MCC: 'Other'
}


def get_stays_filepath(data_filepath, day, month, year):
    return '{}stays/{}_{}/stays_{}_{}_{}.csv'.format(data_filepath, year, month, year, month, day)

def daterange(start_datetime, end_datetime):
    for n in range(int((end_datetime - start_datetime).days) + 1):
        yield start_datetime + timedelta(n)


# ind_days_observed and ind_days_present are used 
# to denote the indices of the days the person is observed or assumed to be present. 
# Indices are used to simplify the computation by avoiding datetime operations.
def get_days_observed(data_filepath, datetimes):
    """
    returns a dict containing one entry for each imsi
    each person-object contains informtion about the user and when they were observed:
        'days': a set containing all the indices of the days they were present
        'months': a set containing all the months they were present
    """
    all_persons_summary = {}
    ind_missing_dates=[]
    print('get days observed')
    for i_date, date in enumerate(datetimes):
        stays_filepath = get_stays_filepath(data_filepath, date.day, date.month, date.year)
        date_str =  date.strftime("%Y-%m-%d")
        if i_date % 10 == 0:
            print('%s/%s %s : %s' %  (i_date, len(datetimes), date_str, datetime.now()))
        if not pathlib.Path(stays_filepath).is_file():
            ind_missing_dates += [i_date]
            print('%s\nfile not found: %s' % (date_str, stays_filepath))
            continue
        users = pd.read_csv(stays_filepath)[[IMSI,MCC, 'parish']].dropna().set_index(IMSI)
        users = users[~users.index.duplicated(keep='first')]
        users[MCC] = users[MCC].astype(str)
        for imsi, row in users.iterrows():
            if imsi not in all_persons_summary:
                all_persons_summary[imsi]={'mcc': row['mcc'], 'months':set([date.month]),'ind_days_observed':set([i_date])}
            else:
                all_persons_summary[imsi]['months'].add(date.month)
                all_persons_summary[imsi]['ind_days_observed'].add(i_date)
    return all_persons_summary, ind_missing_dates


def infer_days_present(ind_days_observed, window=WINDOW, ind_missing_dates=[]):
    """
    Infer which days each user was present based on the days they were observed.<br>
    Assume that small gaps in observations are days when the person was still present but their device was not observed.
    For larger gaps in observation, assume the person left <br>

    eg. assuming a max window size of 3: if someone is observed on days 
    1,2,3,6,7,8,15,16.. 
    we assume they were present on days 1,2,3,4,5,6,7,8,15,16
    we also count a departure on day 9 and an entrance on day 15

    When calculating the number of days the device is missing, 
    don't count days which are entirely missing from the data.
    eg. for the same example above, if days 9,10,11,12 are missing from the data, 
    then the device is assumed to be present during the entire period 
    (because only days 13 and 14 are counted as missing for this user.
    """
    ind_days_observed=sorted(list(ind_days_observed))
    ind_days_present=set()
    entrances=[]
    departures=[]
    # TODO: simplify 3 blocks below into one block by setting ind_days_present= [-1]+[ind_days_present] + [len(datetimes)+1]
    # deal with period between start of study period and first observed day for this device
    missing_from_start=sum([1 for d in range(ind_days_observed[0]) if d not in ind_missing_dates])
    if missing_from_start<=window:
        for j in range(ind_days_observed[0]):
            ind_days_present.add(j)
    else:
        entrances.append(ind_days_observed[0])
    # deal with period between the first and last observed day
    for i in range(len(ind_days_observed)-1):
        ind_days_present.add(ind_days_observed[i])
        n_missing_days=sum([1 for d in range(ind_days_observed[i], ind_days_observed[i+1]) if d not in ind_missing_dates])
        if n_missing_days<=window:
            for j in range(ind_days_observed[i], ind_days_observed[i+1]):
                ind_days_present.add(j) 
        else:
            departures.append(ind_days_observed[i])
            entrances.append(ind_days_observed[i+1])
    # deal with period between last observed day and end of study period
    ind_days_present.add(ind_days_observed[-1])
    missing_from_end=sum([1 for d in range(ind_days_observed[-1], len(datetimes)) if d not in ind_missing_dates])
    if missing_from_end<=window:
        for j in range(ind_days_observed[-1], len(datetimes)):
            ind_days_present.add(j)
    else:
        departures.append(ind_days_observed[-1])
       
    return ind_days_present, departures, entrances


def get_presence_entrances_departures_dfs(datetimes, all_persons_summary, ind_missing_dates, window=WINDOW):
    """
    returns presence_df, entrance_departure_df
    presence_df:
        - 1 row per imsi
        - 1 column per date
        - 0 indicates person was absent, 1 indicates present but no stays, 0s indicate present and has at least 1 stay
    """
    mcc_names=[mcc_names_dict[code]  for code in mcc_names_dict]
    columns=['entrance_{}'.format(name) for name in mcc_names]+['departures_{}'.format(name) for name in mcc_names]
    entrance_departure_df=pd.DataFrame(index=datetimes, columns=columns)
    entrance_departure_df.loc[:,:]=0
    presence_df=pd.DataFrame(index=[imsi for imsi in all_persons_summary], columns=range(len(datetimes)))
    presence_df.loc[:,:]=0
    for ind_imsi, imsi in enumerate(all_persons_summary):
        if ind_imsi%10000==0:
            print('computing presence for imsi %s/%s : %s' % (ind_imsi, len(all_persons_summary), datetime.now()))
        ind_days_observed=all_persons_summary[imsi]['ind_days_observed']
        mcc=all_persons_summary[imsi]['mcc']
        if mcc in mcc_names_dict:
            mcc_name=mcc_names_dict[mcc]
        else:
            mcc_name='Other'
        ind_days_present, departures, entrances = infer_days_present(ind_days_observed, window, ind_missing_dates)
        all_persons_summary[imsi]['entrances']=entrances
        all_persons_summary[imsi]['departures']=departures
        all_persons_summary[imsi]['ind_days_present']=ind_days_present

        # update the the total entrances and exits
        for ind_day in entrances:
            entrance_departure_df.loc[datetimes[ind_day], 'entrance_{}'.format(mcc_name)]+=1
        for ind_day in departures:
            entrance_departure_df.loc[datetimes[ind_day], 'departures_{}'.format(mcc_name)]+=1

        # update the overall dataframe of presence/absence
        presence_df.iloc[ind_imsi][ind_days_present]=1
        presence_df.iloc[ind_imsi][ind_days_observed]=2
    presence_df = presence_df.rename(columns={i:datetimes[i] for i in range(len(datetimes))}) 
    return presence_df, entrance_departure_df


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
    print('--- get presence, entrances, departures ---')
    print('datetimes: %s - %s' % (datetimes[0], datetimes[-1]))
    year = start_date.year
    data_filepath, outputs_filepath = args.data_filepath, args.outputs_filepath
    all_persons_summary, ind_missing_dates= get_days_observed(data_filepath, datetimes)
    print('computed all_persons_summary. %s missing dates' % ind_missing_dates)
    presence_df, entrance_departure_df = get_presence_entrances_departures_dfs(datetimes, all_persons_summary, ind_missing_dates)
    print('computed presence, entrance_departure dfs')
    # mark tourists
    # Tourists: observed on fewer than 50 days of the whole dataset
    # Residents (Ordinary or Temp): everyone else
    for imsi in all_persons_summary:
        if len(all_persons_summary[imsi]['ind_days_observed'])>=50:
            # long term stay: either a permanent resident or temp worker
            all_persons_summary[imsi]['status']='resident'
        else:
            all_persons_summary[imsi]['status']='tourist'
    # Save presence dataframe: one csv for tourists and one for others
    ind_tourist, ind_other=[], []
    for ind, imsi in enumerate(all_persons_summary):
        if all_persons_summary[imsi]['status']=='tourist':
            ind_tourist.append(ind)
        else:
            ind_other.append(ind)
    presence_df_tourists = presence_df.iloc[ind_tourist]
    presence_df_non_tourists = presence_df.iloc[ind_other]
    presence_tourists_filepath = ('%spresence/%s/presence_tourists.csv'%(
        data_filepath, year
    ))
    presence_others_filepath = ('%spresence/%s/presence_others.csv'%(
        data_filepath, year
    ))
    print('saving tourists presence data to %s' % presence_tourists_filepath)
    presence_df_tourists.to_csv(presence_tourists_filepath)
    print('saving others presence data to %s' % presence_others_filepath)
    presence_df_non_tourists.to_csv(presence_others_filepath)

    # make aggregate presence table and save in public outputs/metrics
    assert(
        len(presence_df_tourists.columns) == \
        len(presence_df_non_tourists.columns) == \
        len(datetimes)
    )
    tourists_present = []
    non_tourists_present = []
    all_present = []
    for i, d in enumerate(presence_df_non_tourists.columns):
        tourists_present += [(presence_df_tourists[str(d)] > 0).sum()]
        non_tourists_present += [(presence_df_non_tourists[str(d)] > 0).sum()]
        all_present += [tourists_present[-1] + non_tourists_present[-1]]
    aggregate_presence_df = pd.DataFrame(data={
        DATE:pd.to_datetime(presence_df_tourists.columns),
        'all': all_present,
        'tourists': tourists_present,
        'non-tourists': non_tourists_present,
    }).set_index(DATE)
    aggregate_presence_filepath = ('%s%s/presence.csv'%(
        outputs_filepath, year
    ))
    print('saving aggregate_presence data to %s' % aggregate_presence_filepath)
    aggregate_presence_df.to_csv(aggregate_presence_filepath, index=True, index_label=DATE)

    # save entrances and departures data to public outputs/metrics
    entrance_departure_filepath = ('%s%s/entrance_departure.csv'%(
        outputs_filepath, year
    ))
    print('saving entrance_departure data to %s' % entrance_departure_filepath)
    entrance_departure_df.to_csv(entrance_departure_filepath, index=True, index_label=DATE)
    print('saved')
    