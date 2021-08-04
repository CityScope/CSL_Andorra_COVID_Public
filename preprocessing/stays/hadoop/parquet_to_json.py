import subprocess
import sys
from toolbox import *
import json

def copy_stays_to_local(year, month, day):
    command=['hdfs', 'dfs', '-copyToLocal', 'stays_{}_{}_{}'.format(year, month, day),
             './']
    subprocess.run(command)

if __name__ == '__main__':
    month=int(sys.argv[1])
    days_from=int(sys.argv[2])
    days_to=int(sys.argv[3])
    year=2020
    subprocess.run(['mkdir', 'stays/{}_{}'.format(year, month)])
    for day in range(days_from, days_to+1):
        print('Copying data for {}/{}'.format(month, day))
        copy_stays_to_local(year, month, day)
        print('Creating json for {}/{}'.format(month, day))
        folder='stays_{}_{}_{}'.format(year, month, day)
        file_paths=get_stay_file_paths(folder)
        persons=get_person_data_all_paths(folder, file_paths)
        json.dump(persons, open('stays/{}_{}/stays_{}_{}_{}.json'.format(year, month, year, month, day), 'w'))
        print('Removing local parquet files for {}/{}'.format(month, day))
        subprocess.run(['rm', '-r','stays_{}_{}_{}'.format(year, month, day)])