# User Presence in Andorra

## Purpose
- To identify which users are observed on which days
- To infer which days users were actually present  (filling in small gaps in observed - presence of users)
- To compute new entrances and exits
- To infer which users are full-time residents vs temp workers vs tourists

## Inputs
Stay data csv files for each day

## Criteria
A user is observed on any day during which they have a valid (in Andorra) stay

Assume that small gaps in observations are days when the person was still present but their device was not observed. For larger gaps in observation, assume the person left.

eg. assuming a max window size of 3: if someone is observed on days 1,2,3,6,7,8,15,16.. 
we assume they were present on days 1,2,3,4,5,6,7,8,15,16
we also count a departure on day 9 and an entrance on day 15

When calculating the number of days the device is missing, don't count days which are entirely missing from the data.

eg. for the same example above, if days 9,10,11,12 are missing from the data, then the device is assumed to be present during the entire period (because only days 13 and 14 are counted as missing for this user.


Window used: 13

## Output
presence.csv
- 1 row per imsi
- 1 column per date
- 0 indicates person was absent, 1 indicates present but no stays, 0s indicate present and has at least 1 stay

entrances_departures.csv
- 1 row per date
- A departures column and an entrances column for each nationality

## Script
/presence_entrances_departures.ipynb

