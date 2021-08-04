# Home Parish Inference Data

## Concept
For each user in the stays data, we infer home parish and save a table.  
Home parish is inferred based  on where  the user stayed the most in the nighttime hours over a given period of data.

## Purpose
The saved table can be merged with the mobility data in order to compute mobility metrics for parishes based on people’s homes. (e.g. XX users from Parish A did YY).
Computing the table once with a defined criteria and saving it as a shared  resource will make our different analyses more consistent and more  academically robust, and save us time.

The saved  table has columns

##### imsi,  mcc, parish, days, nights, datafiles

- parish: The inferred home parish  for the imsi/user.
days: The function that  computes the data takes data from a given time period. Days is the  number of  days in  this time  period where the user reported stays data.
- nights: The number of nights the user  reported data, i.e. number  of nights  used to  infer the home parish.

Together, days and nights can help us circumvent  the  issues of precomputing a datapanel and ignore tourists, but letting different analyses choose which users  to consider given the amount  to which they had a presence in the data.

- datafiles:
The number of files available when computing days  and nights. This varies across months. It is consistent across users per month.

##### Time period
Users may enter and  leave the  dataset  and change  their  primary locations over time.
Homes are computed monthly.


We take a month of data at  a  time, infer the home parish for that period, create the table  and  save it.
Data is  saved  to /homes/yyyy_mm_homes.csv
e.g. save to
/homes/2020_3_homes.csv
