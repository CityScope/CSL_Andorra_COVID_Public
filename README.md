# CSL_Andorra_COVID_Public
Open source code repo for MIT-Andorra collaboration on modelling mobility behaviour and COVID-19 spread.

## Data and analysis processing

### Data
Individuals' data are private. Aggregate metrics are computed over individuals' data. Aggregated metrics are output in `/data/public/metrics/`.

### Preprocessing

See /preprocessing/

#### Stay points

/preprocessing/stays/

Raw CDRs data is on a Hadoop server and are processed as 'stay' points for individuals.
The stay points files are produced for each day using the get_stays.py script which runs on the Hadoop server. This produces a list of person objects. Each person object contains
- a unique person id (IMSI)
- a country code (MCC)
- a list of stay_points objects

Files from hadoop are saved as JSON files. There is one file for each day of data.

This code is in `/preprocessing/stays/hadoop/`.

Files are further processed 
- to transform them to tables to save as .csv files
- to attach the parish that contains the stay
This code is in `/preprocessing/stays/`.


#### Presence, entrances, departures

See `/preprocessing/presence`.

In  order  to  compute  daily  mobility  metrics  across  all  peoplepresent in the country, it was necessary to know which people were  present  on  any  day.  Some  devices  were  found  to  beunobserved  in  the  data  for  several  days,  even  during  the  fullgovernment  lockdown.  This  may  be  due  to  a  combinationof  inactivity,  lack  of  telecoms  reception  in  certain  areas, and/or  noisy  data.  It  was  assumed  that  gaps  in  data  of  two weeks or more represented true absence from the country. The beginnings  and  endings  of  periods  of  presence  were  counted as entrances to and departures from the country respectively. By  observing  the  distribution  of  total  number  of  dayspresent  across  all  subscribers,  it  was  found  that  the  majority of  subscribers  were  present  fewer  than  50  days.  The  restof  the  subscribers  were  present  for  a  significantly  greaternumber of days. 50 days of presence in Andorra was thereforeconsidered  as  an  appropriate  cutoff  for  identifying  tourists.Further analysis uses this categorization of tourists versus non-tourists.


#### Home inference

See `/preprocessing/homes/`.

For each user in the stays data, we infer a home parish based  on where  the user stayed the most in the nighttime hours over a given period of data. Homes are inferred for each homth of data. 

To evaluate the inferences of home parishes, this data is compared to official population statistics from 2020. https://www.estadistica.ad/serveiestudis/noticies/noticia5059cat.pdf

The Pearson correlation is 0.958.

## Metrics

Metrics are computed over the preprocessed stays data and output in /data/public/metrics.

## Analysis

Analysis is over the computed metrics. Notebooks are in /analysis/.
