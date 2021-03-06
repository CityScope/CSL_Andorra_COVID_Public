# CSL_Andorra_COVID_Public
Open source code repo for MIT-Andorra collaboration on modelling mobility behaviour and COVID-19 spread.

## Paper citations

This code represents the analysis in the following papers.

Berke, A., Doorley, R., Alonso, L., Arroyo, V., Pons, M., & Larson, K. (2022) Using mobile phone data to estimate dynamic population changes and improve the understanding of a pandemic: A case study in Andorra. PLoS ONE 17(4): e0264860. https://doi.org/10.1371/journal.pone.0264860

Doorley, R., Berke, A., Noyman, A., Alonso, L., Ribó, J., Arroyo, V., Pons, M., & Larson, K. (2021). Mobility and COVID-19 in Andorra: Country-scale analysis of high-resolution mobility patterns and infection spread. IEEE Journal of Biomedical and Health Informatics, 26(1), 183-193. doi: 10.1109/JBHI.2021.3121165.



## Data and analysis processing

### Data
Individuals' data are private. Aggregate metrics are computed over individuals' data. Aggregated metrics are output in `/outputs/metrics/`.

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


#### Trips

See `/preprocessing/trips/`.

Basic daily trips metrics are precomputed from the stays data.

#### Presence, entrances, departures

See `/preprocessing/presence/`.

In  order  to  compute  daily  mobility  metrics  across  all  peoplepresent in the country, it was necessary to know which people were  present  on  any  day.  Some  devices  were  found  to  beunobserved  in  the  data  for  several  days,  even  during  the  fullgovernment  lockdown.  This  may  be  due  to  a  combinationof  inactivity,  lack  of  telecoms  reception  in  certain  areas, and/or  noisy  data.  It  was  assumed  that  gaps  in  data  of  two weeks or more represented true absence from the country. The beginnings  and  endings  of  periods  of  presence  were  counted as entrances to and departures from the country respectively. By  observing  the  distribution  of  total  number  of  dayspresent  across  all  subscribers,  it  was  found  that  the  majority of  subscribers  were  present  fewer  than  50  days.  The  restof  the  subscribers  were  present  for  a  significantly  greaternumber of days. 50 days of presence in Andorra was thereforeconsidered  as  an  appropriate  cutoff  for  identifying  tourists.Further analysis uses this categorization of tourists versus non-tourists.


#### Home inference

See `/preprocessing/homes/`.

For each user in the stays data, we infer a home parish based  on where  the user stayed the most in the nighttime hours over a given period of data. Homes are inferred for each homth of data. 

To evaluate the inferences of home parishes, this data is compared to official population statistics from 2020. https://www.estadistica.ad/serveiestudis/noticies/noticia5059cat.pdf

The Pearson correlation is 0.958.

## Metrics

Metrics are computed over the preprocessed stays data and output in /outputs/metrics.

## Analysis

Analysis is over the computed metrics. Notebooks are in /analysis/.
