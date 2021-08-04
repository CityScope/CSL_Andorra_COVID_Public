# Stays points

The stay points files are produced for each day using the get_stays.py script which runs on the Hadoop server. This produces a list of person objects. Each person object contains
- a unique person id (IMSI)
- a country code (MCC)
- a list of stay_points objects

Each stay-point has attributes:
- p: position in lon, lat
- s: start time in seconds from midnight
- e: end time in seconds from midnight
- n: number of raw RNC observations comprising the stay_point
- n_4G: number of raw RNC observations from 4G towers comprising the stay_point

Files are further processed 
- to transform them to tables to save as .csv files
- to attach the parish that contains the stay

Output files have columns:
imsi, mcc, s, e, n, n_4G, lat, lon, parish

See preprocessing_stays_by_parish.py
