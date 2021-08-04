from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder.appName("Get_Stays New").getOrCreate()
sc = spark.sparkContext

import datetime
import json
import math


def get_haversine_distance(point_1, point_2):
    """
    Calculate the distance between any 2 points on earth given as [lon, lat]
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(math.radians, [point_1[0], point_1[1], 
                                                point_2[0], point_2[1]])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a)) 
    r = 6371000 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

def get_stay_points(line):
    """
    takes a line of an rdd containing all the RNC observations for one person
    Returns a line containing all the stay-points for that person 
    """
    imsi=line[0]
    ts, lat, lon, mcc, is_4G, cellid=line[1]
    ts_unix=[(ts[i] - datetime.datetime(1970,1,1)).total_seconds()%(24*60*60) for i in range(len(ts))]

    # drop observations of tower 14
    drop_ind=[i for i in range(len(cellid)) if cellid[i]=='14']
    ts_unix=[ts_unix[i] for i in range(len(ts_unix)) if i not in drop_ind]
    lat=[lat[i] for i in range(len(lat)) if i not in drop_ind]
    lon=[lon[i] for i in range(len(lon)) if i not in drop_ind]
    is_4G=[is_4G[i] for i in range(len(is_4G)) if i not in drop_ind]
    # indooroutdoor=[indooroutdoor[i] for i in range(len(indooroutdoor)) if i not in drop_ind]
    # cellid=[cellid[i] for i in range(len(cellid)) if i not in drop_ind]

    # sort the series' based on the timestamp
    lat=[x_2 for (x_1, x_2) in sorted(zip(ts_unix,lat))]
    lon=[x_2 for (x_1, x_2) in sorted(zip(ts_unix,lon))]
    is_4G=[x_2 for (x_1, x_2) in sorted(zip(ts_unix,is_4G))]
    # indooroutdoor=[x_2 for (x_1, x_2) in sorted(zip(ts_unix,indooroutdoor))]
    # cellid=[x_2 for (x_1, x_2) in sorted(zip(ts_unix,cellid))]
    #then sort the timestamp itself
    ts_unix.sort()

    stay_points=[]
    i=0
    while i<len(lon):
        j=i
        while ((j+1)<len(lon) and 
                (get_haversine_distance([lon[j+1],lat[j+1]], 
                                [lon[i],lat[i]])<MAX_ROAM)):
            j+=1
        end_index=min(j+1,len(lon)-1)
        # departure time is the timestamp of the next obs after this cluster.
        # Unless this cluster contains the last point in the series
        if (ts_unix[end_index]-ts_unix[i])>MIN_STAY:
            stay_points.extend([{'p':[lon[int((i+j)/2)],lat[int((i+j)/2)]], 
                                 's':ts_unix[i],
                                 'l':ts_unix[j],
                                 'e':ts_unix[end_index],
                                 'n':(j-i+1),
                                 'n_4G': sum(is_4G[i:j+1])}])
                 # 'io': dict(Counter(indooroutdoor[i:j+1])),
                 # 'cellid': dict(Counter(cellid[i:j+1]))
                 # }])       
        i=j+1
    return {'mcc': mcc, 'stay_points': stay_points, 'imsi': imsi}
        
year=2020
dates=[{'month': 3, 'day': d} for d in range(2, 32)] + [{'month': 4, 'day': d} for d in range(1, 31)] + [{'month': 5, 'day': d} for d in range(1, 32)]

hour='*'
# =============================================================================
# Constants
# =============================================================================

MAX_ROAM=200
MIN_STAY=10*60

# =============================================================================
# Get the RNC data for the period
# =============================================================================
for date in dates:
    month=date['month']
    day=date['day']
    print('RNC data')
    rnc_4G_Df=spark.read.parquet('../../data/andorratelecom/GDR/4G/year={}/month={}/day={}/hour={}/*.snappy.parquet'.format(year, month, day, hour))
    rnc_3G_Df=spark.read.parquet('../../data/andorratelecom/GDR/3G/year={}/month={}/day={}/hour={}/*.snappy.parquet'.format(year, month, day, hour))

    rnc_3G_Df=rnc_3G_Df.withColumn('4G', lit(0))
    rnc_4G_Df=rnc_4G_Df.withColumn('4G', lit(1))

    print('Union')
    rnc_Df=rnc_4G_Df.union(rnc_3G_Df)

    print('Transform')
    rncRdd=rnc_Df.rdd
    byIMSE = rncRdd.map(lambda x: (x['imsi'], 
                                    [[x['timestamp']],[x['lat']],
                                     [x['lon']], x['mcc'], [x['4G']], 
                                     # [str(x['indooroutdoor'])], 
                                     [str(x['cellid'])]])).reduceByKey(
        lambda a, b: [a[0] + b[0],
                      a[1] + b[1],
                      a[2] + b[2],
                      a[3],
                      a[4] + b[4],
		      a[5] + b[5]])
		      # a[6] + b[6]])
        
    #byIMSE_part = byIMSE.sample(False, 0.1)
    byIMSE_part=byIMSE


    broadcastConstants = sc.broadcast({
                                'MAX_ROAM':MAX_ROAM,
                                'MIN_STAY':MIN_STAY
                                })
    print('Stay points')    
    persons=byIMSE_part.map(get_stay_points)
    persons.saveAsTextFile('stays/stays2_{}_{}_{}'.format(year, month, day))



