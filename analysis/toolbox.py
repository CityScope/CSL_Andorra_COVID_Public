import math
import os
import json
import pyproj
import collections
import datetime


def create_intervals(interval_length_minutes=30):
    T=interval_length_minutes*60
    intervals=[[i*T, (i+1)*T] for i in range(int((24*60)/interval_length_minutes))]
    return intervals, T

def project_stay_points(persons):
    for p in persons:
        for s in p['stay_points']:
            x, y=pyproj.transform(pyproj.Proj("+init=EPSG:4326"), pyproj.Proj("+init=EPSG:25831"), s['p'][0], s['p'][1])
            s['x'], s['y']=x, y

def point_distance(point_a, point_b):
    return math.sqrt((point_a[0]-point_b[0])**2+ (point_a[1]-point_b[1])**2)

def get_radius_gyration(trajectory):
    N=len(trajectory)
    if N==0:
        return 0
    center=[sum([t[0] for t in trajectory])/N, sum([t[1] for t in trajectory])/N]    
    return math.sqrt(sum([point_distance(t, center)**2 for t in trajectory])/N)

def get_all_gyration_radii(persons):
    radii=[]
    for person in persons:
        trajectory=[[s['x'], s['y']] for s in person['stay_points']]
        radius=get_radius_gyration(trajectory)
        radii.append(radius)
    return radii

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

def max_dist_from_group(ref_point, point_group):
    maxDist=0
    for p in point_group:
        distN=get_haversine_distance(p, ref_point)
        maxDist=max(distN,maxDist)
    return maxDist

def get_person_summaries(year, dates, DIR='../data/private/stays'):
    """
    For each person in data, get their MCC code and the first and last day they were observed
    """
    all_persons_summary={}
    for i_d, date in enumerate(dates):
        month=date['month']
        day=date['day']
    #     print('{}_{}'.format(month, day))
        try:
            persons=json.load(open('{}/{}_{}/stays_{}_{}_{}.json'.format(
                DIR, year, month, year, month, day)))
        except:
            print("Couldn't get data for {}_{}_{}".format(year, month, day))
            persons=[]
        for p in persons:
            if p['imsi'] not in all_persons_summary:
                all_persons_summary[p['imsi']]={'mcc': p['mcc'], 'first': i_d, 'last': i_d}
            else:
                all_persons_summary[p['imsi']]['last']=i_d
    return all_persons_summary
