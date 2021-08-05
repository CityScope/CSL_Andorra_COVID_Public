import h3
from collections import Counter
import datetime

def get_h3_cells_by_interval(resolution, intervals, T, persons):
    # iterate through all stays and get the interval and the h3 cell
    stay_ind_by_interval=[[] for i in range(len(intervals))]
    cell_ids_by_stay_ind=[]
    for person in persons:
        for stay in person['stay_points']:
            int_start=int(stay['s']/T)
            int_end=int(stay['e']/T)
            for int_id in range(int_start, int_end+1):
                stay_ind_by_interval[int_id].append(len(cell_ids_by_stay_ind))  
            cell_id=h3.geo_to_h3(stay['p'][1], stay['p'][0], resolution=resolution)
            cell_ids_by_stay_ind.append(cell_id)
    return stay_ind_by_interval, cell_ids_by_stay_ind

def count_interactions_kring_one_interval(cell_ids_this_interval, cell_radius):
    n_interactions_this_interval=0
    interactions_by_cell_this_interval=[]
    n_stays_by_cell_id=Counter(cell_ids_this_interval)
    for cell_id in n_stays_by_cell_id:
        n_interactions_this_cell=0
        n_stays_centre=n_stays_by_cell_id[cell_id]
        if n_stays_centre>0:
            n_interactions_this_cell+=n_stays_centre*(n_stays_centre-1)/2
            neighbours=h3.k_ring(cell_id, cell_radius)
            n_stays_neighbours=sum([n_stays_by_cell_id[id_n] for id_n in neighbours if not id_n == cell_id])
            n_interactions_this_cell+= 0.5* n_stays_centre*n_stays_neighbours
        n_interactions_this_interval+=n_interactions_this_cell
        interactions_by_cell_this_interval.append([cell_id,int(n_interactions_this_cell)])
    return n_interactions_this_interval, interactions_by_cell_this_interval

def count_interactions_kring_all_intervals(cell_radius, stay_ind_by_interval, cell_ids_by_stay_ind):
    then=datetime.datetime.now()
    n_interactions_all_intervals=[]
    interactions_by_interval_and_cell=[]
    for s_ind_this_interval in stay_ind_by_interval:
        cell_ids_this_interval=[cell_ids_by_stay_ind[s_ind] for s_ind in s_ind_this_interval]
        n_interactions_this_interval, interactions_by_cell_this_interval = count_interactions_kring_one_interval(cell_ids_this_interval, cell_radius)
        n_interactions_all_intervals.append(n_interactions_this_interval)
        interactions_by_interval_and_cell.append(interactions_by_cell_this_interval)
    print(datetime.datetime.now()-then)
    return n_interactions_all_intervals, interactions_by_interval_and_cell
