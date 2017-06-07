"""
Adds new document to the database
"""

import sys
import json
import pymongo
from datetime import datetime
import copy
from munkres import Munkres, make_cost_matrix
from sklearn.preprocessing import LabelEncoder
from bson.objectid import ObjectId


def cluster_assignment(id2label_1, id2label_2):
    """
    Assignes cluster names from first clusterisation to cluster names of second clusterisation
    using Hungarian algorithm of the assignment problem (maximising the similarity between clusters
    from different clusterisations)
    Labels could have different lengths and contain different types of objects

    Arguments:
        id2label_1: dict of id to label from first clusterisation
        id2label_2: dict of id to label from second clusterisation

    Returns:
        dict: how to transform first labels to second
    """
    
    # Get intersection of two dicts
    ids = []
    labels_1, labels_2 = [], []
    for news_id, label_1 in id2label_1.items():
        if news_id in id2label_2:
            ids.append(news_id)
            labels_1.append(label_1)
            labels_2.append(id2label_2[news_id])

    n_clusters_1 = len(set(labels_1))
    n_clusters_2 = len(set(labels_2))
    
    # Encode labels so they would be from 0 to max
    l_encoder_1 = LabelEncoder().fit(labels_1)
    labels_1_transf = l_encoder_1.transform(labels_1)
    l_encoder_2 = LabelEncoder().fit(labels_2)
    labels_2_transf = l_encoder_2.transform(labels_2)
    
    # Create matrix of distances between two clusters
    matrix = [[0]*n_clusters_2 for i in range(n_clusters_1)]
    for i in range(min(len(labels_1), len(labels_2))):
        matrix[labels_1_transf[i]][labels_2_transf[i]] += 1
    
    # Compute Munkres (Hungarian) algorithm
    cost_matrix = make_cost_matrix(matrix, lambda cost: sys.maxsize - cost)
    m = Munkres()
    indices = m.compute(cost_matrix)
    
    # Transform labels back
    transform = {}
    for (label_1, label_2) in indices:
        transform[l_encoder_1.inverse_transform(label_1)] = l_encoder_2.inverse_transform(label_2)
    
    return transform


def add_events_evolution(db_name, clustering_name, events_evolution_name):
    """
    Adds new document to the database (db_name) collecton (events_evolution_name)

    Arguments:
        db_name: string
        clustering_name: collection name with clustering results
        events_evolution_name: collection name where to write this evolution

    Returns:
        None
    """

    client = pymongo.MongoClient()
    
    # Get two last clusterisations data and results
    clustering = client[db_name][clustering_name].find_one(sort=[('date', pymongo.DESCENDING)])
    prev_clustering = client[db_name][clustering_name].find_one({'date': {'$lt': clustering['date']}},
                                                                sort=[('date', pymongo.DESCENDING)])
    # Get news (for time they were published)
    news = client[db_name].news
    
    # Get previous events_evolution data and results
    prev_events_evolution = client[db_name][events_evolution_name].find_one(sort=[('date', pymongo.DESCENDING)])

    # If it is the fisrt time we want to build event_evolution
    if prev_events_evolution == None:
        events_evolution = {'date': datetime.timestamp(datetime.now()), 'labels2id': {}}
        for label, ids in clustering['labels2id'].items():
            events_evolution['labels2id'][label] = [ids.copy()]
        
        # Add new data to the collection
        client[db_name][events_evolution_name].insert_one(events_evolution)

        print('Done')
        return
    events_evolution = copy.deepcopy(prev_events_evolution)
    
    print('Start matching two clusterings...')
    transform = cluster_assignment(prev_clustering['id2labels'], clustering['id2labels'])
    print('Matching finished')

    # Iterate over labels in previous event_evolution
    print('Looking for new evolutions...')
    count_evolutions = 0
    for prev_label, prev_ids in prev_events_evolution['labels2id'].items():
        if int(prev_label) in transform:
            label = str(transform[int(prev_label)]) # label in a new clusterisation

            # Iterate over ids in a cluster in a new clusterisation
            evolutions = []
            for news_id in clustering['labels2id'][label]:
                # Find news ids that happened after previous clusterisation
                if datetime.timestamp(news.find_one({'_id': ObjectId(news_id)})['date']) > prev_clustering['date']:
                    evolutions.append(news_id)

            # Add this list of new news that are evolution of some event
            if evolutions:
                events_evolution['labels2id'][prev_label].append(evolutions)
                count_evolutions += 1

    print('Found {} events evolutions'.format(count_evolutions))
    
    # Rename all cluster labels in accordance to new clusterisation
    new_label2id = {}
    for prev_label, ids in events_evolution['labels2id'].items():
        if int(prev_label) in transform:
            new_label2id[str(transform[int(prev_label)])] = ids

    # Add new news which are not an evolution of eny event
    for label, ids in clustering['labels2id'].items():
        if label not in new_label2id:
            new_label2id[label] = [ids]

    # Add old events evolution which have no new news in current clusterisation
    for prev_label, ids in events_evolution['labels2id'].items():
        if int(prev_label) not in transform:
            new_label = str(max(map(int, new_label2id.keys())) + 1)
            new_label2id[new_label] = ids

    # Add everything to new 'events_evolution'
    events_evolution['labels2id'] = new_label2id

    # Change date
    events_evolution['date'] = datetime.timestamp(datetime.now())

    # Delete old id
    events_evolution.pop('_id')
    
    # Add new data to the collection
    client[db_name][events_evolution_name].insert_one(events_evolution)
    print('Done')


def main(path_to_params):
    js = open(path_to_params) 
    data = json.load(js)
    add_events_evolution(data[0].get('db_name'), 'clustering_dup', 'events_evolution_on_duplicates')
    

if __name__ == "__main__":
    main(sys.argv[1:][0])
