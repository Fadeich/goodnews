import json 
import sys
from clustering_duplicate import clustering_for_duplicates 
from doc2vec_kmeans import clustering_doc2vec
from get_texts import get_norm_texts_from_ids
from datetime import datetime, timedelta
from pymongo import MongoClient
import pandas as pd
from get_texts import get_norm_texts_without_and_with_facts


def save_clustering_doc2vec(db_name, labels_str, kmeans_score, exp_variance_score, k, idx, centers):
    client = MongoClient()
    clustering = client[db_name].clustering_doc2vec
    dict_scores = {'kmeans' : kmeans_score, 'exp_variance' : exp_variance_score}
    labels = list(map(lambda x: int(x), labels_str))
    labels_str = list(map(lambda x: str(x), labels))
    idx = list(map(lambda Id: str(Id), idx))
    df_labels = pd.DataFrame({'labels' : labels, 'id' : idx})
    Dict = {'n_clusters' : k,
            'id2labels' : dict(zip(idx, labels)),
            'labels2id' : dict([(str(label), list(df_labels[df_labels['labels'] == label]['id'])) for label in labels]),
            'scores' : dict_scores,
            'date' : datetime.timestamp(datetime.now()),
            'centers' :centers, 
	    'status': dict(zip(labels_str, [0 for i in range( len(labels) )] )),
    }
    clustering.insert_one(Dict)

def save_clustering_dup(db_name, labels_str, kmeans_score, k, idx):
    client = MongoClient()
    clustering = client[db_name].clustering_dup2
    dict_scores = {'km_inertia' : kmeans_score}
    labels = list(map(lambda x: int(x), labels_str))
    labels_str = list(map(lambda x: str(x), labels))
    l = list(set(labels))
    idx = list(map(lambda Id: str(Id), idx))
    df_labels = pd.DataFrame({'labels' : labels, 'index' : idx})
    Dict = {'n_clusters' : k,
            'id2labels' : dict(zip(idx, labels)),
            'labels2id' : dict([(str(label), list(df_labels[df_labels['labels'] == label]['index'])) for label in l]),
            'scores' : dict_scores,
            'date' : datetime.timestamp(datetime.now()),
	    'status': dict(zip(labels_str, [0 for i in range( len(labels) )] )),
    }
    clustering.insert_one(Dict)


def save_clustering_wmd(db_name, df_labels, wmd_score, k):
    client = MongoClient()
    clustering = client[db_name].clustering_wmd
    dict_scores = {'wmd' : wmd_score}
    idx = map(lambda Id: str(Id), list(df_labels['index']))
    labels = list(df_labels['labels'])
    labels_str =  map(lambda Id: str(Id), labels)
    Dict = {'n_clusters' : k,
            'id2labels' : dict(zip(idx, labels_str)),
            'labels2id' : dict([(str(label), list(df_labels[df_labels['labels'] == label]['index'])) for label in labels]),
            'scores' : dict_scores,
            'date' : datetime.timestamp(datetime.now()),
	    'status': dict(zip(labels_str, [0 for i in range( len(labels) )] )),
    }
    clustering.insert_one(Dict)


def main(path_to_params):

    js = open(path_to_params) 
    data = json.load(js)
    print("Getting norm texts")
    d = datetime.now() + timedelta(days=-1)
    list_of_texts_without, texts_id_without, texts_id_with, facts, list_of_texts_with = get_norm_texts_without_and_with_facts(data[0].get('db_name'), d)
    print("Number of new texts with facts", len(texts_id_with))
    print("Number of new texts without facts", len(texts_id_without))
    print("Clustering tf-idf for duplicate...")
    k_dup, score_dup, labels, index = clustering_for_duplicates(texts_id_with, list_of_texts_with, facts)
    print("Done")
    save_clustering_dup(data[0].get('db_name'), labels, score_dup, k_dup, index)
    idx = texts_id_with + texts_id_without
    texts = list_of_texts_with + list_of_texts_without
    print("Clustering doc2vec...")
    labels_doc2vec, centers_doc2vec, score_doc2vec, k_doc2vec, kmeans_inertia  =  clustering_doc2vec(texts, data[1].get('tolerance'),data[1].get('n_max'),idx, data[1].get('steps'))
    print("Done")
    save_clustering_doc2vec(data[0].get('db_name'), labels_doc2vec, score_doc2vec, kmeans_inertia, k_doc2vec, idx, centers_doc2vec)
    
if __name__ == "__main__":
    main(sys.argv[1:][0])
