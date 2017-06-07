from pymongo import MongoClient
from gensim.models import doc2vec
from scipy.spatial.distance import cdist, pdist
from sklearn.cluster import KMeans

import numpy as np
import matplotlib.pyplot as plt

from normalizer import normalize_text, del_stopwords
from get_texts import get_norm_texts
from sklearn.metrics.pairwise import euclidean_distances
from sklearn.externals import joblib

def preprocess_data(documents):
    '''
    Preprocess data (already normalized) before it can be used in gensim.models.doc2vec.Doc2Vec
    Args:
        documents: list of strings
    Returns:
        list of tagged documents
    '''
    tagged_documents = []
    for i, document in enumerate(documents):
        tagged_documents.append(doc2vec.TaggedDocument(document, [i]))
    return tagged_documents

def KMeans_n_clusters(data, k_range):
    '''
    The elbow method looks at the percentage of variance explained as a function of the number of clusters
    Args:
        data: dataset
        k_range: range of the number of clusters
    Returns:
        numpy.ndarray (shape: (len(k_range),)): percentage of variance explained for each cluster
            (between-cluster sum of squares / total sum of squares)
    '''    
    # fit the KMeans model for each n_clusters=k
    k_means_var = [KMeans(n_clusters=k).fit(data) for k in k_range]
    
    # get cluster centers for each model
    centroids = [X.cluster_centers_ for X in k_means_var]
    
    # get the Euclidean distance from each vector to each cluster center
    k_euclid = [cdist(data, cent) for cent in centroids]
    dist = [np.min(ke, axis=1) for ke in k_euclid]
    
    # total within-cluster sum of squares
    wcss = [sum(d**2) for d in dist]
    
    # total sum of squares
    tss = sum(pdist(data) ** 2) / len(data)
    
    # between-cluster sum of squares
    bss = tss - wcss
    
    return bss / tss

def clf_new_document(document, d2v_model, clust_model):
    '''
    Args:
        document: string
        d2v_model: gensim.models.doc2vec.Doc2Vec
        clust_model: sklearn trained model with predict method
    Return:
        int: cluster
    '''
    # normalize document text
    #norm_document = del_stopwords(normalize_text(document))
    
    # get vector for new document with doc2vec model
    vec = d2v_model.infer_vector(document) #.split()
    
    # predict cluster with trained clustering model 
    cluster = clust_model.predict(vec.reshape(1, -1))[0]
    
    return cluster


def clustering_doc2vec(norm_texts, tolerance, n_max, idxs, steps=15):
	#norm_texts = np.array(get_norm_texts(dbname))
    tagged_norm_texts = preprocess_data(norm_texts)
    d2v_model = doc2vec.Doc2Vec(tagged_norm_texts, size=100, window=8, min_count=10, workers=4)

    step = int(len(norm_texts)/steps)
    if step >= n_max:
        k_grid = range(step, int(len(norm_texts)/2), step)
    else:
        k_grid = range(step, n_max, step)

    pve = KMeans_n_clusters(d2v_model.docvecs, k_grid)

    k_opt = k_grid[np.argmax(pve >= tolerance)]

    kmeans = KMeans(n_clusters=k_opt)
    kmeans.fit(d2v_model.docvecs)

    c = kmeans.cluster_centers_
	
    d2v_elems = np.array(d2v_model.docvecs)

    centers = []
    centers = [idxs[np.argmin(euclidean_distances(c[i].reshape(1, -1), d2v_elems))] for i in range(k_opt)]
    d2v_model.save("doc2vec")
    joblib.dump(kmeans, 'kmeans.pkl')
    return kmeans.labels_,centers, pve[np.argmax(pve >= tolerance)], k_opt, kmeans.inertia_

def predict_clustering_doc2vec(norm_texts):
    kmeans = joblib.load('kmeans.pkl')
    d2v_model = doc2vec.Doc2Vec.load("doc2vec")
    return list(map(lambda document: clf_new_document(document, d2v_model, kmeans), norm_texts))