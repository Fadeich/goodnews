"""
Module to cluster texts with facts

Fuction: clustering_for_duplicates

Args:
    labels : result of clustering
    norm_text_len: array of number of words in each news
    index : indexes of news
    num_labels : number of clusters (max label)
    
"""
from sklearn.decomposition import PCA
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.cluster import KMeans
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from pymongo import MongoClient
import numpy as np

def find_duplicate(texts, facts, times=1, num_clusters=350):
    """"
    Find the duplicates among news
    
    Agrs:
        tfidf_matrix: tfidf of normalized news (with some facts!)
        facts: array of strings
        times : importance of facts (1 -- very important, 2 -- less and so on)
        df_full : DataFrame with facts and tfidf
        num_clusters : number of clusters
    Returns:
        df_full : DataFrame with facts and tfidf
        labels : result of clustering
    """
    tfidf_vectorizer = TfidfVectorizer(min_df=0.01, max_df=0.9)
    tfidf_matrix = tfidf_vectorizer.fit_transform(texts)
    print("shape tfidf", tfidf_matrix.shape)
    count_vectorizer = CountVectorizer(min_df=2)
    df_facts = pd.DataFrame(count_vectorizer.fit_transform(facts).toarray())
    print("shape facts", df_facts.shape)
    if tfidf_matrix.shape[1] < 200000 and df_facts.shape[1]*times < tfidf_matrix.shape[1]:
        pca = PCA(n_components=df_facts.shape[1]*times, random_state=1)
        df_texts = pd.DataFrame(pca.fit_transform(tfidf_matrix.toarray()))
    elif df_facts.shape[1]*times < tfidf_matrix.shape[1]:
        tsvd = TruncatedSVD(n_components=df_facts.shape[1]*times, random_state=1)
        df_texts = tsvd.fit_transform(tfidf_matrix)
    else:
        df_texts = pd.DataFrame(tfidf_matrix.toarray())
    df_full = pd.concat((df_texts, df_facts), axis=1)
    km = KMeans(n_clusters=num_clusters, random_state=5)
    km.fit(df_full)
    return df_full, km.labels_.tolist(), km.inertia_

def relabel(lab, dic):
    return dic[lab]

def get_labels_dup(labels, index, num_labels):
    """"
    Preprocessing of clustering (search for duplicate) results 
    
    Agrs:
        labels : result of clustering
        index : indexes of news
        num_labels : number of clusters (max label)
    Returns:
        labels : labels (not bigger than 10 percent)
        index : index for labels
    """
    df_labels = pd.DataFrame({'labels' : labels, 'index' : index}, index=index)
    sizes = df_labels.groupby('labels', group_keys=False).size().get_values()
    ind_labels = (sizes < len(labels)*0.1)*(sizes > 1)
    labels = np.arange(num_labels)[ind_labels]
    df_labels = df_labels[df_labels['labels'].apply(lambda x: ind_labels[x])]
    return list(df_labels['labels']), list(df_labels['index'])


def clustering_for_duplicates(index, norm_texts, facts):
    """"
    Search for duplicate
    
    Agrs:
        facts: array of facts
        norm_text_len: array of number of words in each news
        index: indexes of news
    Returns:
        labels : labels (not bigger than 10 percent)
        index : index for labels
        num_labels : number of clusters
        score : k_means inertia
    """
    norm_texts_str = list(map(lambda text: " ".join(text), norm_texts))
    facts_str = list(map(lambda text: " ".join(text), facts))
    num_labels = len(facts_str)//10
    
    df_full, labels, score = find_duplicate(norm_texts_str, facts_str, num_clusters=num_labels)
    norm_text_len = list(map(lambda text: len(text), norm_texts))
    #df_labels, uniq_idxs = get_dataframe_for_dup(labels, norm_text_len, index, num_labels)
    labels, index = get_labels_dup(labels, index, num_labels)
    return len(labels), score, labels, index

