"""
Module to get texts from database

Args:
    db_name: database name
	
"""

from pymongo import MongoClient
from datetime import datetime, timedelta

def get_row_texts(db_name, time):
    """
    Get unnormalized texts from database
    
    Agrs:
        db_name: database name
        time: timedate datum point
    Returns:
        two arrays: array unnormalized texts and their ids.
    """
    client = MongoClient()
    News = client[db_name].news
    News_norm = client[db_name].norm_news
    list_of_texts = []
    id_texts = []
    d = time + timedelta(hours=-10)
    print(d)
    print(News.find({ 'date': {"$gt": d} }).count())
    for news in News.find({ 'date': {"$gt": d} }):
        if News_norm.find_one({'text_id' : news['_id']}) == None:
            list_of_texts.append(news['title'] + " " + news['description'])
            id_texts.append(news['_id'])
    return list_of_texts, id_texts


def get_norm_texts(db_name, d):
    """
    Get normalized texts from database
    
    Agrs:
        db_name: database name
    
    Returns:
        array: array of normalized texts
    """
    client = MongoClient()
    News = client[db_name].news
    News_norm = client[db_name].norm_news
    list_of_texts = []
    for news in News.find({ 'date': {"$gt": d} }):
        norm_news = News_norm.find_one({'text_id' : news['_id']})
        list_of_texts.append(norm_news['norm_text'])
    return list_of_texts

def get_norm_texts_from_ids(texts_id, db_name):
    """
    Get normalized texts from database
    
    Agrs:
        db_name: database name
    
    Returns:
        array: array of normalized texts
    """
    client = MongoClient()
    News_norm = client[db_name].norm_news
    list_of_texts = []
    for ids in texts_id:
        norm_news = News_norm.find_one({'text_id' : ids})
        if norm_news:
            list_of_texts.append(norm_news['norm_text'])
    return list_of_texts

def get_norm_texts_without_and_with_facts(db_name, d):
    """
    Get normalized texts from database
    
    Agrs:
        db_name: database name
        date: timedate datum point
    Returns:
        array: array of normalized texts
    """
    client = MongoClient()
    News_facts = client[db_name].news_facts
    News_norm = client[db_name].norm_news
    News = client[db_name].news
    list_of_texts_without = []
    texts_id_without = []
    facts = []
    texts_id_with = []
    list_of_texts_with = []
    for news in News.find({ 'date': {"$gt": d} }):
            norm_news = News_norm.find_one({'text_id' : news['_id']})
            fact = News_facts.find_one({'news_id' : news['_id']})
            if fact == None and norm_news:
                list_of_texts_without.append(norm_news['norm_text'])
                texts_id_without.append(norm_news['text_id'])
            elif norm_news:
                news_id = news['_id']
                facts.append(fact['locations'] + [dic.get('lastname') for dic in fact['persons'] if dic.get('lastname') != ""])
                texts_id_with.append(news_id)
                list_of_texts_with.append(norm_news['norm_text'])
    return list_of_texts_without, texts_id_without, texts_id_with, facts, list_of_texts_with