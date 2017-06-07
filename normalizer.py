"""
Module to normalize texts and save them to database
Call function transform
Args:
    texts (list): list of texts
    id_text: their id in news collection
    db_name: database name where to save normalized texts
"""

from pymongo import MongoClient
from stop_words import get_stop_words
import pymorphy2
import re


def normalize_text(text):
    """
    Transforms words from text to normal form
    
    Agrs:
        text (str): text
    
    Returns:
        array: array of words in normal form
    """
    words = ''.join(char for char in text.lower() if char.isalpha() or char == ' ').split()
    morph = pymorphy2.MorphAnalyzer()
    return [morph.parse(word)[0].normal_form for word in words]


def del_stopwords(text_array):
    """
    Deletes stop-words from text
    
    Args:
        text (array): array of words
        
    Returns:
        array: array without stop-words
    """
    stop_words = get_stop_words('russian')
    return [word for word in text_array if word not in stop_words]


def put_to_db(norm_text, id_text, db_name):
    """
    Inserts text to database
    
    Args:
        norm_text (array): normalized text
        id-text: id in news collection
        db_name: database name
    """
    client = MongoClient()
    News_norm = client[db_name].norm_news
    norm_news = {'text_id' : id_text,
                'norm_text' : norm_text
                }
    News_norm.insert_one(norm_news)
    
    
def transform(texts, id_text, db_name):
    """
    Transformes texts and save them to database
    
    Args:
        texts (list): list of texts
        id_text: their id in news collection
        db_name: database name where to save normalized texts
    """
    for i in range(len(texts)):
        norm_text = del_stopwords(normalize_text(texts[i]))
        put_to_db(norm_text, id_text[i], db_name)



