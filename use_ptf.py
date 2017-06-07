from parser import start_processes
from normalizer import transform
from facts import add_facts
from get_texts import get_row_texts
from pymongo import MongoClient
import json 
import sys
from datetime import datetime
"""
Module to start parser, normalizer, adding facts

Args:
    path to json

Example:
    [{"limit": 10000, "db_name": "db2", "times" : 2, "time" : 30}]
"""



def main(path_to_params):
    js = open(path_to_params) 
    params = json.load(js)
    db_name = params[0].get('db_name')
    limit = params[0].get('limit')
    times = params[0].get('times')
    time = params[0].get('time')
    d = datetime.now()
    print("########################################")
    print("Start parser rss")
    start_processes(times, time, db_name, limit)
    print("Parser rss ended work")
    print("########################################")
    print("Start getting texts")
    texts, id_text = get_row_texts(db_name, d)
    print("Number of texts", len(texts))
    print("Start normalizer")
    transform(texts, id_text, db_name)
    print("Normalizer ended work")
    print("########################################")
    print("Start adding facts")
    add_facts(texts, id_text, db_name)
    print("Adding facts ended")
    print("########################################")
	

if __name__ == "__main__":
    main(sys.argv[1:][0])