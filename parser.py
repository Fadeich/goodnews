"""
Module to parse rss.
Call function start_processes with atguments:
    times -- how many times to parse each resource, 
    time -- how much to wait before parsing
    db_name -- where to save data
    limit -- approximate required quantity of news
Get 'db_name' mongo database with collection news:
        Fields: title, source, pubDate and description
    and collection statistics
        Fields: source, number and time
Attention! description can be missed
"""


from bs4 import BeautifulSoup
import urllib.request
import threading
import time
from dateutil.parser import parse
from pymongo import MongoClient
from multiprocessing import Process
from datetime import datetime
from urllib.error import  URLError, HTTPError

def put_to_db(item, News, path):
    """
    insert data to database
    Fields: title, source, pubDate and description
    """
    if News.find_one({'title' : item.title.string.strip()}) == None:
        news = {'title' : item.title.string.strip(),
                'link' : item.link.string.strip(),
                'source' : path, 
                'pubDate' : parse(item.pubDate.string),
                'date' : datetime.now(),
                }
        if (item.find('description') != None) and (len(item.description.text)!=0):
            news['description'] = item.description.string.strip()
        else:
            news['description'] = " "
        News.insert_one(news)
        return 1
    else:
        return 0


def add_stat(Statistics, path, num_news):
    """
    insert statistics to database
    Fields: source, number and time
    """
    Statistics.insert_one({
        'source' : path, 
        'number' : num_news,
        'time' : str(datetime.now()), 
    })
    pipe = [{'$group': {'_id': None, 'total': {'$sum': '$number'}}}]
    num = 0
    for st in Statistics.aggregate(pipeline=pipe):
        num = st['total']
    print("There are", num, "news now. ", path)
    return num


def parse_news(path, db_name):
    """
    Parse the rss-file, opens database and calls the function put_to_db
    """
    client = MongoClient()
    News = client[db_name].news
    Statistics = client[db_name].statistics
    try:
        req = urllib.request.Request(path)
        response = urllib.request.urlopen(req)
    except URLError as e:
        print(e.reason)
        return add_stat(Statistics, path, 0)
    except HTTPError as e:
        print('The server couldn\'t fulfill the request.')
        print('Error code: ', e.code)
        return add_stat(Statistics, path, 0)
    except:
        return add_stat(Statistics, path, 0)
    try:
        content = response.read()
    except:
        return add_stat(Statistics, path, 0)
    soup = BeautifulSoup(content, 'xml')
    num_news = 0
    for item in reversed(soup.findAll('item')):
        try:
            num_news += put_to_db(item, News, path)
        except (ValueError, TypeError, AttributeError):
            pass
    return add_stat(Statistics, path, num_news)


def run(path, times, t, db_name, limit):
    """
    Runs the function parse_news 'times' times with an interval of t seconds.
    """
    print("run", path)
    for i in range(times):
        time.sleep(t)
        num = parse_news(path, db_name)
        if num >= limit:
           break


# Starting processes for resources
#if __name__ == '__main__':
def start_processes(times, t, db_name, limit):
    """
    Starts a process for each resource and calls the function run.
    input: times -- how many times to parse each resource, 
           time -- how much to wait before parsing
           db_name -- where to save data
    """
    Array_of_links = ['https://news.yandex.ru/index.rss', 
                      'https://meduza.io/rss/news',
                      'https://russian.rt.com/rss', 
                      'http://static.feed.rbc.ru/rbc/logical/footer/news.rss', 
                      'http://www.vedomosti.ru/rss/news',
                      'https://www.gazeta.ru/export/rss/first.xml', 
                      'https://www.gazeta.ru/export/rss/lenta.xml',
                      'http://www.vesti.ru/vesti.rss', 
                      'https://tvrain.ru/export/rss/all.xml', 
                      'https://rg.ru/xml/index.xml',
                      'http://wsjournal.ru/feed/', 
                      'https://life.ru/xml/feed.xml',
                      #'http://www.pravda.com.ua/rus/rss/'
                      'http://www.kommersant.ru/RSS/main.xml',
                      'http://www.kommersant.ru/RSS/news.xml',
                      'http://www.interfax.ru/rss.asp',
                      'http://izvestia.ru/xml/rss/all.xml',
                      'http://tass.ru/rss/v2.xml',
                      'http://vm.ru/rss/vmdaily.xml',
                      'http://www.aif.ru/rss/all.php',
                      'http://fedpress.ru/feed/rss',
                      'https://regnum.ru/rss/news',
                      'http://www.svpressa.ru/newrss/',
                      'http://www.ntv.ru/exp/newsrss_top.jsp',
                      'http://ren.tv/export/feed.xml',
                      'https://www.bfm.ru/news.rss?type=news',
                      'http://www.ng.ru/rss/',
                      'http://ura.ru/rss',
                     ]
    Array_of_threads = []
    for link in Array_of_links:
        p = Process(target=run, args=(link, times, t, db_name, limit))
        Array_of_threads.append(p)
        p.start()
    while sum([i.is_alive() for i in Array_of_threads]):
      time.sleep(30)

