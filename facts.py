"""
Module to make collection of facts.
Call function add_facts with argument:
    db_name -- where to get and save the data
Adds collection 'news_facts' to 'db_name' mongo database:
    Fields: news_id
            persons -- array of {name, middlename, lastname, descriptor}
            locations -- array of names
"""

from yargy.interpretation import InterpretationEngine
from natasha import Combinator
from natasha.grammars import Person, Location
from natasha.grammars.person import PersonObject
from natasha.grammars.location import LocationObject
from pymongo import MongoClient

class Facts():
    
    def __init__(self, news_id):
        self.news_id = news_id
        self.persons = []
        self.locations = []
    
    def add_to_persons(self, fact):
        self.persons.append(fact)

    def add_to_locations(self, fact):
        self.locations.append(fact)
        
    def get_values(self):
        facts = {'news_id' : self.news_id,
                'persons' : [f.get_dict() for f in self.persons],
                'locations' : [f.get_name() for f in self.locations]
                }
        return facts

def get_norm_fact(token):
    if isinstance(token, list):
        return " ".join(map(lambda f: f.forms[0]['normal_form'], token))
    elif token != None:
        return token.forms[0]['normal_form']
    else:
        return ""

class Person_():
    
    def __init__(self, person):
        self.name = get_norm_fact(person.firstname)
        self.middlename = get_norm_fact(person.middlename)
        self.lastname = get_norm_fact(person.lastname)
        self.descriptor = get_norm_fact(person.descriptor)


    def get_dict(self):
        fact = {'name' : self.name,
                'middlename' : self.middlename,
                'lastname' : self.lastname,
                'descriptor' : self.descriptor,
                }
        return fact


class Location_():
    
    def __init__(self, location):
        self.name = get_norm_fact(location.name)
    
    def get_name(self):
        return self.name


def find_facts(text, news_id):
    """"
    Find the facts in the news
    
    Agrs:
        title: the title text
        desc: the description text
        news_id
        
    Returns:
        dictionary:
            news_id
            persons -- array of {name, middlename, lastname, descriptor}
            locations -- array of names
    """
    engine_per = InterpretationEngine(PersonObject)
    engine_loc = InterpretationEngine(LocationObject)
    combinator = Combinator([Person, Location,])
    facts = Facts(news_id)
    matches = combinator.resolve_matches(combinator.extract(text),)

    for person in list(engine_per.extract(matches)):
        fact = Person_(person)
        facts.add_to_persons(fact)
    
    matches = combinator.resolve_matches(combinator.extract(text),)
    
    for location in list(engine_loc.extract(matches)):
        fact = Location_(location)
        facts.add_to_locations(fact)
    return facts.get_values()


def add_facts(texts, id_text, db_name):
    """"
    Add facts to database
    
    Agrs:
        db_name: database name
    
    News facts:
        dictionary:
            news_id
            persons -- array of {name, middlename, lastname, descriptor}
            locations -- array of names
    """
    client = MongoClient()
    News_facts = client[db_name].news_facts
    for i in range(len(texts)):
        text = texts[i]
        news_id = id_text[i]
        val = find_facts(text, news_id)
        if News_facts.find_one({'news_id' : news_id}) == None and (len(val['persons']) + len(val['locations']) > 0):
            News_facts.insert_one(val)