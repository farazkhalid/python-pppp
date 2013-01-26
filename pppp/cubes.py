import pandas as pd
from contextlib import closing

class Cube(object):
    def __init__(self, sources={}, relationships=[], measures=None, filename=None, tables={}):
        self.sources = sources
        self.relationships = Cube.process_relationships(relationships)
        self.measures = (measures if measures else Measures)()
        self.m = self.measures
        self.tables = tables
        self.filename = filename
       
    @staticmethod
    def process_relationships(relationships):
        processed = {}
        for join in relationships:
            l, r = [e.strip().split(".") for e in join.split("->")]
            processed[(l[0], r[0])] = (l[1], r[1])
        return processed

    def measure(self, *args, **kwargs):
        return self.measures.add_measure(*args, **kwargs)

    def join(self, base_table, *tables):
        def join_table((table_names, table), next_table_name):
            for table_name in table_names:
                r = self.relationships.get((table_name, next_table_name))
                if r:
                    return table_names + [next_table_name], table.merge(self.tables[next_table_name], left_on=r[0], right_on=r[1])
            raise ValueError("Don't know how to join %s to %s" % (next_table_name, "/".join(table_names)))
        return reduce(join_table, tables, ([base_table], self.tables[base_table]))[1]
    
    def refresh(self, *tables):
        if not tables: tables = self.sources.keys()
        if callable(tables[0]): tables = [k for k, v in self.sources.items() if tables[0](v)]
        for t in tables:
            self.tables[t] = self.sources[t]()

    def save_data(self, filename=None):
        from pandas.io.pytables import HDFStore
        with closing(HDFStore(self.filename or filename)) as store:
            for name, data in self.tables.items():
                store['/data/%s' % name] = data

    def load_data(self, filename=None):
        from pandas.io.pytables import HDFStore
        with closing(HDFStore(self.filename or filename)) as store:
            tables = set(k.replace("/data/", "") for k in store.keys() if k.startswith('/data/'))
            for key in (tables & set(self.sources.keys())):
                self.tables[key] = store['/data/%s' % key]
            
class Measures(object):
    def add_measure(self, fn, name=None):
        import new
        im = new.instancemethod(fn, self, Measures)
        setattr(self, name if name else fn.func_name, im)
        return im

class Gecko(object):
    def __init__(self, api_key):
        self.api_key = api_key
        
    def push(self, widget_key, data):
        import requests, json
        return requests.post("https://push.geckoboard.com/v1/send/%s" % widget_key, json.dumps({'api_key' : self.api_key, 'data' : data}))
        
    def number(self, widget_key, number1, number2=None):
        data = {'item' : []}
        if number1: data['item'].append({'value' : number1, 'text' : ''})
        if number2: data['item'].append({'value' : number2, 'text' : ''})
        return self.push(widget_key, data)
    
    def rag(self, widget_key, *items):
        data = {'item' : []}
        for item in items:
            data['item'].append({'value' : item[0], 'text' : item[1]})
        return self.push(widget_key, data)
        
