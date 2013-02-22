import pandas as pd

class Source(object): pass

class SQL(Source):
    name = 'sql'
    engines = {}

    def __init__(self, query, engine, index_col=None):
        self.query, self.engine, self.index_col = query, engine, index_col

    def __call__(self):
        import sqlalchemy
        if self.engine not in SQL.engines: 
            SQL.engines[self.engine] = sqlalchemy.create_engine(self.engine, pool_recycle=3600)
        results = SQL.engines[self.engine].execute(self.query)
        ret = pd.DataFrame.from_records(list(results), columns=results.keys(), coerce_float=True)
        if self.index_col is not None:
            ret.set_index(self.index_col)    
        return ret


class GDoc(Source):
    name = 'gdoc'
    def __init__(self, key, gid=0, csv_args={}):
        self.key, self.gid = key, gid
        self.csv_args = csv_args

    def __call__(self):
        import urllib2, csv, urllib
        return pd.read_csv(urllib2.urlopen("https://docs.google.com/spreadsheet/pub?key=%s&single=true&gid=%s&output=txt" % (self.key, self.gid)), sep="\t", **self.csv_args)

