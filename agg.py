import pandas as pd
from os.path import join
from os import walk
from json import load
from datetime import datetime, timedelta
from pytimeparse.timeparse import timeparse
from abc import ABC, abstractmethod
from anytree import NodeMixin

class AggTreeBase(ABC):

    class TimeSeriesBase(ABC, NodeMixin):

        def __init__(self, name, time_range: timedelta, time_delta: timedelta, parent=None, children=None):
    
            self.q, r = divmod(time_range, time_delta)
            if time_range < time_delta or r:
                raise ValueError
    
            self.name = name
            
            self.time_range = time_range
            self.time_delta = time_delta
            self.time_start = None
    
            self.queue = dict()
    
            self.parent = parent
            if children:
                self.children = children

        @abstractmethod
        def delete_zero_elements(self):
            pass

        @abstractmethod
        def add(self, *args):
            pass

    @abstractmethod
    def __init__(self, tree: dict, params: list):
        self.tree = self._create_tree(tree)

    @abstractmethod
    def _correct_params(self, params):
        pass

    def _create_tree(self, js):
        if not js:
            return None
        if not js.get('name', None) or not js.get('range', None) or not js.get('delta', None):
            raise Exception('Bad json format')
        return self.TimeSeries(name=js['name'],
                               time_range=timedelta(seconds=timeparse(js['range'])),
                               time_delta=timedelta(seconds=timeparse(js['delta'])),
                               children=[self._create_tree(c) for c in js.get('child', [])])

    @abstractmethod
    def print(self):
        pass

    @abstractmethod
    def delete_zero_elements(self):
        pass

    @abstractmethod
    def select_params(self, row):
       pass
    
    @abstractmethod
    def aggregate(self, row):
        pass
    
    def _is_sublist(self, sub_lst, lst):
        return set(sub_lst) < set(lst)

    @abstractmethod
    def _gen_relatives(self, relatives):
        pass

    @abstractmethod
    def filter(self, timeseries_name: list = [], absolute: list = [], relative: list = []):
        pass

def load_tree(path):
    f = open(path)
    return load(f)

def load_params(path):
    f = open(path)
    return load(f)

def aggregate(tree, data_path):
    
    for (_, _, filenames) in walk(data_path):
        for filename in sorted(filenames):
            if filename.endswith('.parquet'):

                filepath = join(data_path, filename)
                df = pd.read_parquet(filepath)
                df['datetime'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'], errors='coerce', format='%d%b%Y %H:%M:%S')
                df.drop(axis=1, columns=['date', 'time'], inplace=True)
                df.rename(columns={'i/f_name' : 'if_name', 'i/f_dir' : 'if_dir'}, inplace=True)

                print(f'    {datetime.now()} -> {filename} ({len(df.index)} rows)')
                
                td = timedelta(0)

                for index, row in df.iterrows():
                    tm = datetime.now()
                    if not row['src'] or not row['dst']:
                        continue
                    try:
                        tree.aggregate(row)
                    except Exception as e:
                        print(f'Exception at {index}: {e}')
                    td += (datetime.now() - tm)

                tree.delete_zero_elements()
                
                yield tree, td
        break