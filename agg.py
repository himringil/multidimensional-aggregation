from json import load
from datetime import timedelta
from pytimeparse.timeparse import timeparse
from abc import ABC, abstractmethod
from anytree import NodeMixin
from pympler import asizeof

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

    def delete_zero_elements(self):
        self.tree.delete_zero_elements()
        for descendant in self.tree.descendants:
            descendant.delete_zero_elements()

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

    def size(self):
        size = asizeof.asizeof(self.tree.queue)
        for descendant in self.tree.descendants:
            size += asizeof.asizeof(descendant.queue)
        return size

def load_tree(path):
    f = open(path)
    return load(f)

def load_params(path):
    f = open(path)
    return load(f)

def aggregate(tree, df):
    for index, row in df.iterrows():
        if not row['src'] or not row['dst']:
            continue
        try:
            tree.aggregate(row)
        except Exception as e:
            print(f'Exception at {index}: {e}')
    return tree
