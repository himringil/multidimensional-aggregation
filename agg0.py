from sys import argv
from os import walk
from os.path import join
from datetime import datetime, timedelta
from pytimeparse.timeparse import timeparse
from json import load

import pandas as pd
from anytree import NodeMixin, RenderTree

from AggResult import AggResult

class AggTree():

    class TimeSeries(NodeMixin):
    
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
    
        def _delete_zero_elements(self):
            for key in list(self.queue.keys()):
                if sum(self.queue[key]) == 0:
                    self.queue.pop(key)

        def add(self, dt: datetime, values):
    
            # start queue if it is empty
            if not self.time_start:
                for el in values:
                    if values[el] != 0:
                        self.queue[el] = [0] * self.q
                        self.queue[el][-1] = values[el]
                if self.queue:
                    self.time_start = dt - self.time_range + self.time_delta
                return
    
            if dt < self.time_start + self.time_range - self.time_delta:
                print(dt, self.time_start, self.time_range, self.time_delta, self.time_start + self.time_range - self.time_delta)
                raise ValueError
    
            # pop elements from queue and insert into childs while new element time not reached
            while not self.time_start + self.time_range - self.time_delta <= dt < self.time_start + self.time_range:
                old_values = dict()
                for el in self.queue:
                    old_values[el] = self.queue[el].pop(0)
                    self.queue[el].append(0)
                for child in self.children:
                    child.add(self.time_start, old_values)
                self.time_start += self.time_delta
    
            # new element belongs to last element of queue
            for el in values:
                if not self.queue.get(el):
                    self.queue[el] = [0] * self.q
                self.queue[el][-1] += values[el]
    
            self._delete_zero_elements()


    def __init__(self, tree: dict, params: list):
        # TODO: check params struct
        self.tree = self._create_tree(tree)
        self.params = params

    def _create_tree(self, js):
        
        if not js:
            return None
    
        if not js.get('name', None) or not js.get('range', None) or not js.get('delta', None):
            raise Exception('Bad json format')
    
        return self.TimeSeries(name=js['name'],
                               time_range=timedelta(seconds=timeparse(js['range'])),
                               time_delta=timedelta(seconds=timeparse(js['delta'])),
                               children=[self._create_tree(c) for c in js.get('child', [])])

    def print(self):
        for pre, _, node in RenderTree(self.tree):
            treestr = u"%s%s" % (pre, node.name)
            print(f'{treestr.ljust(8)}: ts={node.time_start} tr={node.time_range} td={node.time_delta}')
            for el in sorted(node.queue):
                print(f'{" " * len(pre)}{el}: {node.queue[el]}')

    def select_params(self, row):
        values = dict()
        for param in self.params:
            key = ' && '.join([f'{el}={row[el]}' for el in param]) if type(param) is list else f'{param}={row[param]}'
            values[key] = 1
        return row['datetime'], values
    
    def aggregate(self, row):
        datetime, values = self.select_params(row)
        self.tree.add(datetime, values)
    
    def filter(self, params: list, timeseries_name: list):
        result = AggResult()
        time_series_nodes = [self.tree]
        while time_series_nodes:
            time_series_node = time_series_nodes.pop(0)
            for child in time_series_node.children:
                time_series_nodes.append(child)
            # filter time series
            if time_series_node.name not in timeseries_name:
                continue
            result.add(time_series_node.name, time_series_node.time_start, time_series_node.time_range, time_series_node.time_delta)
            # filter queues
            for key in time_series_node.queue:
                for param in params:
                    if param[0] not in key or f'{param[0]}={param[1]}' not in key:
                        break
                else:
                    # name of queue contain all requires params
                    result[time_series_node.name].add(key, time_series_node.queue[key])
        return result


def load_tree(path):
    f = open(path)
    return load(f)

def load_params(path):
    f = open(path)
    return load(f)

def aggregate(tree_conf: str, params_conf: str, data_path: str):
    tree = AggTree(tree_conf, params_conf)
    
    for (_, _, filenames) in walk(data_path):
        for filename in sorted(filenames):
            if filename.endswith('.parquet'):
                
                filepath = join(data_path, filename)
                df = pd.read_parquet(filepath)
                df['datetime'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'], errors='coerce', format='%d%b%Y %H:%M:%S')
                df.drop(axis=1, columns=['date', 'time'], inplace=True)
                df.rename(columns={'i/f_name' : 'if_name', 'i/f_dir' : 'if_dir'}, inplace=True)
                
                for index, row in df.iterrows():
                    if not row['src']:
                        continue
                    try:
                        tree.aggregate(row)
                    except Exception as e:
                        pass
                
                tree.print()

                print('--------------------------------')
                tree.filter([['service', '']], ['10sec -> 1sec']).print()
                print('--------------------------------')
                tree.filter([['service', '137']], ['10min -> 30sec']).print()
                print('--------------------------------')
                tree.filter([['', '192.168.1.20'], ['', '44818']], ['2hours -> 20min', '1hour -> 5min']).print()

                return

if __name__ == '__main__':

    if len(argv) < 4:
        raise Exception('args: <tree_config_path> <params_config_path> <data_folder_path>')

    aggregate(load_tree(argv[1]), load_params(argv[2]), argv[3])