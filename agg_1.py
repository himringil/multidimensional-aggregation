from sys import argv
from os import walk
from os.path import join
from datetime import datetime, timedelta
from pytimeparse.timeparse import timeparse
from json import load

import pandas as pd
from anytree import NodeMixin, RenderTree

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
    
        def add_element(self, dt: datetime, values):
    
            # start queue if it is empty
            if not self.time_start:
                for el in values:
                    self.queue[el] = [0] * self.q
                    self.queue[el][-1] = values[el]
                self.time_start = dt - self.time_range + self.time_delta
                return
    
            if dt < self.time_start + self.time_range - self.time_delta:
                print(dt, self.time_start, self.time_range, self.time_delta, self.time_start + self.time_range - self.time_delta)
                raise ValueError
    
            # new element belongs to last element of queue
            if self.time_start + self.time_range - self.time_delta <= dt < self.time_start + self.time_range:
                for el in values:
                    if not self.queue.get(el):
                        self.queue[el] = [0] * self.q
                    self.queue[el][-1] += values[el]
                return
    
            # pop elements from queue and insert into childs while new element time not reached
            while not self.time_start + self.time_range - self.time_delta <= dt < self.time_start + self.time_range:
                old_values = dict()
                for el in self.queue:
                    old_values[el] = self.queue[el].pop(0)
                    self.queue[el].append(0)
                yield old_values, self.time_start
                self.time_start += self.time_delta
    
            for el in values:
                if not self.queue.get(el):
                    self.queue[el] = [0] * self.q
                self.queue[el][-1] = values[el]
    
            return

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

    @staticmethod
    def print_tree(tree):
        for pre, _, node in RenderTree(tree):
            treestr = u"%s%s" % (pre, node.name)
            print(f'{treestr.ljust(8)}: ts={node.time_start} tr={node.time_range} td={node.time_delta}')
            for el in sorted(node.queue):
                if sum(node.queue[el]) / len(node.queue[el]) > 1:
                    print(f'{" " * len(pre)}{el}: {node.queue[el]}')

    def print(self):
        AggTree.print_tree(self.tree)

    def select_params(self, row):
    
        values = dict()
    
        for param in self.params:
            values[' && '.join([f'{el}={row[el]}' for el in param]) if type(param) is list else f'{param}={row[param]}'] = 1
    
        return row['datetime'], values

    def modify_node(self, node: TimeSeries, dt: datetime, values):
        for _values, _time_start in node.add_element(dt, values):
            # insert it to childs
            for child in node.children:
                self.modify_node(child, _time_start, _values)

    def aggregate(self, row):
        datetime, values = self.select_params(row)
        self.modify_node(self.tree, datetime, values)

    def _get_node_queues(self, params, node):
        new_node = self.TimeSeries(node.name, node.time_range, node.time_delta, parent=None, children=[self._get_node_queues(params, child) for child in node.children])
        for key in node.queue:
            for param in params:
                if param[0] not in key or f'{param[0]}={param[1]}' not in key:
                    break
            else:
                new_node.queue[key] = node.queue[key]
        return new_node

    def get_queues(self, params: list):
        return self._get_node_queues(params, self.tree)


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
                        print(e)
                        
                tree.print()

                print('--------------------------------')
                AggTree.print_tree(tree.get_queues([['service', '']]))
                print('--------------------------------')
                AggTree.print_tree(tree.get_queues([['service', '137']]))
                print('--------------------------------')
                AggTree.print_tree(tree.get_queues([['', '192.168.1.20'], ['', '44818']]))
                print('--------------------------------')

                return

if __name__ == '__main__':

    if len(argv) < 4:
        raise Exception('args: <tree_config_path> <params_config_path> <data_folder_path>')

    aggregate(load_tree(argv[1]), load_params(argv[2]), argv[3])
