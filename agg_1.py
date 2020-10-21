from sys import argv
from os import walk
from os.path import join
from datetime import datetime, timedelta
from pytimeparse.timeparse import timeparse
from json import load

import pandas as pd
from anytree import NodeMixin, RenderTree

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

def modify_node(node: TimeSeries, dt: datetime, value):
    for values, time_start in node.add_element(dt, value):
        # insert it to childs
        for child in node.children:
            modify_node(child, time_start, values)

def select_params(row, params):

    values = dict()

    for param in params:
        values[str(param) + '__' + str(row[param])] = 1

    return row['datetime'], values

def print_tree(tree):
    for pre, _, node in RenderTree(tree):
        treestr = u"%s%s" % (pre, node.name)
        print(f'{treestr.ljust(8)}: ts={node.time_start} tr={node.time_range} td={node.time_delta}')
        for el in sorted(node.queue):
            if sum(node.queue[el]) / len(node.queue[el]) > 1:
                print(f'{" " * len(pre)}{el}: {node.queue[el]}')

def aggregate(path, tree, params):

    for (_, _, filenames) in walk(path):
        for filename in sorted(filenames):
            if filename.endswith('.parquet'):
                
                filepath = join(path, filename)
                df = pd.read_parquet(filepath)
                df['datetime'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'], errors='coerce', format='%d%b%Y %H:%M:%S')
                df.drop(axis=1, columns=['date', 'time'], inplace=True)
                df.rename(columns={'i/f_name' : 'if_name', 'i/f_dir' : 'if_dir'}, inplace=True)
                
                for index, row in df.iterrows():
                    if not row['src']:
                        continue
                    try:
                        datetime, values = select_params(row, params)
                        modify_node(tree, datetime, values)
                    except Exception as e:
                        print(e)
                        
                print_tree(tree)
                return

def create_tree(js):
    
    if not js:
        return None

    if not js.get('name', None) or not js.get('range', None) or not js.get('delta', None):
        raise Exception('Bad json format')

    return TimeSeries(name=js['name'],
                      time_range=timedelta(seconds=timeparse(js['range'])),
                      time_delta=timedelta(seconds=timeparse(js['delta'])),
                      children=[create_tree(c) for c in js.get('child', [])])

def load_tree(path):
    f = open(path)
    js = load(f)
    return create_tree(js)

def load_params(path):
    f = open(path)
    return load(f)

if __name__ == '__main__':
    
    if len(argv) < 4:
        raise Exception('args: <data_folder_path> <tree_config_path> <params_config_path>')

    tree = load_tree(argv[2])
    params = load_params(argv[3])

    aggregate(argv[1], tree, params)
