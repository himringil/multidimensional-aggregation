from sys import argv
from os import walk
from os.path import join
from datetime import datetime, timedelta
from pytimeparse.timeparse import timeparse
from json import load

import pandas as pd
from anytree import NodeMixin, RenderTree
import networkx as nx

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
            self.graph = nx.DiGraph()
    
            self.parent = parent
            if children:
                self.children = children
    
        def add_element(self, dt: datetime, values, graph):
    
            # start queue if it is empty
            if not self.time_start:
                for el in values:
                    self.queue[el] = [0] * self.q
                    self.queue[el][-1] = values[el]
                self.time_start = dt - self.time_range + self.time_delta
                self.graph = nx.compose(self.graph, graph)
                return
    
            if dt < self.time_start + self.time_range - self.time_delta:
                #print(dt, self.time_start, self.time_range, self.time_delta, self.time_start + self.time_range - self.time_delta)
                raise ValueError
    
            # pop elements from queue and insert into childs while new element time not reached
            while not self.time_start + self.time_range - self.time_delta <= dt < self.time_start + self.time_range:
                old_values = dict()
                for el in self.queue:
                    old_values[el] = self.queue[el].pop(0)
                    self.queue[el].append(0)
                yield old_values, self.time_start, self.graph
                self.time_start += self.time_delta
    
            # new element belongs to last element of queue
            for el in values:
                if not self.queue.get(el):
                    self.queue[el] = [0] * self.q
                self.queue[el][-1] += values[el]
            self.graph = nx.compose(self.graph, graph)
    
            for key in list(self.queue.keys()):
                if sum(self.queue[key]) == 0:
                    self.queue.pop(key)
                    self.graph.remove_node(key)
    
            return

    def __init__(self, tree: dict, params: list):
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

    def print_tree(self):
      for pre, _, node in RenderTree(self.tree):
          treestr = u"%s%s" % (pre, node.name)
    
          print(f'{treestr.ljust(8)}: ts={node.time_start} tr={node.time_range} td={node.time_delta}'
                f' nodes={node.graph.number_of_nodes()} edges={node.graph.number_of_edges()}')
          
          for el in sorted(node.queue):
              print(f'{" " * len(pre)}{el}: {node.queue[el]}')

    def select_params(self, row, params):
    
        values = dict()
        graph = nx.DiGraph()
        
        for param in params:
            key = ' && '.join([f'{el}={row[el]}' for el in param]) if type(param) is list else f'{param}={row[param]}'
            values[key] = 1
            graph.add_node(key)
        
        for node1 in graph.nodes:
            for node2 in graph.nodes:
                if node1 != node2:
                    ps = node2.split(' && ')
                    for p in ps:
                        if node1.find(p) == -1:
                            break
                    else:
                        # node1 contain all parameters from node2
                        graph.add_edge(node1, node2)
        
        return row['datetime'], values, graph
 
    def modify_node(self, node: TimeSeries, dt: datetime, values, graph):
        for _values, _time_start, _graph in node.add_element(dt, values, graph):
            # insert it to childs
            for child in node.children:
                self.modify_node(child, _time_start, _values, _graph)

    def aggregate(self, row):
        datetime, values, graph = self.select_params(row, self.params)
        self.modify_node(self.tree, datetime, values, graph)
    
        
def load_tree(path):
    f = open(path)
    return load(f)

def load_params(path):
    f = open(path)
    return load(f)

def aggregate(path: str, tree: AggTree):
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
                        tree.aggregate(row)
                    except Exception as e:
                        pass

                tree.print_tree()
                return

if __name__ == '__main__':

    if len(argv) < 4:
        raise Exception('args: <tree_config_path> <params_config_path> <data_folder_path>')

    aggregate(argv[3], AggTree(load_tree(argv[1]), load_params(argv[2])))
