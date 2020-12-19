from sys import argv
from os import walk
from os.path import join
from datetime import datetime, timedelta
from pytimeparse.timeparse import timeparse
from json import load

import pandas as pd
from anytree import NodeMixin, RenderTree

from AggResult import AggResult

import time

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
            self.graph = dict()
    
            self.parent = parent
            if children:
                self.children = children
       
        def delete_zero_elements(self):
            for key in list(self.queue.keys()):
                if sum(self.queue[key]) == 0:
                    self.queue.pop(key)

                    # remove node from graph
                    self.graph.pop(key)
                    for graph_key in self.graph.keys():
                        if key in self.graph[graph_key]:
                            self.graph[graph_key].remove(key)

        def _append_graph(self, graph: dict):
            for key in graph.keys():
                if self.graph.get(key, None) is None:
                    self.graph[key] = graph[key].copy()
                    continue
                self.graph[key] |= graph[key]

        def add(self, dt: datetime, values, graph: dict):
    
            # start queue if it is empty
            if not self.time_start:
                for el in values:
                    self.queue[el] = [0] * self.q
                    self.queue[el][-1] = values[el]
                self.time_start = dt - self.time_range + self.time_delta
                self.graph = graph.copy()
                return
    
            if dt < self.time_start + self.time_range - self.time_delta:
                print(dt, self.time_start, self.time_range, self.time_delta, self.time_start + self.time_range - self.time_delta)
                raise ValueError
    
            # pop elements from queue and insert into childs while new element time not reached
            while not self.time_start + self.time_range - self.time_delta <= dt < self.time_start + self.time_range:
                old_values = dict()
                for el in self.queue:
                    value = self.queue[el].pop(0)
                    if value:
                        old_values[el] = value
                    self.queue[el].append(0)

                # filter graph
                old_graph = dict()
                for key in self.graph.keys():
                    if old_values.get(key, None) is not None:
                        old_graph[key] = set()
                        for value in self.graph[key]:
                            if old_values.get(value, None) is not None:
                                old_graph[key].add(value)

                for child in self.children:
                    child.add(self.time_start, old_values, old_graph)

                self.time_start += self.time_delta
    
            # new element belongs to last element of queue
            for el in values:
                if not self.queue.get(el):
                    self.queue[el] = [0] * self.q
                self.queue[el][-1] += values[el]
            self._append_graph(graph)
    

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
            print(f'{treestr.ljust(8)}: ts={node.time_start} tr={node.time_range} td={node.time_delta} nodes={len(node.graph.keys())}')
            for el in sorted(node.queue):
                print(f'{" " * len(pre)}{el}: {node.queue[el]}')
            # for el in node.graph:
            #     print(f'    {el}: {node.graph[el]}')

    def delete_zero_elements(self):
        self.tree.delete_zero_elements()
        for descendant in self.tree.descendants:
            descendant.delete_zero_elements()

    def select_params(self, row):
    
        values = dict()
        graph = dict()
        
        for param in self.params:
            key = ' & '.join([f'{el}={row[el]}' for el in param]) if type(param) is list else f'{param}={row[param]}'
            values[key] = 1
            graph[key] = set()

        for node1 in graph.keys():
            for node2 in graph.keys():
                if node1 != node2:
                    ps = node2.split(' & ')
                    for p in ps:
                        if node1.find(p) == -1:
                            break
                    else:
                        # node1 contain all parameters from node2
                        graph[node2].add(node1)
        
        return row['datetime'], values, graph
    
    def aggregate(self, row):
        datetime, values, graph = self.select_params(row)
        self.tree.add(datetime, values, graph)
    
    def _is_sublist(self, sub_lst, lst):
        return set(sub_lst) < set(lst)

    def _gen_relatives(self, relatives, graph: dict, queue: list):
        for rel in relatives:
            lst_params = rel[1].split(' & ')
            for sub_lst in rel[0]:
                for neightbor in graph[sub_lst]:
                    neightbor_params = [n.split('=')[0] for n in neightbor.split(' & ')]
                    if len(neightbor_params) == len(lst_params) and sorted(neightbor_params) == sorted(lst_params):
                        yield f'{neightbor} / {sub_lst}', [format(l/subl if subl else 0, '.3f') for subl, l in zip(rel[0][sub_lst], queue[neightbor])]

    def filter(self,  timeseries_name: list = [], absolute: list = [], relative: list = []):
        result = AggResult()
        if not timeseries_name:
            return result

        # delete bad relatives
        relative = [[param[0], param[1]] for param in relative if self._is_sublist(param[0].split(' & '), param[1].split(' & '))]

        time_series_nodes = [self.tree]
        while time_series_nodes:
            time_series_node = time_series_nodes.pop(0)
            for child in time_series_node.children:
                time_series_nodes.append(child)
            
            # filter time series
            if time_series_node.name not in timeseries_name:
                continue
            
            result.add(time_series_node.name, time_series_node.time_start, time_series_node.time_range, time_series_node.time_delta)
            rel_result = []
            for i in relative:
                rel_result.append([dict(), i[1]])

            # filter queues
            for key in time_series_node.queue:
                
                if absolute:
                    # filter absolute
                    for param in absolute:
                        if param[0] not in key or f'{param[0]}={param[1]}' not in key:
                            break
                    else:
                        # name of queue contain all required params
                        result[time_series_node.name].add(key, time_series_node.queue[key])

                # filter relative
                for i, param in enumerate(relative):
                    keys = [k.split('=')[0] for k in key.split(' & ')]
                    p_lst = param[0].split(' & ')
                    if len(keys) == len(p_lst) and sorted(keys) == sorted(p_lst):
                        rel_result[i][0][key] = time_series_node.queue[key]

            for key, value in self._gen_relatives(rel_result, time_series_node.graph, time_series_node.queue):
                result[time_series_node.name].add(key, value)

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

if __name__ == '__main__':

    if len(argv) < 4:
        raise Exception('args: <tree_config_path> <params_config_path> <data_folder_path>')

    for tree, td in aggregate(load_tree(argv[1]), load_params(argv[2]), argv[3]):
        #tree.print()
    
        ts = ['10sec -> 1sec', '10min -> 1min', '5hour -> 30min']
    
        print('--------------------------------')
        tree.filter(ts, [['src', '192.168.1.10']]).print()
        print('--------------------------------')
        tree.filter(ts, [['service', '137']]).print()
        print('--------------------------------')
        tree.filter(ts, [['', '192.168.1.50'], ['service', '']]).print()
    
         
        tree.filter(['10sec -> 1sec'],
                    absolute=[['src', ''], ['dst', '']],
                    relative=[['src', 'src & dst'],
                              ['dst', 'src & dst']]).print()
        
        break
