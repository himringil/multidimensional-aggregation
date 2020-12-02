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
    
        class ValuesTree(NodeMixin):
             
            def __init__(self, fullname, name, value, parent=None, children=None):
    
                self.fullname = fullname
                self.name = name
                self.value = value

                self.parent = parent
                if children:
                    self.children = children

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
    
        def _merge_trees(self, nodes_to, node_from):
            for node_to in nodes_to:
                if node_to.name == node_from.name:
                    node_to.value[-1] += node_from.value
                    for child_from in node_from.children:
                        if not self._merge_trees(node_to.children, child_from):
                            value = child_from.value
                            child_from.value = [0] * self.q
                            child_from.value[-1] += value
                            child_from.parent = node_to
                            for descendant in child_from.descendants:
                                value = descendant.value
                                descendant.value = [0] * self.q
                                descendant.value[-1] += value
                    return True
            return False

        def _oldest_values_to_values_tree(self, el):
            el.value.append(0)
            return self.ValuesTree(el.fullname, el.name, el.value.pop(0), children=[self._oldest_values_to_values_tree(c) for c in el.children])

        def _delete_zero_nodes(self, node):
            for child in node.children:
                if sum(child.value) == 0:
                    child.parent = None
                else:
                    self._delete_zero_nodes(child)

        def add_element(self, dt: datetime, values):
    
            # start queue if it is empty
            if not self.time_start:
                for el in values:
                    self.queue[el] = self.ValuesTree('', '', [0] * self.q)
                    self._merge_trees([self.queue[el]], values[el])
                self.time_start = dt - self.time_range + self.time_delta
                return
    
            if dt < self.time_start + self.time_range - self.time_delta:
                #print(dt, self.time_start, self.time_range, self.time_delta, self.time_start + self.time_range - self.time_delta)
                raise ValueError
    
            # pop elements from queue and insert into childs while new element time not reached
            while not self.time_start + self.time_range - self.time_delta <= dt < self.time_start + self.time_range:
                old_values = dict()
                for el in self.queue:
                    old_values[el] = self._oldest_values_to_values_tree(self.queue[el])
                yield old_values, self.time_start
                self.time_start += self.time_delta
    
            # new element belongs to last element of queue
            for el in values:
                if not self.queue.get(el):
                    self.queue[el] = self.ValuesTree('', '', [0] * self.q)
                self._merge_trees([self.queue[el]], values[el])
    
            for el in self.queue:
                if sum(self.queue[el].value) == 0:
                    self.queue[el] = self.ValuesTree('', '', [0] * self.q)
                else:
                    self._delete_zero_nodes(self.queue[el])
    
            return

    def __init__(self, tree: dict, params: list):
        # TODO: check params struct
        self.params = dict()
        for param in params:
            self.params[self._get_full_name(param)] = param
        self.tree = self._create_tree(tree)

    def _get_full_name(self, param):
        name = ' && '.join([el for el in param if type(el) == str])
        subparam = param[-1] if type(param[-1]) == list else []
        while subparam:
            name = name + ' | ' + ' && '.join([el for el in subparam if type(el) == str])
            subparam = subparam[-1] if type(subparam[-1]) == list else []
        return name

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
            print(f'{treestr.ljust(8)}: ts={node.time_start} tr={node.time_range} td={node.time_delta}\n')
            for el in sorted(node.queue):
                print(f'{" " * len(pre)}{el}:\n')
                for pre1, _, node1 in RenderTree(node.queue[el]):
                    treestr1 = u"%s%s" % (pre1, node1.name)
                    print(f'{" " * len(pre)}{treestr1.ljust(8)}:    {node1.value}\n')
                    
    def _create_values_tree(self, row, param, prev):
        name = ' && '.join([f'{el}={row[el]}' for el in param if type(el) == str])
        fullname = ' | '.join([prev, name]) if prev else name
        l = param[-1] if type(param[-1]) == list else []
        return self.TimeSeries.ValuesTree(fullname, name, 1, children=[self._create_values_tree(row, l, fullname)] if l else [])

    def select_params(self, row):
        values = dict()
        for param in self.params:
            values[param] = self.TimeSeries.ValuesTree('', '', 1, children=[self._create_values_tree(row, self.params[param], '')])
        return row['datetime'], values
 
    def modify_node(self, node: TimeSeries, dt: datetime, values):
        for _values, _time_start in node.add_element(dt, values):
            # insert it to childs
            for child in node.children:
                self.modify_node(child, _time_start, _values)

    def aggregate(self, row):
        datetime, values = self.select_params(row)
        self.modify_node(self.tree, datetime, values)

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
                    if param[0] not in key:
                        break
                else:
                    # name of queue contain all requires params
                    # need check every node in tree
                    for values_tree_node in time_series_node.queue[key].descendants:
                        for param in params:
                            if f'{param[0]}={param[1]}' not in values_tree_node.fullname:
                                break
                        else:
                            result[time_series_node.name].add(values_tree_node.fullname, values_tree_node.value)
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
                        print(index, e)
                        return
                        #pass

                tree.print()

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
