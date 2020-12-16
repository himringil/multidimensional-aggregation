from sys import argv
from os import walk
from os.path import join
from datetime import datetime, timedelta
from pytimeparse.timeparse import timeparse
from json import load

import pandas as pd
from anytree import NodeMixin, RenderTree, LevelOrderGroupIter

from AggResult import AggResult

import time

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

        def _delete_zero_elements(self, node):
            for child in node.children:
                if sum(child.value) == 0:
                    child.parent = None
                else:
                    self._delete_zero_elements(child)

        def add(self, dt: datetime, values):

            # start queue if it is empty
            if not self.time_start:
                for el in values:
                    self.queue[el] = self.ValuesTree('', '', [0] * self.q)
                    self._merge_trees([self.queue[el]], values[el])
                self.time_start = dt - self.time_range + self.time_delta
                return
    
            if dt < self.time_start + self.time_range - self.time_delta:
                print(dt, self.time_start, self.time_range, self.time_delta, self.time_start + self.time_range - self.time_delta)
                raise ValueError
    
            # pop elements from queue and insert into childs while new element time not reached
            while not self.time_start + self.time_range - self.time_delta <= dt < self.time_start + self.time_range:
                old_values = dict()
                for el in self.queue:
                    old_values[el] = self._oldest_values_to_values_tree(self.queue[el])
                for child in self.children:
                    child.add(self.time_start, old_values)
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
                    self._delete_zero_elements(self.queue[el])

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

    def aggregate(self, row):
        datetime, values = self.select_params(row)
        self.tree.add(datetime, values)
    
    def _is_sublist(self, sub_lst, lst):
        return set(sub_lst) < set(lst)

    def _gen_relatives(self, param, key, root):

        sub_lst_params = param[0].split(' & ')
        lst_params = param[1].split(' & ')

        levels = key.split(' | ')
        params = []
        lvl_sub_lst = 0
        lvl_lst = 0

        result = dict()

        for level in levels:

            params += level.split(' & ')
            
            if not lvl_lst:
                # firstly find tree level which contain sublist
                lvl_sub_lst += 1
                if len(params) == len(sub_lst_params) and sorted(params) == sorted(sub_lst_params):
                    lvl_lst = lvl_sub_lst
                    continue 
                if len(params) >= len(sub_lst_params):
                    return result

            else:
                # secondly find tree level which contain list
                lvl_lst += 1
                if len(params) == len(lst_params) and sorted(params) == sorted(lst_params):
                    break
                if len(params) >= len(lst_params):
                    return result

        if lvl_sub_lst == lvl_lst:
            return result

        # get all tree nodes on sublists level
        sub_lsts = list(LevelOrderGroupIter(root, maxlevel=lvl_sub_lst + 1))[-1]

        for sub_lst in sub_lsts:
            # get all tree nodes on lists level
            lsts = list(LevelOrderGroupIter(sub_lst, maxlevel=lvl_lst - lvl_sub_lst + 1))[-1]
            for lst in lsts:
                result[f'{lst.fullname} / {sub_lst.fullname}'] = [format(l/subl if subl else 0, '.3f') for subl, l in zip(sub_lst.value, lst.value)]

        return result

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

            # filter queues
            for key in time_series_node.queue:
                
                if absolute:
                    # filter absolute
                    for param in absolute:
                        if param[0] not in key:
                            break
                    else:
                        # name of queue contain all required params
                        # need check every node in tree
                        for values_tree_node in time_series_node.queue[key].descendants:
                            for param in absolute:
                                if f'{param[0]}={param[1]}' not in values_tree_node.fullname:
                                    break
                            else:
                                result[time_series_node.name].add(values_tree_node.fullname, values_tree_node.value)

                for param in relative:
                    rel_result = self._gen_relatives(param, key, time_series_node.queue[key])
                    for k in rel_result:
                        result[time_series_node.name].add(k, rel_result[k])

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

                yield tree

if __name__ == '__main__':

    if len(argv) < 4:
        raise Exception('args: <tree_config_path> <params_config_path> <data_folder_path>')

    for i, tree in enumerate(aggregate(load_tree(argv[1]), load_params(argv[2]), argv[3])):
        tree.print()
        print('--------------------------------')
        if i == 2:
            break
