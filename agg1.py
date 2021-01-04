from sys import argv
from datetime import datetime, timedelta
from pytimeparse.timeparse import timeparse
from anytree import NodeMixin, RenderTree, LevelOrderGroupIter

from agg import *
from agg_result import AggResult

import time

class AggTree(AggTreeBase):

    class TimeSeries(AggTreeBase.TimeSeriesBase):
    
        class ValuesNode(NodeMixin):
             
            def __init__(self, fullname, name, value, parent=None, children=None):
    
                self.fullname = fullname
                self.name = name
                self.value = value

                self.parent = parent
                if children:
                    self.children = children

        def delete_zero_elements(self):
            for key in list(self.queue.keys()):
                if sum([el.value for el in self.queue[key]]) == 0:
                    self.queue[key] = [self.ValuesNode('', '', 0) for i in range(self.q)]
    
        def _merge_trees(self, nodes_to, node_from):
            for node_to in nodes_to:
                if node_to.name == node_from.name:
                    node_to.value += node_from.value
                    for child_from in node_from.children:
                        if not self._merge_trees(node_to.children, child_from):
                            child_from.parent = node_to
                    return True
            return False

        def add(self, dt: datetime, values):

            # start queue if it is empty
            if not self.time_start:
                for el in values:
                    self.queue[el] = [self.ValuesNode('', '', 0) for i in range(self.q)]
                    #self.queue[el] = [self.ValuesTree('', '', 0)] * self.q
                    self.queue[el][-1] = values[el]
                self.time_start = dt - self.time_range + self.time_delta
                return
    
            if dt < self.time_start + self.time_range - self.time_delta:
                print(dt, self.time_start, self.time_range, self.time_delta, self.time_start + self.time_range - self.time_delta)
                raise ValueError
    
            # pop elements from queue and insert into childs while new element time not reached
            while not self.time_start + self.time_range - self.time_delta <= dt < self.time_start + self.time_range:
                old_values = dict()
                for el in self.queue:
                    value = self.queue[el].pop(0)
                    if value.value:
                        old_values[el] = value
                    self.queue[el].append(self.ValuesNode('', '', 0))
                for child in self.children:
                    child.add(self.time_start, old_values)
                self.time_start += self.time_delta
    
            # new element belongs to last element of queue
            for el in values:
                if not self.queue.get(el):
                    self.queue[el] = [self.ValuesNode('', '', 0) for i in range(self.q)]
                    #self.queue[el] = [self.ValuesTree('', '', 0)] * self.q
                self._merge_trees([self.queue[el][-1]], values[el])
    

    def __init__(self, tree: dict, params: list):
        super().__init__(tree, params)
        if not self._correct_params(params):
            raise Exception('Bad parameters format')
        self.params = dict()
        for param in params:
            self.params[self._get_full_name(param)] = param

    def _check_param(self, param):
        if not type(param) == list or len(param) == 0:
            return False
        str_params = [el for el in param if type(el) == str]
        lst_param = param[-1] if type(param[-1]) == list else None
        if len(str_params) == 0 or not len(str_params) + (0 if lst_param is None else 1) == len(param):
            return False
        if not lst_param is None:
            return self._check_param(lst_param)
        return True

    def _correct_params(self, params):
        if not type(params) == list:
            return False
        for param in params:
            if not self._check_param(param):
                return False
        return True

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
                for i in node.queue[el]:
                    for pre1, _, node1 in RenderTree(i):
                        treestr1 = u"%s%s" % (pre1, node1.name)
                        print(f'{" " * len(pre)}{treestr1.ljust(8)}:    {node1.value}\n')

    def _create_values_tree(self, row, param, prev):
        name = ' && '.join([f'{el}={row[el]}' for el in param if type(el) == str])
        fullname = ' | '.join([prev, name]) if prev else name
        l = param[-1] if type(param[-1]) == list else []
        return self.TimeSeries.ValuesNode(fullname, name, 1, children=[self._create_values_tree(row, l, fullname)] if l else [])

    def select_params(self, row):
        values = dict()
        for param in self.params:
            values[param] = self.TimeSeries.ValuesNode('', '', 1, children=[self._create_values_tree(row, self.params[param], '')])
        return row['datetime'], values

    def aggregate(self, row):
        datetime, values = self.select_params(row)
        self.tree.add(datetime, values)
    
    def _gen_relatives(self, param, key, trees):

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

        for i, tree in enumerate(trees):

            # get all tree nodes on sublists level
            sub_lsts = list(LevelOrderGroupIter(tree, maxlevel=lvl_sub_lst + 1))[-1]
            
            for sub_lst in sub_lsts:
                # get all tree nodes on lists level
                lsts = list(LevelOrderGroupIter(sub_lst, maxlevel=lvl_lst - lvl_sub_lst + 1))[-1]

                for lst in lsts:
                    # create empty list if it not exists
                    if f'{lst.fullname} / {sub_lst.fullname}' not in result.keys():
                        result[f'{lst.fullname} / {sub_lst.fullname}'] = []
                    # add zero elements to list if it contain gaps
                    if len(result[f'{lst.fullname} / {sub_lst.fullname}']) < i:
                        result[f'{lst.fullname} / {sub_lst.fullname}'] += [0] * (i - len(result[f'{lst.fullname} / {sub_lst.fullname}']))
                    # add new element to list
                    result[f'{lst.fullname} / {sub_lst.fullname}'].append(format(lst.value/sub_lst.value if sub_lst.value else 0, '.3f'))

        for el in result:
            # add zero elements to list if it not full
            if len(result[el]) < len(trees):
                result[el] += [0] * (len(trees) - len(result[el]))
            assert len(result[el]) == len(trees)

        return result

    def filter(self, timeseries_name: list = [], absolute: list = [], relative: list = []):
        
        result = AggResult()
        
        # delete bad relatives
        relative = [[param[0], param[1]] for param in relative if self._is_sublist(param[0].split(' & '), param[1].split(' & '))]

        time_series_nodes = [self.tree]
        while time_series_nodes:
            time_series_node = time_series_nodes.pop(0)
            for child in time_series_node.children:
                time_series_nodes.append(child)
            
            # filter time series
            if len(timeseries_name) > 0 and time_series_node.name not in timeseries_name:
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
                        # need check every tree in queue
                        queues = dict()
                        for i, cur_tree in enumerate(time_series_node.queue[key]):
                            for values_tree_node in cur_tree.descendants:
                                for param in absolute:
                                    if f'{param[0]}={param[1]}' not in values_tree_node.fullname:
                                        break
                                else:
                                    # create empty list if it not exists
                                    if values_tree_node.fullname not in queues.keys():
                                        queues[values_tree_node.fullname] = []
                                    # add zero elements to list if it contain gaps
                                    if len(queues[values_tree_node.fullname]) < i:
                                        queues[values_tree_node.fullname] += [0] * (i - len(queues[values_tree_node.fullname]))
                                    # add new element to list
                                    queues[values_tree_node.fullname].append(values_tree_node.value)

                        for el in queues:
                            # add zero elements to list if it not full
                            if len(queues[el]) < len(time_series_node.queue[key]):
                                queues[el] += [0] * (len(time_series_node.queue[key]) - len(queues[el]))
                            assert len(queues[el]) == len(time_series_node.queue[key])
                            result[time_series_node.name].add(el, queues[el])

                for param in relative:
                    rel_result = self._gen_relatives(param, key, time_series_node.queue[key])
                    for k in rel_result:
                        result[time_series_node.name].add(k, rel_result[k])

        return result


if __name__ == '__main__':

    if len(argv) < 4:
        raise Exception('args: <tree_config_path> <params_config_path> <data_folder_path>')

    tree = AggTree(load_tree(argv[1]), load_params(argv[2]))

    for tree, td in aggregate(tree, argv[3]):
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
