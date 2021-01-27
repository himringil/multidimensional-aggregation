from sys import argv
from datetime import datetime, timedelta
from anytree import RenderTree

from agg import *
from agg_function import *
from agg_result import AggResult

class AggTree(AggTreeBase):

    class TimeSeries(AggTreeBase.TimeSeriesBase):
    
        def delete_zero_elements(self):
            for key in list(self.queue.keys()):
                if self._get_func(key) == 'count' and sum([el for el in self.queue[key] if el is not None]) == 0:
                    self.queue.pop(key)

        def add(self, dt: datetime, values):
    
            # start queue if it is empty
            if not self.time_start:
                for el in values:
                    self.queue[el] = [None] * self.q
                    self.queue[el][-1] = values[el]
                self.time_start = dt - self.time_range + self.time_delta
                return
    
            if dt < self.time_start + self.time_range - self.time_delta:
                raise ValueError(f'{dt} < {self.time_start + self.time_range - self.time_delta}: ts={self.time_start} tr={self.time_range} td={self.time_delta}')
    
            # pop elements from queue and insert into childs while new element time not reached
            while not self.time_start + self.time_range - self.time_delta <= dt < self.time_start + self.time_range:
                old_values = dict()
                for el in self.queue:
                    value = self.queue[el].pop(0)
                    if value:
                        old_values[el] = value
                    self.queue[el].append(0 if self._get_func(el) == 'count' else None)

                for child in self.children:
                    child.add(self.time_start, old_values)
                self.time_start += self.time_delta
    
            # new element belongs to last element of queue
            for el in values:
                if not self.queue.get(el):
                    self.queue[el] = [0 if self._get_func(el) == 'count' else None] * self.q
                self.queue[el][-1] = self._new_value(el, self.queue[el][-1], values[el])
                    
    
    def __init__(self, tree: dict, params: list):
        super().__init__(tree, params)
        if not self._correct_params(params):
            raise Exception('Bad parameters format')
        self.params = params

    def _correct_params_count(self, params):
        if not type(params) == list:
            return False
        for param in params:
            if not type(param) == list:
                return False
            for el in param:
                if not type(el) == str:
                    return False
        return True

    def print(self):
        for pre, _, node in RenderTree(self.tree):
            treestr = u"%s%s" % (pre, node.name)
            print(f'{treestr.ljust(8)}: ts={node.time_start} tr={node.time_range} td={node.time_delta}')
            for el in sorted(node.queue):
                print(f'{" " * len(pre)}{el}: {node.queue[el]}')

    def select_params(self, row):
        values = dict()
        for key in self.params.keys():
            if key == 'count':
                for param in self.params[key]:
                    k = 'count : ' + ' & '.join([f'{el}={row[el]}' for el in sorted(param)])
                    values[k] = 1
            elif key in [ 'min', 'max', 'sum' ]:
                for param in self.params[key]:
                    k = f'{key} : ' + ' & '.join([f'{el}' for el in sorted(param)])
                    l = [row[el] for el in sorted(param)]
                    values[k] = self._get_val(key, l)
        return row['datetime'], values
    
    def aggregate(self, row):
        datetime, values = self.select_params(row)
        self.tree.add(datetime, values)
    
    def _gen_relatives(self, relatives):
        for rel in relatives:
            for sub_lst in rel[0]:
                sub_lst_params = sub_lst.split(' & ')
                for lst in rel[1]:
                    lst_params = lst.split(' & ')
                    if self._is_sublist(sub_lst_params, lst_params):
                        yield f'{lst} / {sub_lst}', [format(l/subl if subl else 0, '.3f') for subl, l in zip(rel[0][sub_lst], rel[1][lst])]

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
            rel_result = []
            for i in relative:
                rel_result.append([dict(), dict()])

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
                    for j, p in enumerate(param):
                        p_lst = p.split(' & ')
                        if len(keys) == len(p_lst) and sorted(keys) == sorted(p_lst):
                            rel_result[i][j][key] = time_series_node.queue[key]
                            break

            for key, value in self._gen_relatives(rel_result):
                result[time_series_node.name].add(key, value)

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
