import pandas as pd
from sys import argv
from datetime import datetime, timedelta
from os import walk
from os.path import join

from agg import *
from agg_result import AggResult

class AggTree():

    class TimeSeries():

        def __init__(self, name, time_range: timedelta, time_delta: timedelta):
    
            self.q, r = divmod(time_range, time_delta)
            if time_range < time_delta or r:
                raise ValueError
    
            self.name = name
            
            self.time_range = time_range
            self.time_delta = time_delta
            self.time_start = None
    
            self.queue = dict()
    
        def delete_zero_elements(self):
            for key in list(self.queue.keys()):
                if sum(self.queue[key]) == 0:
                    self.queue.pop(key)

        def add(self, dt: datetime, values):
    
            # start queue if it is empty
            if not self.time_start:
                for el in values:
                    self.queue[el] = [0] * self.q
                    self.queue[el][-1] = values[el]
                self.time_start = dt - self.time_range + self.time_delta
                return
    
            if dt < self.time_start + self.time_range - self.time_delta:
                raise ValueError(f'{dt} < {self.time_start + self.time_range - self.time_delta}: ts={self.time_start} tr={self.time_range} td={self.time_delta}')
    
            # pop elements from queue while new element time not reached
            while not self.time_start + self.time_range - self.time_delta <= dt < self.time_start + self.time_range:
                for el in self.queue:
                    self.queue[el].pop(0)
                    self.queue[el].append(0)
                self.time_start += self.time_delta
    
            # new element belongs to last element of queue
            for el in values:
                if not self.queue.get(el):
                    self.queue[el] = [0] * self.q
                self.queue[el][-1] += values[el]
    
    def __init__(self, tree: dict, params: list):

        if not tree:
            raise Exception('Bad json format')

        if not self._correct_params(params):
            raise Exception('Bad parameters format')
        
        self.params = params

        self.tree = list()

        nodes = [tree]

        while nodes:
            cur = nodes.pop(0)

            if not cur.get('name', None) or not cur.get('range', None) or not cur.get('delta', None):
                raise Exception('Bad json format')
            
            self.tree.append(self.TimeSeries(name=cur['name'],
                                             time_range=timedelta(seconds=timeparse(cur['range'])),
                                             time_delta=timedelta(seconds=timeparse(cur['delta']))))
            for c in cur.get('child', []):
                nodes.append(c)

    def _correct_params(self, params):
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
        for node in self.tree:
            print(f'{node.name}: ts={node.time_start} tr={node.time_range} td={node.time_delta}')
            for el in sorted(node.queue):
                print(f'    {el}: {node.queue[el]}')

    def delete_zero_elements(self):
        for tree in self.tree:
            tree.delete_zero_elements()

    def select_params(self, row):
        values = dict()
        for param in self.params:
            key = ' & '.join([f'{el}={row[el]}' for el in param])
            values[key] = 1
        return row['datetime'], values
    
    def aggregate(self, row):
        datetime, values = self.select_params(row)
        for tree in self.tree:
            tree.add(datetime, values)
    
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

        for time_series_node in self.tree:
            
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

    def size(self):
        return sum([asizeof.asizeof(tree.queue) for tree in self.tree])


def aggregate_folder(tree, data_path):
    
    for (_, _, filenames) in walk(data_path):
        for filename in sorted(filenames):
            if filename.endswith('.parquet'):

                filepath = join(data_path, filename)
                df = pd.read_parquet(filepath)
                df['datetime'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'], errors='coerce', format='%d%b%Y %H:%M:%S')
                df.drop(axis=1, columns=['date', 'time'], inplace=True)
                df.rename(columns={'i/f_name' : 'if_name', 'i/f_dir' : 'if_dir'}, inplace=True)

                tree = aggregate(tree, df)
                tree.delete_zero_elements()
                yield tree
        break

if __name__ == '__main__':

    if len(argv) < 4:
        raise Exception('args: <tree_config_path> <params_config_path> <data_folder_path>')

    tree = AggTree(load_tree(argv[1]), load_params(argv[2]))

    for tree in aggregate_folder(tree, argv[3]):
        tree.print()
    
        ts = ['10sec -> 1sec', '10min -> 1min', '5hour -> 30min']
    
        print('--------------------------------')
        tree.filter(ts, [['src', '192.168.1.10']]).print()
        print('--------------------------------')
        tree.filter(ts, [['service', '137']]).print()
        print('--------------------------------')
        tree.filter(ts, [['', '192.168.1.50'], ['service', '']]).print()
        print('--------------------------------')
        tree.filter(['10sec -> 1sec'],
                    absolute=[['src', ''], ['dst', '']],
                    relative=[['src', 'src & dst'],
                              ['dst', 'src & dst']]).print()
        
        break
