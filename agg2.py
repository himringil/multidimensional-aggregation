from sys import argv
from datetime import datetime, timedelta
from anytree import NodeMixin, RenderTree, LevelOrderGroupIter

from agg import *
from agg_function import *
from agg_result import AggResult

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

        def _delete_zero_elements(self, node):
            for child in node.children:
                if sum([el for el in child.value if el is not None]) == 0:
                    child.parent = None
                else:
                    self._delete_zero_elements(child)

        def delete_zero_elements(self):
            for el in self.queue:
                f = self._get_func(el)
                if f == 'count':
                    if sum([el for el in self.queue[el].value if el is not None]) == 0:
                        self.queue[el] = self.ValuesNode(f, f, [0] * self.q)
                    else:
                        self._delete_zero_elements(self.queue[el])

        def _oldest_values_to_values_tree(self, el):
            el.value.append(0 if self._get_func(el.name) == 'count' else None)
            return self.ValuesNode(el.fullname, el.name, el.value.pop(0), children=[self._oldest_values_to_values_tree(c) for c in el.children])

        def _merge_trees(self, nodes_to, node_from):
            for node_to in nodes_to:
                if node_to.name == node_from.name:
                    node_to.value[-1] = self._new_value(node_to.name, node_to.value[-1], node_from.value)
                    for child_from in node_from.children:
                        if not self._merge_trees(node_to.children, child_from):
                            value = child_from.value
                            child_from.value = [0 if self._get_func(child_from.name) == 'count' else None] * self.q
                            child_from.value[-1] = self._new_value(child_from.name, child_from.value[-1], value)
                            child_from.parent = node_to
                            for descendant in child_from.descendants:
                                value = descendant.value
                                descendant.value = [0 if self._get_func(descendant.name) == 'count' else None] * self.q
                                descendant.value[-1] = self._new_value(descendant.name, descendant.value[-1], value)
                    return True
            return False

        def add(self, dt: datetime, values):

            # start queue if it is empty
            if not self.time_start:
                for el in values:
                    f = self._get_func(el)
                    self.queue[el] = self.ValuesNode(f'{f}', f'{f}', [None] * self.q)
                    self._merge_trees([self.queue[el]], values[el])
                self.time_start = dt - self.time_range + self.time_delta
                return
    
            if dt < self.time_start + self.time_range - self.time_delta:
                raise ValueError(f'{dt} < {self.time_start + self.time_range - self.time_delta}: ts={self.time_start} tr={self.time_range} td={self.time_delta}')
    
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
                    f = self._get_func(el)
                    self.queue[el] = self.ValuesNode(f'{f}', f'{f}', [0 if f == 'count' else None] * self.q)
                self._merge_trees([self.queue[el]], values[el])

    def __init__(self, tree: dict, params: list):
        super().__init__(tree, params)
        if not self._correct_params(params):
            raise Exception('Bad parameters format')
        self.params = dict()
        for key in params.keys():
            self.params[key] = dict()
            for param in params[key]:
                self.params[key][self._get_full_name(param)] = param

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

    def _correct_params_count(self, params):
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

    def print(self):
        for pre, _, node in RenderTree(self.tree):
            treestr = u"%s%s" % (pre, node.name)
            print(f'{treestr.ljust(8)}: ts={node.time_start} tr={node.time_range} td={node.time_delta}\n')
            for el in sorted(node.queue):
                print(f'{" " * len(pre)}{el}:\n')
                for pre1, _, node1 in RenderTree(node.queue[el]):
                    treestr1 = u"%s%s" % (pre1, node1.name)
                    print(f'{" " * len(pre)}{treestr1.ljust(8)}:    {node1.value}\n')

    def _create_count_values_tree(self, row, param, prev):

        if not param:
            return []

        str_params = [el for el in param if type(el) == str]
        lst_param = param[-1] if type(param[-1]) == list else []

        name = ' && '.join([f'{el}={row[el]}' for el in sorted(str_params)])
        fullname = ' | '.join([prev, name]) if prev else name

        return [self.TimeSeries.ValuesNode(f'count : {fullname}', f'count : {name}', 1, children=self._create_count_values_tree(row, lst_param, fullname))]

    def _create_values_tree(self, row, param, prev, func, prev_val):

        if not param:
            return []

        str_params = [el for el in param if type(el) == str]
        lst_param = param[-1] if type(param[-1]) == list else []

        name = ' && '.join([f'{el}' for el in sorted(str_params)])
        fullname = ' | '.join([prev, name]) if prev else name

        l = [row[el] for el in sorted(str_params)]
        val = self._new_value(func, prev_val, self._get_val(func, l))

        return [self.TimeSeries.ValuesNode(f'{func} : {fullname}', f'{func} : {name}', val, children=self._create_values_tree(row, lst_param, fullname, func, val))]

    def select_params(self, row):
        values = dict()
        for key in self.params.keys():
            if key == 'count':
                for param in self.params[key]:
                    values[f'{key} : {param}'] = self.TimeSeries.ValuesNode(f'{key}', f'{key}', 1,
                                                                            children=self._create_count_values_tree(row, self.params[key][param], ''))
            else:
                for param in self.params[key]:
                    str_params = []
                    for el in param.split(' | '):
                        str_params += el.split(' && ')
                    l = [row[el] for el in sorted(str_params)]
                    values[f'{key} : {param}'] = self.TimeSeries.ValuesNode(f'{key}', f'{key}', self._get_val(key, l),
                                                                            children=self._create_values_tree(row, self.params[key][param], '', key, None))
        return row['datetime'], values

    def aggregate(self, row):
        datetime, values = self.select_params(row)
        self.tree.add(datetime, values)
    
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
