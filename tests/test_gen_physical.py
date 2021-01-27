import pandas as pd
from datetime import datetime, timedelta
from random import randint
from os import walk
from os.path import join

import agg, agg0, agg1, agg2, agg3

def generate(params_cnt, values_cnt):
    result = dict()
    for p in range(1, params_cnt + 1):
        v_rand = randint(1, values_cnt)
        result[f'p{p}'] = v_rand
    return result

if __name__ == '__main__':

    time_range = 10

    tree_conf = {
     "name":"node 1",
     "range":f"{time_range}s",
     "delta":"1s"
     }

    cnt_params = 12

    params_conf0 = {
        'count' : [['p1'], ['p2'], ['p4'],
                   ['p1', 'p2', 'p3'],
                   ['p4', 'p5']],
        'min' : [['p6'], ['p7'], ['p6', 'p7']],
        'max' : [['p6'], ['p8'], ['p6', 'p8']],
        'sum' : [['p6'], ['p9'], ['p6', 'p9']]
        }

    params_conf1 = {
        'count' : [['p1', [ 'p2', 'p3' ]],
                   ['p2', [ 'p1', 'p3' ]],
                   ['p4', [ 'p5' ]],
                  ],
        'min' : [['p6', [ 'p7' ]],
                 ['p7']],
        'max' : [['p6', 'p7', [ 'p8' ]],
                 ['p8']],
        'sum' : [['p6', [ 'p9' ]],
                 ['p9']]
        }
        
    trees = [ agg0.AggTree(tree_conf, params_conf0),
              agg1.AggTree(tree_conf, params_conf1),
              agg2.AggTree(tree_conf, params_conf1),
              agg3.AggTree(tree_conf, params_conf0)
            ]

    dt = pd.to_datetime('19Dec2020 19:00:00', errors='coerce', format='%d%b%Y %H:%M:%S')
    packet = dict()

    for i in range(time_range + 1):

        print(f'{datetime.now()} -> {i}')
        
        packets_per_second = randint(5, 10)
        
        for j in range(packets_per_second):
        
            packet = generate(cnt_params, 3)
            packet['datetime'] = dt

            for tree in trees:
                tree.aggregate(packet)
        
        dt += timedelta(seconds=1)
    
    for tree in trees:
        tree.print()
        print()
        print('--------------------------------------------------------------------------------------------------------------------------------')
        print()
