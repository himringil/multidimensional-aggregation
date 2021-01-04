import datetime
from random import randint
import pandas as pd

import agg0, agg3
from pympler import asizeof

def generate(params_cnt: int = 10, values_cnt: int = 50):
    result = dict()
    for p in range(1, params_cnt + 1):
        v_rand = randint(1, values_cnt)
        result[f'p{p}'] = v_rand
    return result

def test():

    time_range = 500

    tree_conf = {
     "name":"1second",
     "range":f"{time_range}s",
     "delta":"1s"
     }
    
    cnt_tests = 5
    cnt_params = 12

    params_conf = []
    params_rel = []

    for n in range(2, cnt_params):
        conf = [f'p{i}' for i in range(1, n + 1)]
        params_conf.append(conf + [conf])
        confs = ' & '.join(conf)
        rel = [[f'p{i}', confs] for i in range(1, n + 1)]
        params_rel.append(rel)

    tree0 = [agg0.AggTree(tree_conf, params) for params in params_conf]
    tree3 = [agg3.AggTree(tree_conf, params) for params in params_conf]

    dt = pd.to_datetime('19Dec2020 19:00:00', errors='coerce', format='%d%b%Y %H:%M:%S')
    packet = dict()

    for i in range(time_range + 1):

        print(f'{datetime.datetime.now()} -> {i}')

        packets_per_second = randint(200, 300)

        for j in range(packets_per_second):

            packet = generate(cnt_params)
            packet['datetime'] = dt

            #for tree in tree0:
            #    tree.aggregate(packet)

            for tree in tree3:
                tree.aggregate(packet)

        dt += datetime.timedelta(seconds=1)

    # filter relative
    f = open('test_result.txt', 'w')

    #for i, (tree, rel) in enumerate(zip(tree0, params_rel)):
    #    rel_time = datetime.timedelta(0)
    #    for _ in range(cnt_tests):
    #        tm = datetime.datetime.now()
    #        tree.filter(['1second'],
    #                    relative=rel)
    #        rel_time += (datetime.datetime.now() - tm)
    #    rel_time /= cnt_tests
    #    rel_time = rel_time.total_seconds() * 1000
    #    print(f'    {i+2}: {len(tree.tree.queue)}')
    #    print(f'agg0, {i+2}, rel_time, {rel_time}')
    #    f.write(f'agg0,{i+2},rel_time,{rel_time}\n')
    #    f.flush()

    for i, (tree, rel) in enumerate(zip(tree3, params_rel)):
        rel_time = datetime.timedelta(0)
        for _ in range(cnt_tests):
            tm = datetime.datetime.now()
            tree.filter(['1second'],
                        relative=rel)
            rel_time += (datetime.datetime.now() - tm)
        rel_time /= cnt_tests
        rel_time = rel_time.total_seconds() * 1000
        print(f'agg3, {i+2}, rel_time, {rel_time}')
        f.write(f'agg3,{i+2},rel_time,{rel_time}\n')
        f.flush()
    
    f.close()

if __name__ == '__main__':

    test()