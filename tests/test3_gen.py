from datetime import datetime, timedelta
from random import randint
import pandas as pd

import agg, agg0, agg3

def generate(params_cnt, values_cnt):
    result = dict()
    for p in range(1, params_cnt + 1):
        v_rand = randint(1, values_cnt)
        result[f'p{p}'] = v_rand
    return result

def test(uniq_params):

    time_range = 100

    tree_conf = {
     "name":"1second",
     "range":f"{time_range}s",
     "delta":"1s"
     }
    
    cnt_tests = 5
    cnt_params = 12

    params_conf = []
    params_rel = []

    for n in range(2, cnt_params + 1):
        conf = [[f'p{i}'] for i in range(1, n + 1)]
        params_conf.append({'count' : conf + [[c[0] for c in conf]]})
        confs = ' & '.join([c[0] for c in conf])
        rel = [[f'p{i}', confs] for i in range(1, n + 1)]
        params_rel.append(rel)

    tree0 = [agg0.AggTree(tree_conf, params) for params in params_conf]
    tree3 = [agg3.AggTree(tree_conf, params) for params in params_conf]

    time0 = [timedelta(0)] * len(params_conf)
    time3 = [timedelta(0)] * len(params_conf)

    dt = pd.to_datetime('19Dec2020 19:00:00', errors='coerce', format='%d%b%Y %H:%M:%S')
    packet = dict()

    cnt_packets = 0

    for i in range(time_range + 1):

        print(f'{datetime.now()} -> {i}')

        packets_per_second = randint(200, 300)
        cnt_packets += packets_per_second

        for j in range(packets_per_second):

            packet = generate(cnt_params, uniq_params)
            packet['datetime'] = dt

            for i, tree in enumerate(tree0):
                ts = datetime.now()
                tree.aggregate(packet)
                time0[i] += (datetime.now() - ts)

            for i, tree in enumerate(tree3):
                ts = datetime.now()
                tree.aggregate(packet)
                time3[i] += (datetime.now() - ts)

        dt += timedelta(seconds=1)

    print(f'Packets: {cnt_packets}')
    for i, (t0, t3) in enumerate(zip(time0, time3)):
        print(f'{i+2}: {format(t0.total_seconds() * 1000 / cnt_packets * 10000, ".3f")} - {format(t3.total_seconds() * 1000 / cnt_packets * 10000, ".3f")}')
    
    return

    f = open(f'results/test3-{uniq_params}.csv', 'w')

    # filter relative

    for i, (tree, rel) in enumerate(zip(tree3, params_rel)):
        rel_time = timedelta(0)
        for _ in range(cnt_tests):
            tm = datetime.now()
            tree.filter(['1second'], relative=rel)
            rel_time += (datetime.now() - tm)
        rel_time /= cnt_tests
        rel_time = rel_time.total_seconds() * 1000
        
        print(f'agg3,{i+2},cnt_queu,{len(tree.tree.queue)}')
        f.write(f'agg3,{i+2},cnt_queu,{len(tree.tree.queue)}\n')
        
        sum_size = tree.size()
        print(f'agg3,{i+2},sum_size,{sum_size}')
        f.write(f'agg3,{i+2},sum_size,{sum_size}\n')
        
        print(f'agg3,{i+2},rel_time,{rel_time}')
        f.write(f'agg3,{i+2},rel_time,{rel_time}\n')
        
        f.flush()

    for i, (tree, rel) in enumerate(zip(tree0, params_rel)):
        rel_time = timedelta(0)
        for _ in range(cnt_tests):
            tm = datetime.now()
            tree.filter(['1second'], relative=rel)
            rel_time += (datetime.now() - tm)
        rel_time /= cnt_tests
        rel_time = rel_time.total_seconds() * 1000

        print(f'agg0,{i+2},cnt_queu,{len(tree.tree.queue)}')
        f.write(f'agg0,{i+2},cnt_queu,{len(tree.tree.queue)}\n')
        
        sum_size = tree.size()
        print(f'agg0,{i+2},sum_size,{sum_size}')
        f.write(f'agg0,{i+2},sum_size,{sum_size}\n')

        print(f'agg0,{i+2},rel_time,{rel_time}')
        f.write(f'agg0,{i+2},rel_time,{rel_time}\n')
        
        f.flush()
    
    f.close()

if __name__ == '__main__':

    for i in [50]:
        test(i)
