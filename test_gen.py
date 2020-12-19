import datetime
from random import randint

import agg0, agg3
from pympler import asizeof

def generate(params_cnt: int, values_cnt: int = 100):
    result = dict()
    for p in range(params_cnt):
        p_rand = randint(1, params_cnt)
        v_rand = randint(1, values_cnt)
        result[f'p{p_rand}'] = v_rand
    return result

def test():

    tree_conf = {
     "name":"1second",
     "range":"10000s",
     "delta":"1s"
     }
    
    cnt_tests = 5
    cnt_params = 10

    params_conf = []
    params_rel = []

    for n in range(2, cnt_params + 1):
        conf = [f'p{i}' for i in range(1, n + 1)]
        params_conf.append(conf + [conf])
        confs = ' & '.join(conf)
        for i in range(1, n + 1):
            rel = [[f'p{i}', confs]]
            params_rel.append(rel)

    for n in range(2, cnt_params + 1):
        print(params_conf[n])

    for n in range(2, cnt_params + 1):
        print(params_rel[n])

    return

    f = open('res4.csv', 'w')

    for agg in [agg0, agg3]:
    
        tree = AggTree(tree_conf, params_conf)

        print(f'{datetime.datetime.now()} -> {agg.__name__} (conf)')
        agg_time = datetime.timedelta(0)
    
        for cnt, (tree, td) in enumerate(agg.aggregate(tree_conf, conf, data_path)):
    
            agg_time += td
    
            if cnt + 1 == n_files:
                
                # time to aggregate 100000 elements
                agg_time = int(agg_time.total_seconds() * 1000)  # ms
                print(f'{agg.__name__}, agg_time, {agg_time}')
                f.write(f'{agg.__name__},agg_time,{agg_time}\n')
                f.flush()
    
                # filter relative
                rel_time = datetime.timedelta(0)
                for _ in range(cnt_tests):
                    tm = datetime.datetime.now()
                    tree.filter(['1second'],
                                relative=[['src', 'src & dst'],
                                          ['dst', 'src & dst']
                                         ])
                    rel_time += (datetime.datetime.now() - tm)
                rel_time /= cnt_tests
                rel_time = rel_time.total_seconds() * 1000
                print(f'{agg.__name__}, rel_time, {rel_time}')
                f.write(f'{agg.__name__},rel_time,{rel_time}\n')
                f.flush()
    
                print('')
    
                break

    f.close()

if __name__ == '__main__':

    test()