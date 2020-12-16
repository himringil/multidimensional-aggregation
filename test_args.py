from sys import argv
import datetime

import agg0, agg1, agg2, agg3
from pympler import asizeof

def test(time_range):

    tree_conf = {
     "name":"1second",
     "range":"1s",
     "delta":"1s"
     }
    
    params_conf = [ [ "src",
                      "dst",
                      [ "src", "dst" ]
                    ],
                    [ [ "src", [ "dst" ]],
                      [ "dst", [ "src" ]]
                    ]
                  ]

    data_path = 'data'
    
    ranges = [ 10, 50, 100, 500, 1000, 5000, 10000, 15000, 20000, 25000, 30000, 35000, 38000 ]
    n_files = [ 1,  1,   1,   1,    1,    4,     8,    11,    15,    19,    22,    26,    28 ]

    ranges = dict(zip(ranges, n_files))
    n = ranges[time_range]

    cnt_tests = 3

    result = dict.fromkeys(ranges,
                           dict.fromkeys([agg.__name__ for agg in [agg0, agg1, agg2, agg3]],
                                         dict.fromkeys(['sum_size', 'avg_size', 'abs_time', 'rel_time'],
                                                       None
                                                      )
                                        )
                          )
    f = open('test_result.txt', 'w')

    tree_conf['range'] = f'{time_range}s'

    for agg, i in zip([agg0, agg1, agg2, agg3], [ 0, 1, 1, 0 ]):

        print(f'{datetime.datetime.now()} -> {agg.__name__} (time_range={time_range})')
        cnt = 0
        for tree in agg.aggregate(tree_conf, params_conf[i], data_path):

            cnt += 1
            if cnt == n:

                # sum size of queues
                result[time_range][agg.__name__]['sum_size'] = asizeof.asizeof(tree.tree.queue) + (asizeof.asizeof(tree.tree.graph) if agg.__name__ == agg3.__name__ else 0)
                print(f'{time_range}, {agg.__name__}, sum_size, {result[time_range][agg.__name__]["sum_size"]}')
                f.write(f'{time_range}, {agg.__name__}, sum_size, {result[time_range][agg.__name__]["sum_size"]}\n')
                f.flush()
                
                # average size of queue
                result[time_range][agg.__name__]['avg_size'] = int(result[time_range][agg.__name__]['sum_size'] / len(tree.tree.queue))
                print(f'{time_range}, {agg.__name__}, avg_size, {result[time_range][agg.__name__]["avg_size"]}')
                f.write(f'{time_range}, {agg.__name__}, avg_size, {result[time_range][agg.__name__]["avg_size"]}\n')
                f.flush()

                # filter absolute
                result[time_range][agg.__name__]['abs_time'] = datetime.timedelta(0)
                for _ in range(cnt_tests):
                    tm = datetime.datetime.now()
                    tree.filter(['1second'],
                                absolute=[['src', ''], ['dst', '']])
                    result[time_range][agg.__name__]['abs_time'] += (datetime.datetime.now() - tm)
                result[time_range][agg.__name__]['abs_time'] /= cnt_tests
                print(f'{time_range}, {agg.__name__}, abs_time, {result[time_range][agg.__name__]["abs_time"]}')
                f.write(f'{time_range}, {agg.__name__}, abs_time, {result[time_range][agg.__name__]["abs_time"]}\n')
                f.flush()

                # filter relative
                result[time_range][agg.__name__]['rel_time'] = datetime.timedelta(0)
                for _ in range(cnt_tests):
                    tm = datetime.datetime.now()
                    tree.filter(['1second'],
                                relative=[['src', 'src & dst'],
                                          ['dst', 'src & dst']
                                         ])
                    result[time_range][agg.__name__]['rel_time'] += (datetime.datetime.now() - tm)
                result[time_range][agg.__name__]['rel_time'] /= cnt_tests
                print(f'{time_range}, {agg.__name__}, rel_time, {result[time_range][agg.__name__]["rel_time"]}')
                f.write(f'{time_range}, {agg.__name__}, rel_time, {result[time_range][agg.__name__]["rel_time"]}\n')
                f.flush()

                break

        print('')

    f.close()

if __name__ == '__main__':

    test(int(argv[1]))
