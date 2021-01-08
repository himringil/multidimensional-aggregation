import pandas as pd
from os import walk
from os.path import join
from datetime import datetime, timedelta

import agg, agg0, agg1, agg2, agg3

def aggregate(tree, data_path):
    
    for (_, _, filenames) in walk(data_path):
        for filename in sorted(filenames):
            if filename.endswith('.parquet'):

                filepath = join(data_path, filename)
                df = pd.read_parquet(filepath)
                df['datetime'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'], errors='coerce', format='%d%b%Y %H:%M:%S')
                df.drop(axis=1, columns=['date', 'time'], inplace=True)
                df.rename(columns={'i/f_name' : 'if_name', 'i/f_dir' : 'if_dir'}, inplace=True)

                print(f'   {datetime.now()} -> {filename} ({len(df.index)} rows)')
                ts = datetime.now()
                agg.aggregate(tree, df)
                yield tree, datetime.now() - ts
        break

def test():

    tree_conf = {
     "name":"1second",
     "range":"1s",
     "delta":"1s"
     }
    
    params_conf = [ [ [ "src" ],
                      [ "dst" ],
                      [ "src", "dst" ]
                    ],
                    [ [ "src", [ "dst" ]],
                      [ "dst", [ "src" ]]
                    ]
                  ]

    data_path = 'data_example'
    
    ranges = [ 500, 1000, 5000, 10000, 15000, 20000, 25000, 30000 ]
    n_files = [  1,    1,    4,     8,    11,    15,    19,    22 ]

    cnt_tests = 3

    f = open('results/test1.csv', 'w')

    for time_range, n in zip(ranges, n_files):

        tree_conf['range'] = f'{time_range}s'

        for aggN, i in zip([agg0, agg1, agg2, agg3], [ 0, 1, 1, 0 ]):

            print(f'{datetime.now()} -> {aggN.__name__} (time_range={time_range}, {n} files)')

            tree = aggN.AggTree(tree_conf, params_conf[i])

            agg_time = timedelta(0)

            for cnt, (tree, td) in enumerate(aggregate(tree, data_path)):

                agg_time += td

                tree.delete_zero_elements()

                if cnt + 1 == n:

                    # count of queues
                    print(f'{time_range},{aggN.__name__},cnt_queu,{len(tree.tree.queue)}')
                    f.write(f'{time_range},{aggN.__name__},cnt_queu,{len(tree.tree.queue)}\n')
                    f.flush()

                    # time to aggregate 100000 elements
                    agg_time /= (5 * n)
                    agg_time = int(agg_time.total_seconds() * 1000)  # ms
                    print(f'{time_range},{aggN.__name__},agg_time,{agg_time}')
                    f.write(f'{time_range},{aggN.__name__},agg_time,{agg_time}\n')
                    f.flush()

                    # sum size of queues
                    sum_size = tree.size()
                    print(f'{time_range},{aggN.__name__},sum_size,{sum_size}')
                    f.write(f'{time_range},{aggN.__name__},sum_size,{sum_size}\n')
                    f.flush()
                    
                    # average size of queue
                    avg_size = int(sum_size / len(tree.tree.queue))
                    print(f'{time_range},{aggN.__name__},avg_size,{avg_size}')
                    f.write(f'{time_range},{aggN.__name__},avg_size,{avg_size}\n')
                    f.flush()

                    # filter absolute
                    abs_time = timedelta(0)
                    for _ in range(cnt_tests):
                        tm = datetime.now()
                        tree.filter(['1second'],
                                    absolute=[['src', ''], ['dst', '']])
                        abs_time += (datetime.now() - tm)
                    abs_time /= cnt_tests
                    abs_time = abs_time.total_seconds() * 1000
                    print(f'{time_range},{aggN.__name__},abs_time,{abs_time}')
                    f.write(f'{time_range},{aggN.__name__},abs_time,{abs_time}\n')
                    f.flush()

                    # filter relative
                    rel_time = timedelta(0)
                    for _ in range(cnt_tests):
                        tm = datetime.now()
                        tree.filter(['1second'],
                                    relative=[['src', 'src & dst'],
                                              ['dst', 'src & dst']
                                             ])
                        rel_time += (datetime.now() - tm)
                    rel_time /= cnt_tests
                    rel_time = rel_time.total_seconds() * 1000
                    print(f'{time_range},{aggN.__name__},rel_time,{rel_time}')
                    f.write(f'{time_range},{aggN.__name__},rel_time,{rel_time}\n')
                    f.flush()

                    print('')

                    break

    f.close()

if __name__ == '__main__':

    test()