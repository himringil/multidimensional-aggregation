import pandas as pd
from os import walk
from os.path import join
from datetime import datetime, timedelta

import agg, agg0, agg1, agg2, agg3, agg4

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

    tree_conf = {}
    
    conf = [ ["src"],
             ["dst"],
             ["service"],
             ["proxy_src_ip"],
             ["SCADA_Tag"],
             ["s_port"],
             ["appi_name"],
             ["proto"],
             [ "src", "dst", "service", "proxy_src_ip", "SCADA_Tag", "s_port", "appi_name", "proto" ]
           ]

    data_path = 'data_example'
    
    n_files = 4

    f = open('results/test4.csv', 'w')

    for i in range(1, 21):

        if not tree_conf:
            tree_conf = {'name':f'{i}', 'range':'100s', 'delta':'1s'}
        else:
            cur = tree_conf
            while cur.get('child', None) is not None:
                cur = cur['child'][0]
            cur['child'] = [{}]
            cur = cur['child'][0]
            cur['name'] = f'{i}'
            cur['delta'] = f'{i}s'
            cur['range'] = f'{i}00s'

        for aggN in [agg0, agg4]:
    
            print(f'{datetime.now()} -> {aggN.__name__} ({conf})')
    
            tree = aggN.AggTree(tree_conf, conf)
            
            agg_time = timedelta(0)
        
            for cnt, (tree, td) in enumerate(aggregate(tree, data_path)):
        
                agg_time += td
        
                if cnt + 1 == n_files:
                    
                    # count of queues
                    count = tree.count()
                    print(f'{i},{aggN.__name__},cnt_queu,{count}')
                    f.write(f'{i},{aggN.__name__},cnt_queu,{count}\n')
                    f.flush()
    
                    # time to aggregate 100000 elements
                    agg_time /= (5 * n_files)
                    agg_time = int(agg_time.total_seconds() * 1000)  # ms
                    print(f'{i},{aggN.__name__},agg_time,{agg_time}')
                    f.write(f'{i},{aggN.__name__},agg_time,{agg_time}\n')
                    f.flush()
    
                    # sum size of queues
                    sum_size = tree.size()
                    print(f'{i},{aggN.__name__},sum_size,{sum_size}')
                    f.write(f'{i},{aggN.__name__},sum_size,{sum_size}\n')
                    f.flush()
        
                    print('')
        
                    break

    f.close()

if __name__ == '__main__':

    test()