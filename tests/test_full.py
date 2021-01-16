import pandas as pd
from datetime import datetime
from os import walk
from os.path import join

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

                print(f'   --- {datetime.now()} -> {filename} ({len(df.index)} rows)')
                agg.aggregate(tree, df)
                print(f'   +++ {datetime.now()} -> {filename} ({len(df.index)} rows)')

                tree.delete_zero_elements()
                
                yield tree
        break

if __name__ == '__main__':

    data_path = 'data_example'

    tree_conf = 'configs/tree.json'
    params_confs = [ 'configs/parameters03.json', 'configs/parameters12.json',
                     'configs/parameters12.json', 'configs/parameters03.json' ]

    for aggN, params_conf in zip([agg0, agg1, agg2, agg3], params_confs):
    
        print(aggN.__name__)

        tree = aggN.AggTree(agg.load_tree(tree_conf), agg.load_params(params_conf))

        for tree in aggregate(tree, data_path):

            tree.print()

            print('--------------------------------')
            tree.filter(['10sec -> 1sec', '10min -> 1min'], [['src', '192.168.1.10']]).print()
            print('--------------------------------')
            tree.filter([], [['service', '137']]).print()
            print('--------------------------------')
            tree.filter(['10sec -> 1sec', '10min -> 1min'], [['', '192.168.1.50'], ['service', '']]).print()
            print('--------------------------------')
            tree.filter('10sec -> 1sec',
                        absolute=[['src', ''], ['dst', '']],
                        relative=[['src', 'src & dst'],
                                  ['dst', 'src & dst']]).print()

            break

        print('')
        print('----------------------------------------------------------------')
        print('')
        print('')
