import agg, agg0, agg1, agg2, agg3

if __name__ == '__main__':

    data_path = 'data_example'

    tree_conf = 'configs/tree.json'
    params_confs = [ 'configs/parameters03.json', 'configs/parameters12.json',
                     'configs/parameters12.json', 'configs/parameters03.json'
                   ]

    for aggN, params_conf in zip([agg0, agg1, agg2, agg3], params_confs):
    
        print(aggN.__name__)
    
        tree = aggN.AggTree(agg.load_tree(tree_conf), agg.load_params(params_conf))

        for tree, td in agg.aggregate(tree, data_path):

            ts = ['10sec -> 1sec', '10min -> 1min']
    
            print('--------------------------------')
            tree.filter(ts, [['src', '192.168.1.10']]).print()
            print('--------------------------------')
            tree.filter(ts, [['service', '137']]).print()
            print('--------------------------------')
            tree.filter(ts, [['', '192.168.1.50'], ['service', '']]).print()
            print('--------------------------------')
            tree.filter(ts,
                        absolute=[['src', ''], ['dst', '']],
                        relative=[['src', 'src & dst'],
                                  ['dst', 'src & dst']]).print()

            break

        print('----------------------------------------------------------------')
        print('')
        print('')

