from sys import argv
import time, datetime

import agg0, agg1, agg2, agg3

def test(range):

    tree_conf = {
     "name":"1second",
     "range":f"{range}s",
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

    for agg, i in zip([agg0, agg1, agg2, agg3], [ 0, 1, 1, 0 ]):
        print(f'{datetime.datetime.now()} -> {str(agg)}')
        tree = agg.aggregate(tree_conf, params_conf[i], data_path)
        #tree.print()
        tree.filter(['1second'],
                    absolute=[['src', ''], ['dst', '']],
                    relative=[['src', 'src & dst'],
                              ['dst', 'src & dst']
                             ]).print()
        print(f'{datetime.datetime.now()}')
        print('')
        print('----------------------------------------------------------------')
        print('')

if __name__ == '__main__':

    # ranges = [ 10, 50, 100, 500, 1000 ] + [i for i in range(5000, 40001, 5000)]

    if len(argv) < 2:
        raise Exception('args: <time_range>')

    test(argv[1])