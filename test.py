import datetime

import agg0, agg1, agg2, agg3
from pympler import asizeof

def test():

    tree_conf = {
     "name":"1second",
     "range":"5000s",
     "delta":"1s"
     }
    
    params_conf = [ [ "src",
                      "dst",
                      [ "src", "dst" ]
                    ],
                    [ "src",
                      "dst",
                      [ "src", "dst" ],
                      [ "src", "service" ],
                      [ "dst", "service" ],
                      [ "src", "dst", "service" ]
                    ],
                    [ "src",
                      "dst",
                      [ "src", "dst" ],
                      [ "src", "proxy_src_ip" ],
                      [ "src", "proxy_src_ip" ],
                      [ "src", "service" ],
                      [ "dst", "service" ],
                      [ "dst", "proxy_src_ip" ],
                      [ "service", "proxy_src_ip" ],
                      [ "src", "dst", "service" ],
                      [ "src", "dst", "proxy_src_ip" ],
                      [ "src", "service", "proxy_src_ip" ],
                      [ "dst", "service", "proxy_src_ip" ],
                      [ "src", "dst", "service", "proxy_src_ip" ]
                    ]
                   
                  ]

    params_rel = [[['src', 'src & dst'],
                   ['dst', 'src & dst']
                  ],
                  [['src', 'src & dst'],
                   ['dst', 'src & dst'],
                   ['src', 'src & service'],
                   ['service', 'src & service'],
                   ['dst', 'dst & service'],
                   ['service', 'dst & service'],
                   ['src', 'src & dst & service'],
                   ['dst', 'src & dst & service'],
                   ['service', 'src & dst & service'],
                   ['src & dst', 'src & dst & service'],
                   ['src & service', 'src & dst & service'],
                   ['dst & service', 'src & dst & service'],
                  ],
                  [['src', 'src & dst'],
                   ['src', 'src & service'],
                   ['src', 'src & proxy_src_ip'],
                   ['dst', 'src & dst'],
                   ['dst', 'dst & service'],
                   ['dst', 'dst & proxy_src_ip'],
                   ['service', 'src & service'],
                   ['service', 'dst & service'],
                   ['service', 'service & proxy_src_ip'],
                   ['proxy_src_ip', 'src & proxy_src_ip'],
                   ['proxy_src_ip', 'dst & proxy_src_ip'],
                   ['proxy_src_ip', 'service & proxy_src_ip'],
                   ['src', 'src & dst & service'],
                   ['src', 'src & dst & proxy_src_ip'],
                   ['src', 'src & service & proxy_src_ip'],
                   ['dst', 'src & dst & service'],
                   ['dst', 'src & dst & proxy_src_ip'],
                   ['dst', 'dst & service & proxy_src_ip'],
                   ['service', 'src & dst & service'],
                   ['service', 'src & service & proxy_src_ip'],
                   ['service', 'dst & service & proxy_src_ip'],
                   ['proxy_src_ip', 'src & dst & proxy_src_ip'],
                   ['proxy_src_ip', 'src & service & proxy_src_ip'],
                   ['proxy_src_ip', 'dst & service & proxy_src_ip'],
                   ['src & dst', 'src & dst & service'],
                   ['src & service', 'src & dst & service'],
                   ['dst & service', 'src & dst & service'],
                   ['src & dst', 'src & dst & proxy_src_ip'],
                   ['src & proxy_src_ip', 'src & dst & proxy_src_ip'],
                   ['dst & proxy_src_ip', 'src & dst & proxy_src_ip'],
                   ['src & service', 'src & service & proxy_src_ip'],
                   ['src & proxy_src_ip', 'src & service & proxy_src_ip'],
                   ['service & proxy_src_ip', 'src & service & proxy_src_ip'],
                   ['dst & service', 'dst & service & proxy_src_ip'],
                   ['dst & proxy_src_ip', 'dst & service & proxy_src_ip'],
                   ['service & proxy_src_ip', 'dst & service & proxy_src_ip'],
                   ['src', 'src & dst & service & proxy_src_ip'],
                   ['dst', 'src & dst & service & proxy_src_ip'],
                   ['service', 'src & dst & service & proxy_src_ip'],
                   ['proxy_src_ip', 'src & dst & service & proxy_src_ip'],
                   ['src & dst', 'src & dst & service & proxy_src_ip'],
                   ['src & service', 'src & dst & service & proxy_src_ip'],
                   ['src & proxy_src_ip', 'src & dst & service & proxy_src_ip'],
                   ['dst & service', 'src & dst & service & proxy_src_ip'],
                   ['dst & proxy_src_ip', 'src & dst & service & proxy_src_ip'],
                   ['service & proxy_src_ip', 'src & dst & service & proxy_src_ip'],
                   ['src & dst & service', 'src & dst & service & proxy_src_ip'],
                   ['src & dst & proxy_src_ip', 'src & dst & service & proxy_src_ip'],
                   ['dst & service & proxy_src_ip', 'src & dst & service & proxy_src_ip'],
                  ]
                 ]

                     
    data_path = 'data'
    
    n_files = 4
    cnt_tests = 3

    f = open('test_result.txt', 'w')

    for conf, relative in zip(params_conf, params_rel):
        for agg in [agg0, agg3]:
        
            print(f'{datetime.datetime.now()} -> {agg.__name__} (conf)')
            agg_time = datetime.timedelta(0)
        
            for cnt, (tree, td) in enumerate(agg.aggregate(tree_conf, conf, data_path)):
        
                agg_time += td
        
                if cnt + 1 == n_files:
                    
                    # time to aggregate 500000 elements
                    agg_time /= n_files
                    agg_time = int(agg_time.total_seconds() * 1000)  # ms
                    print(f'{agg.__name__}, agg_time, {agg_time}')
                    f.write(f'{agg.__name__},agg_time,{agg_time}\n')
                    f.flush()
        
                    # filter absolute
                    abs_time = datetime.timedelta(0)
                    for _ in range(cnt_tests):
                        tm = datetime.datetime.now()
                        tree.filter(['1second'],
                                    absolute=[['', '']])
                        abs_time += (datetime.datetime.now() - tm)
                    abs_time /= cnt_tests
                    abs_time = abs_time.total_seconds() * 1000
                    print(f'{agg.__name__}, abs_time, {abs_time}')
                    f.write(f'{agg.__name__},abs_time,{abs_time}\n')
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