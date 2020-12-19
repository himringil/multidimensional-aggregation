import datetime

import agg0, agg1, agg2, agg3
from pympler import asizeof

def test():

    tree_conf = {
     "name":"1second",
     "range":"1000s",
     "delta":"1s"
     }
    
    params_conf = [ [ "src",
                      "dst",
                      [ "src", "dst" ]
                    ],
                    [ "src",
                      "dst",
                      "service",
                      [ "src", "dst", "service" ]
                    ],
                    [ "src",
                      "dst",
                      "service",
                      "proxy_src_ip",
                      [ "src", "dst", "service", "proxy_src_ip" ]
                    ],
                    [ "src",
                      "dst",
                      "service",
                      "proxy_src_ip",
                      "SCADA_Tag",
                      [ "src", "dst", "service", "proxy_src_ip", "SCADA_Tag" ],
                    ],
                    [ "src",
                      "dst",
                      "service",
                      "proxy_src_ip",
                      "SCADA_Tag",
                      "s_port",
                      [ "src", "dst", "service", "proxy_src_ip", "SCADA_Tag", "s_port" ]
                    ],
                    [ "src",
                      "dst",
                      "service",
                      "proxy_src_ip",
                      "SCADA_Tag",
                      "s_port",
                      "appi_name",
                      [ "src", "dst", "service", "proxy_src_ip", "SCADA_Tag", "s_port", "appi_name" ]
                    ],
                    [ "src",
                      "dst",
                      "service",
                      "proxy_src_ip",
                      "SCADA_Tag",
                      "s_port",
                      "appi_name",
                      "proto",
                      [ "src", "dst", "service", "proxy_src_ip", "SCADA_Tag", "s_port", "appi_name", "proto" ]
                    ],
                    [ "src",
                      "dst",
                      "service",
                      "proxy_src_ip",
                      "SCADA_Tag",
                      "s_port",
                      "appi_name",
                      "proto",
                      "orig",
                      [ "src", "dst", "service", "proxy_src_ip", "SCADA_Tag", "s_port", "appi_name", "proto",
                        "orig" ]
                    ],
                    [ "src",
                      "dst",
                      "service",
                      "proxy_src_ip",
                      "SCADA_Tag",
                      "s_port",
                      "appi_name",
                      "proto",
                      "orig",
                      "type",
                      [ "src", "dst", "service", "proxy_src_ip", "SCADA_Tag", "s_port", "appi_name", "proto",
                        "orig", "type" ]
                    ],
                    [ "src",
                      "dst",
                      "service",
                      "proxy_src_ip",
                      "SCADA_Tag",
                      "s_port",
                      "appi_name",
                      "proto",
                      "orig",
                      "type",
                      "if_name",
                      [ "src", "dst", "service", "proxy_src_ip", "SCADA_Tag", "s_port", "appi_name", "proto",
                        "orig", "type", "if_name" ]
                    ],
                    [ "src",
                      "dst",
                      "service",
                      "proxy_src_ip",
                      "SCADA_Tag",
                      "s_port",
                      "appi_name",
                      "proto",
                      "orig",
                      "type",
                      "if_name",
                      "if_dir",
                      [ "src", "dst", "service", "proxy_src_ip", "SCADA_Tag", "s_port", "appi_name", "proto",
                        "orig", "type", "if_name", "if_dir" ]
                    ],
                    [ "src",
                      "dst",
                      "service",
                      "proxy_src_ip",
                      "SCADA_Tag",
                      "s_port",
                      "appi_name",
                      "proto",
                      "orig",
                      "type",
                      "if_name",
                      "if_dir",
                      "Modbus_Function_Code",
                      [ "src", "dst", "service", "proxy_src_ip", "SCADA_Tag", "s_port", "appi_name", "proto",
                        "orig", "type", "if_name", "if_dir", "Modbus_Function_Code" ]
                    ]
                  ]

    params_rel = [[['src', 'src & dst'],
                   ['dst', 'src & dst']
                  ],
                  [['src', 'src & dst & service'],
                   ['dst', 'src & dst & service'],
                   ['service', 'src & dst & service'],
                  ],
                  [['src', 'src & dst & service & proxy_src_ip'],
                   ['dst', 'src & dst & service & proxy_src_ip'],
                   ['service', 'src & dst & service & proxy_src_ip'],
                   ['proxy_src_ip', 'src & dst & service & proxy_src_ip'],
                  ],
                  [['src', 'src & dst & service & proxy_src_ip & SCADA_Tag'],
                   ['dst','src & dst & service & proxy_src_ip & SCADA_Tag'],
                   ['service','src & dst & service & proxy_src_ip & SCADA_Tag'],
                   ['proxy_src_ip','src & dst & service & proxy_src_ip & SCADA_Tag'],
                   ['SCADA_Tag','src & dst & service & proxy_src_ip & SCADA_Tag'],
                  ],
                  [['src', 'src & dst & service & proxy_src_ip & SCADA_Tag & s_port'],
                   ['dst','src & dst & service & proxy_src_ip & SCADA_Tag & s_port'],
                   ['service','src & dst & service & proxy_src_ip & SCADA_Tag & s_port'],
                   ['proxy_src_ip','src & dst & service & proxy_src_ip & SCADA_Tag & s_port'],
                   ['SCADA_Tag','src & dst & service & proxy_src_ip & SCADA_Tag & s_port'],
                   ['s_port','src & dst & service & proxy_src_ip & SCADA_Tag & s_port'],
                  ],
                  [['src', 'src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name'],
                   ['dst','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name'],
                   ['service','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name'],
                   ['proxy_src_ip','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name'],
                   ['SCADA_Tag','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name'],
                   ['s_port','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name'],
                   ['appi_name','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name'],
                  ],
                  [['src', 'src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto'],
                   ['dst','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto'],
                   ['service','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto'],
                   ['proxy_src_ip','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto'],
                   ['SCADA_Tag','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto'],
                   ['s_port','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto'],
                   ['appi_name','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto'],
                   ['proto','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto'],
                  ],
                  [['src', 'src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig'],
                   ['dst','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig'],
                   ['service','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig'],
                   ['proxy_src_ip','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig'],
                   ['SCADA_Tag','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig'],
                   ['s_port','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig'],
                   ['appi_name','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig'],
                   ['proto','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig'],
                   ['orig','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig'],
                  ],
                  [['src', 'src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type'],
                   ['dst','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type'],
                   ['service','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type'],
                   ['proxy_src_ip','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type'],
                   ['SCADA_Tag','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type'],
                   ['s_port','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type'],
                   ['appi_name','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type'],
                   ['proto','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type'],
                   ['orig','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type'],
                   ['type','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type'],
                  ],
                  [['src', 'src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name'],
                   ['dst','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name'],
                   ['service','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name'],
                   ['proxy_src_ip','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name'],
                   ['SCADA_Tag','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name'],
                   ['s_port','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name'],
                   ['appi_name','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name'],
                   ['proto','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name'],
                   ['orig','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name'],
                   ['type','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name'],
                   ['if_name','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name'],
                  ],
                  [['src', 'src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir & Modbus_Function_Code'],
                   ['dst','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir & Modbus_Function_Code'],
                   ['service','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir & Modbus_Function_Code'],
                   ['proxy_src_ip','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir & Modbus_Function_Code'],
                   ['SCADA_Tag','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir & Modbus_Function_Code'],
                   ['s_port','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir & Modbus_Function_Code'],
                   ['appi_name','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir & Modbus_Function_Code'],
                   ['proto','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir & Modbus_Function_Code'],
                   ['orig','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir & Modbus_Function_Code'],
                   ['type','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir & Modbus_Function_Code'],
                   ['if_name','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir & Modbus_Function_Code'],
                   ['if_dir','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir & Modbus_Function_Code'],
                  ]
                 ]

                     
    data_path = 'data'
    
    n_files = 1
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