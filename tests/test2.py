import pandas as pd
from os import walk
from os.path import join
from datetime import datetime, timedelta
from pympler import asizeof

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
     "range":"5000s",
     "delta":"1s"
     }
    
    params_conf = [ [ ["src"],
                      ["dst"],
                      [ "src", "dst" ]
                    ],
                    [ ["src"],
                      ["dst"],
                      ["service"],
                      [ "src", "dst", "service" ]
                    ],
                    [ ["src"],
                      ["dst"],
                      ["service"],
                      ["proxy_src_ip"],
                      [ "src", "dst", "service", "proxy_src_ip" ]
                    ],
                    [ ["src"],
                      ["dst"],
                      ["service"],
                      ["proxy_src_ip"],
                      ["SCADA_Tag"],
                      [ "src", "dst", "service", "proxy_src_ip", "SCADA_Tag" ],
                    ],
                    [ ["src"],
                      ["dst"],
                      ["service"],
                      ["proxy_src_ip"],
                      ["SCADA_Tag"],
                      ["s_port"],
                      [ "src", "dst", "service", "proxy_src_ip", "SCADA_Tag", "s_port" ]
                    ],
                    [ ["src"],
                      ["dst"],
                      ["service"],
                      ["proxy_src_ip"],
                      ["SCADA_Tag"],
                      ["s_port"],
                      ["appi_name"],
                      [ "src", "dst", "service", "proxy_src_ip", "SCADA_Tag", "s_port", "appi_name" ]
                    ],
                    [ ["src"],
                      ["dst"],
                      ["service"],
                      ["proxy_src_ip"],
                      ["SCADA_Tag"],
                      ["s_port"],
                      ["appi_name"],
                      ["proto"],
                      [ "src", "dst", "service", "proxy_src_ip", "SCADA_Tag", "s_port", "appi_name", "proto" ]
                    ],
                    [ ["src"],
                      ["dst"],
                      ["service"],
                      ["proxy_src_ip"],
                      ["SCADA_Tag"],
                      ["s_port"],
                      ["appi_name"],
                      ["proto"],
                      ["orig"],
                      [ "src", "dst", "service", "proxy_src_ip", "SCADA_Tag", "s_port", "appi_name", "proto",
                        "orig" ]
                    ],
                    [ ["src"],
                      ["dst"],
                      ["service"],
                      ["proxy_src_ip"],
                      ["SCADA_Tag"],
                      ["s_port"],
                      ["appi_name"],
                      ["proto"],
                      ["orig"],
                      ["type"],
                      [ "src", "dst", "service", "proxy_src_ip", "SCADA_Tag", "s_port", "appi_name", "proto",
                        "orig", "type" ]
                    ],
                    [ ["src"],
                      ["dst"],
                      ["service"],
                      ["proxy_src_ip"],
                      ["SCADA_Tag"],
                      ["s_port"],
                      ["appi_name"],
                      ["proto"],
                      ["orig"],
                      ["type"],
                      ["if_name"],
                      [ "src", "dst", "service", "proxy_src_ip", "SCADA_Tag", "s_port", "appi_name", "proto",
                        "orig", "type", "if_name" ]
                    ],
                    [ ["src"],
                      ["dst"],
                      ["service"],
                      ["proxy_src_ip"],
                      ["SCADA_Tag"],
                      ["s_port"],
                      ["appi_name"],
                      ["proto"],
                      ["orig"],
                      ["type"],
                      ["if_name"],
                      ["if_dir"],
                      [ "src", "dst", "service", "proxy_src_ip", "SCADA_Tag", "s_port", "appi_name", "proto",
                        "orig", "type", "if_name", "if_dir" ]
                    ],
                    [ ["src"],
                      ["dst"],
                      ["service"],
                      ["proxy_src_ip"],
                      ["SCADA_Tag"],
                      ["s_port"],
                      ["appi_name"],
                      ["proto"],
                      ["orig"],
                      ["type"],
                      ["if_name"],
                      ["if_dir"],
                      ["Modbus_Function_Code"],
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
                  [['src', 'src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir'],
                   ['dst','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir'],
                   ['service','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir'],
                   ['proxy_src_ip','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir'],
                   ['SCADA_Tag','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir'],
                   ['s_port','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir'],
                   ['appi_name','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir'],
                   ['proto','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir'],
                   ['orig','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir'],
                   ['type','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir'],
                   ['if_name','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir'],
                   ['if_dir','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir'],
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
                   ['Modbus_Function_Code','src & dst & service & proxy_src_ip & SCADA_Tag & s_port & appi_name & proto & orig & type & if_name & if_dir & Modbus_Function_Code'],
                  ]
                 ]

    data_path = 'data_example'
    
    n_files = 1
    cnt_tests = 5

    f = open('results/test2.csv', 'w')

    for conf, relative in zip(params_conf, params_rel):

        for aggN in [agg0, agg3]:

            print(f'{datetime.now()} -> {aggN.__name__} ({conf})')

            tree = aggN.AggTree(tree_conf, conf)
            
            agg_time = timedelta(0)
        
            for cnt, (tree, td) in enumerate(aggregate(tree, data_path)):
        
                agg_time += td
        
                if cnt + 1 == n_files:
                    
                    print(f'{time_range},{aggN.__name__},cnt_queu,{len(tree.tree.queue)}')

                    # time to aggregate 100000 elements
                    agg_time /= (5 * n_files)
                    agg_time = int(agg_time.total_seconds() * 1000)  # ms
                    print(f'{aggN.__name__},agg_time,{agg_time}')
                    f.write(f'{aggN.__name__},agg_time,{agg_time}\n')
                    f.flush()
        
                    # time to filter relative
                    rel_time = timedelta(0)
                    for _ in range(cnt_tests):
                        tm = datetime.now()
                        tree.filter(['1second'],
                                    relative=relative)
                        rel_time += (datetime.now() - tm)
                    rel_time /= cnt_tests
                    rel_time = rel_time.total_seconds() * 1000
                    print(f'{aggN.__name__},rel_time,{rel_time}')
                    f.write(f'{aggN.__name__},rel_time,{rel_time}\n')
                    f.flush()
        
                    print('')
        
                    break

    f.close()

if __name__ == '__main__':

    test()