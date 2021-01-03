import pandas as pd
from os.path import join
from os import walk
from json import load
from datetime import datetime, timedelta

def load_tree(path):
    f = open(path)
    return load(f)

def load_params(path):
    f = open(path)
    return load(f)

def aggregate(tree, data_path):
    
    for (_, _, filenames) in walk(data_path):
        for filename in sorted(filenames):
            if filename.endswith('.parquet'):

                filepath = join(data_path, filename)
                df = pd.read_parquet(filepath)
                df['datetime'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'], errors='coerce', format='%d%b%Y %H:%M:%S')
                df.drop(axis=1, columns=['date', 'time'], inplace=True)
                df.rename(columns={'i/f_name' : 'if_name', 'i/f_dir' : 'if_dir'}, inplace=True)

                print(f'    {datetime.now()} -> {filename} ({len(df.index)} rows)')
                
                td = timedelta(0)

                for index, row in df.iterrows():
                    tm = datetime.now()
                    if not row['src'] or not row['dst']:
                        continue
                    try:
                        tree.aggregate(row)
                    except Exception as e:
                        print(f'Exception at {index}: {e}')
                    td += (datetime.now() - tm)

                tree.delete_zero_elements()
                
                yield tree, td
        break