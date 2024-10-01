import os
from pathlib import Path

import pandas as pd

# conf paths
in_path = 'Output'
out_path = 'Databases'

# list files
files = os.listdir(in_path)

# filter csv files
files = [f for f in files if f.endswith('.csv')]

# get unique ids
ids = [f.split('_')[0] for f in files]

ids = [val for val in ids if 'hi' not in val]

# remove duplicates
ids = list(set(ids))

# sort ids
ids = sorted(ids)

for id in ids:
    # filter files by id
    files2 = [f for f in files if f.split('_')[0] == id]
    # read files
    dfs = [pd.read_csv(os.path.join(in_path, f)) for f in files2]
    l = []
    # check is has at least one row
    for df in dfs:
        if df.shape[0] > 0:
            l.append(df)
    if len(l) == 0:
        continue
    # concat files
    df = pd.concat(l)
    # check if df is empty
    if df.empty:
        continue
    else:
        pass
    # reset index
    df = df.reset_index(drop=True)
    # convert time to datetime
    idx = pd.to_datetime(df['water_area_time'], unit='ms')
    # change name of index to index
    idx.name = 'index'
    # convert index to datetime
    idx = idx.dt.strftime('%Y-%m-%d')
    # set index
    df = df.set_index(idx)
    # sort index
    df = df.sort_index()
    # drop duplicate by index
    df = df.drop_duplicates(subset='water_area_time', keep='last')
    # drop columns
    df = df.drop(columns=['Unnamed: 0'])
    # save file
    out_file = Path(os.path.join(out_path, id + '.csv'))

    # check if file exists
    if out_file.exists():
        # read file
        df2 = pd.read_csv(os.path.join(out_path, id + '.csv'), index_col=0)
        # concat files
        df2 = pd.concat([df, df2])
        # reset index
        df2 = df2.sort_values(by='water_area_time')
        # drop duplicate by index
        df2 = df2.drop_duplicates(subset='water_area_time', keep='last')
        # save file
        df2.to_csv(out_file)
    else:
        df.to_csv(out_file)

# remove files
for f in files:
    # remove files
    os.remove(os.path.join(in_path, f))
