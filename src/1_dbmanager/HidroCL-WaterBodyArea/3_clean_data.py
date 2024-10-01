import os
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings('ignore')

in_path = 'Databases'
out_path = 'Cleaned'

files = os.listdir(in_path)

files = [f for f in files if f.endswith('.csv')]

def get_data(filename):
    df = pd.read_csv(filename)

    df = df.rename(columns={
        'water_area_time': 'time'
    })

    df['time_ms'] = df.time

    df.reset_index(inplace=True)
    df.drop(columns=['index'], inplace=True)

    df.time = pd.to_datetime(df.time, unit='ms')
    df = df.set_index('time')

    df = df[df.water_area_filled_fraction < 0.3]
    df = df[df.quality_score < 0.3]

    return df


def remove_large_gradients(df, th):
    df['water_area_filled_grad'] = df.area.diff()
    df = df[pd.notnull(df.water_area_filled_grad)]

    if len(df.water_area_filled_grad.to_numpy()):
        grad_th = np.percentile(
            np.abs(df.water_area_filled_grad.to_numpy()), th)
        df = df[np.abs(df.water_area_filled_grad) < grad_th]

    return df


def clean_data(df_eo, step='M', skip_missings=True, min_missings_step=12):
    d = df_eo
    d['area'] = d.water_area_filled
    d = d[['area']]

    # round to days
    d.index = d.index.round('D')

    #d = remove_large_gradients(d, 90)
    #d = remove_large_gradients(d, 99)

    # take top 90% (eliminate underfilling due to lower trust in water occurrence)
    d = d.resample(step).apply(lambda x: x.quantile(0.90))

    # create mask
    if skip_missings:
        mask = d.copy()
        grp = ((mask.notnull() != mask.shift().notnull()).cumsum())
        grp['ones'] = 1
        mask['area'] = (grp.groupby('area')['ones'].transform(
            'count') < min_missings_step) | d['area'].notnull()

    # smoothen
    d = d.interpolate(method='pchip')
    # d = d.shift(-1)

    # apply missing values mask (>6 months)
    if skip_missings:
        d[mask.area == False] = None

    return d


# get today's date
today = pd.Timestamp.today().strftime('%Y-%m-%d')

# create a empty dataframe with the dates from 2000-01-01 to today with monthly frequency
idx = pd.date_range('2000-01-01', today, freq='M')
# create a template dataframe with the dates
template = pd.DataFrame({'date': idx})

# create a list of dataframes
dbs = []

for file in files:
    df = get_data(os.path.join(in_path, file))
    dfc = clean_data(df, min_missings_step=18, skip_missings=True)
    dfo = template.merge(dfc['area'], how='left', left_on='date', right_index=True)
    # fill last 8 months with last value
    dfo['area'][-8:] = dfo['area'][-8:].fillna(method='ffill')
    # fill the first NANs with the first valid observation
    dfo['area'][:15] = dfo['area'][:15].fillna(method='bfill')
    # fill nans values with 0
    dfo['area'] = dfo['area'].fillna(0)
    # add reservoir name
    dfo['reservoir'] = file.split('.')[0]
    # append to list
    dbs.append(dfo)

# merge all reservoirs
df = pd.concat(dbs)

# pivot wider
df = df.pivot(index='date', columns='reservoir', values='area')

# reset all indices
df.reset_index().to_csv('Cleaned/reservoirs.csv', index=False)
