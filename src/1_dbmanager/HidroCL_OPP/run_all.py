import os
import subprocess

import pandas as pd
import sys

import dotenv

dotenv.load_dotenv()
wd = os.getenv('DOWNLOAD_SCRIPT_PATH')
log_folder = os.getenv('DOWNLOAD_LOG_PATH')

os.chdir(wd)

today = pd.Timestamp.today().strftime('%Y%m%d%H%M')

exit_code = 0

print('Starting run_all.py at', today)

"""
Codes:
    0: success
    1: error
    0: no data to process
    3: no new data to download, although it should be
    4: no new data to download, up to date
    5: insufficient data to download and process
"""


def run_stuff(file, alias=None):
    """
    Run stuff in bash

    Args:
        file: path to the file to be run

    Returns:
        code: code of the execution
    """
    global exit_code
    try:
        now = pd.Timestamp.today()
        print(f"{alias} starting at {now.strftime('%Y%m%d%H%M')}")
        code = subprocess.call(['python', file])
        then = pd.Timestamp.today()
        diff = then - now
        print(f"{alias} finished at {then.strftime('%Y%m%d%H%M')}. It took {diff.seconds} seconds")
    except:
        code = 1
        exit_code = 1
        raise(f'Error running extraction of {alias}')
    if code == 2:
        exit_code = 1
    return code


era5 = run_stuff('era5.py', alias='ERA5')
era5land = run_stuff('era5land.py', alias='ERA5LAND')
era5pl = run_stuff('era5pl.py', alias='ERA5PL')
mcd15a2h = run_stuff('mcd15a2h.py', alias='MCD15A2H')
mod10a2 = run_stuff('mod10a2.py', alias='MOD10A2')
mod13q1 = run_stuff('mod13q1.py', alias='MOD13Q1')
pdirnow = run_stuff('pdirnow.py', alias='PDIRNOW')
imerggis = run_stuff('imerggis.py', alias='IMERGGIS')

remove_duplicates = run_stuff('remove_duplicates.py', alias='REMOVE_DUPLICATES')

pd.DataFrame({'era5': [era5],
              'era5land': [era5land],
              'era5pl': [era5pl],
              'mcd15a2h': [mcd15a2h],
              'mod10a2': [mod10a2],
              'mod13q1': [mod13q1],
              'pdirnow': [pdirnow],
              'imerggis': [imerggis]}).to_csv(f'{log_folder}/log_{today}.csv',
                                              index=False)
sys.exit(exit_code)

