import os
import subprocess
import time

import pandas as pd
import sys

import dotenv

dotenv.load_dotenv()
wd = os.getenv('DOWNLOAD_SCRIPT_PATH')
log_folder = os.getenv('DOWNLOAD_LOG_PATH')

os.chdir(wd)

today = pd.Timestamp.today().strftime('%Y%m%d%H%M')

SLEEPTIME = 60*10
TIMES = 72
exit_code = 0

print('Starting run_gfs.py at', today)

"""
Codes:
    0: success
    1: error
    2: no data to process
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
    if code == 3:
        exit_code = 3
    elif code == 0:
        exit_code = 0
    else:
        pass
    return code


i = 0


while i < TIMES:
    gfs = run_stuff('gfs.py', alias='GFS')
    if gfs != 3:
        break
    else:
        i += 1
        print(f"No new data for GFS: moving to iteration {i}: Time to fall asleep for {SLEEPTIME} seconds")
        time.sleep(SLEEPTIME)
        print(f"Waking up at {pd.Timestamp.today().strftime('%Y%m%d%H%M')}")


if gfs in [0,4]:
    print('GFS ran successfully')
    pd.DataFrame({'gfs': [gfs]}).to_csv(f'{log_folder}/gfs_log_{today}.csv', index=False)
    sys.exit(0)
else:
    # raise an error
    print('GFS failed')
    raise ValueError(f'GFS failed')
