import os
import sys
import hidrocl
import subprocess
import dotenv
import pandas as pd


dotenv.load_dotenv()

wb_path = os.getenv('WB_PATH')

wd = os.getenv('HIDROCL_ROOT_PATH')

def run_stuff(file, alias=None):
    """
    Run stuff in bash

    Args:
        file: path to the file to be run

    Returns:
        code: code of the execution
    """
    try:
        now = pd.Timestamp.today()
        print(f"{alias} starting at {now.strftime('%Y%m%d%H%M')}")
        code = subprocess.call(['python', file])
        then = pd.Timestamp.today()
        diff = then - now
        print(f"{alias} finished at {then.strftime('%Y%m%d%H%M')}. It took {diff.seconds} seconds")
    except:
        code = 1
        raise(f'Error running extraction of {alias}')
    return code

step1a = run_stuff(f'{wd}/src/1_dbmanager/HidroCL_OPP/run_gfs.py', '1a_gfs')
step1b = run_stuff(f'{wd}/src/1_dbmanager/HidroCL_OPP/run_all.py', '1b_dynamic')
wb_dates = pd.read_csv(wb_path, usecols=['date'])
wb_dates = pd.to_datetime(wb_dates['date'], format='%Y-%m-%d')
last_date = max(wb_dates)
today = pd.Timestamp.today()
if today.month > (last_date.month + 1):
    if today.day >= 7:
        print('Downloading new data for water bodies')
        step1c = run_stuff(f'{wd}/src/1_dbmanager/HidroCL-WaterBodyArea/run_all.py', '1c_waterbodies')
    else:
        print('Giving a bit more of days before downloading new data')
step2 = run_stuff(f'{wd}/src/2_imputation/impute_stuff.py', '2_imputing')
step3 = run_stuff(f'{wd}/src/3_structuring/Variable_Class.py', '3_structuring')
step4 = run_stuff(f'{wd}/src/4_CNN_model/run_model.py', '4_infering')

total = pd.DataFrame({'vals':[step1a, step1b, step1c, step2, step3, step4]})
exit_code = total['vals'].max()

sys.exit(exit_code)
