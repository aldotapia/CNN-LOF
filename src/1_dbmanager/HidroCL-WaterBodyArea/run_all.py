import os
import sys
import subprocess
import dotenv
import pandas as pd

dotenv.load_dotenv()
wd = os.getenv('WATERBODY_PATH')

os.chdir(wd)

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

step1 = run_stuff('1_get_data.py', '1_get_data')
step2 = run_stuff('2_order_data.py', '2_order_data')
step3 = run_stuff('3_clean_data.py', '3_clean_data')
step4 = run_stuff('4_get_hidrocl_variable.py', '4_get_hidrocl_variable')

total = pd.DataFrame({'vals':[step1, step2, step3, step4]})
exit_code = total['vals'].max()

sys.exit(exit_code)
