import os
import pandas as pd
import subprocess

import dotenv

dotenv.load_dotenv()
wd = os.getenv('HIDROCL_CNN_PATH')

os.chdir(wd)

today = pd.Timestamp.today().strftime('%Y%m%d%H%M')

print('Starting run_models.py at', today)

subprocess.call(['python', 'operational_predict.py'])
