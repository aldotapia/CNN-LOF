import os
import shutil
import sys

import pandas as pd

import hidrocl
import hidrocl.paths as hcl
#from config import project_path
import dotenv

dotenv.load_dotenv()
project_path = os.getenv('PROJECT_PATH')

"""
Set the project path and the processing path
"""
print('Setting paths')
ppath = project_path

today = hidrocl.get_today_date()

hidrocl.set_project_path(ppath)

tempdir = hidrocl.temporal_directory()

hidrocl.set_processing_path(tempdir.name)

hidrocl.prepare_path(hcl.pdirnow)

product_path = hcl.pdirnow

"""
Load databases
"""
print('Loading databases')

pp = hidrocl.HidroCLVariable("pp",
                             hcl.pp_o_pdir_pp_mean_b_none_d1_p0d,
                             hcl.pp_o_pdir_pp_mean_b_pc)

"""
Get last date of each database
"""
print('Getting last dates')

if len(pp.observations.index) == 0:
    print('No data in database')
    sys.exit(1)

start = pd.to_datetime(pp.observations.index, format='%Y-%m-%d').sort_values().max()
start = start + pd.Timedelta(days=1)

end = today

if start == end:
    print('No new data to download')
    sys.exit(4)

start = start.strftime('%Y-%m-%d')
end = end.strftime('%Y-%m-%d')

"""
Download data
"""
print('Downloading data')

hidrocl.download.download_pdirnow(start, end, hcl.pdirnow, check_ppath=True)

nfiles = len(os.listdir(product_path))

if nfiles == 0:
    print('No new files to process')
    sys.exit(0)

"""
Extract data
"""
print('Extracting data')

pdirnow = hidrocl.Pdirnow(pp, product_path=hcl.pdirnow,
                          vector_path=hcl.hidrocl_wgs84,
                          pp_log=hcl.log_pp_o_pdir_pp_mean)

pdirnow.run_extraction()

if 'tempdir' in locals():
    if tempdir.name == hidrocl.processing_path:
        shutil.rmtree(product_path)

print('Done')
