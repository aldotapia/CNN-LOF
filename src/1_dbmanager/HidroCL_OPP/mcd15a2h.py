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

hidrocl.prepare_path(hcl.mcd15a2h_path)

product_path = hcl.mcd15a2h_path

"""
Load databases
"""
print('Loading databases')

lai = hidrocl.HidroCLVariable("lai",
                              hcl.veg_o_modis_lai_mean,
                              hcl.veg_o_modis_lai_pc)

fpar = hidrocl.HidroCLVariable("fpar",
                               hcl.veg_o_modis_fpar_mean,
                               hcl.veg_o_modis_fpar_pc)

"""
Get last date of each database
"""
print('Getting last dates')

lastlai = pd.to_datetime(lai.observations.index, format='%Y-%m-%d').sort_values().max()
lastfpar = pd.to_datetime(fpar.observations.index, format='%Y-%m-%d').sort_values().max()

lastdates = [lastlai, lastfpar]

start = min(lastdates)
start = start + pd.Timedelta(days=8)

end = today

if start == end:
    print('No new data to download')
    sys.exit(4)

start = start.strftime('%Y-%m-%d')
end = end.strftime('%Y-%m-%d')

p = pd.period_range(pd.to_datetime(start, format="%Y-%m-%d"),
                    pd.to_datetime(end, format="%Y-%m-%d"), freq='D')

"""
Download data
"""
print('Downloading data')

hidrocl.download.earthdata_download('lai', product_path, start, end)

nfiles = len(os.listdir(product_path))

if nfiles == 0:
    print('No new files to process')
    sys.exit(0)

"""
Extract data
"""
print('Extracting data')

mcd15 = hidrocl.Mcd15a2h(lai, fpar,
                         product_path=hcl.mcd15a2h_path,
                         vector_path=hcl.hidrocl_sinusoidal,
                         lai_log=hcl.log_veg_o_modis_lai_mean,
                         fpar_log=hcl.log_veg_o_modis_fpar_mean)

mcd15.run_extraction()

if 'tempdir' in locals():
    if tempdir.name == hidrocl.processing_path:
        shutil.rmtree(product_path)

print('Done')
