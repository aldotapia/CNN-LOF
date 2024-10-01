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

hidrocl.prepare_path(hcl.mod10a2_path)

product_path = hcl.mod10a2_path

"""
Load databases
"""
print('Loading databases')

nsnow = hidrocl.HidroCLVariable("nsnow",
                                hcl.snw_o_modis_sca_cum_n_d8_p0d,
                                hcl.snw_o_modis_sca_cum_n_pc)

ssnow = hidrocl.HidroCLVariable("ssnow",
                                hcl.snw_o_modis_sca_cum_s_d8_p0d,
                                hcl.snw_o_modis_sca_cum_s_pc)

"""
Get last date of each database
"""
print('Getting last dates')

lastnsnow = pd.to_datetime(nsnow.observations.index, format='%Y-%m-%d').sort_values().max()
lastssnow = pd.to_datetime(ssnow.observations.index, format='%Y-%m-%d').sort_values().max()

lastdates = [lastnsnow, lastssnow]

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

hidrocl.download.earthdata_download('snow', product_path, start, end)

nfiles = len(os.listdir(product_path))

if nfiles == 0:
    print('No new files to process')
    sys.exit(0)

"""
Extract data
"""
print('Extracting data')

mod10 = hidrocl.Mod10a2(nsnow, ssnow,
                        product_path=hcl.mod10a2_path,
                        north_vector_path=hcl.hidrocl_north,
                        south_vector_path=hcl.hidrocl_south,
                        snow_log=hcl.log_snw_o_modis_sca_cum)

mod10.run_extraction()

if 'tempdir' in locals():
    if tempdir.name == hidrocl.processing_path:
        shutil.rmtree(product_path)

print('Done')
