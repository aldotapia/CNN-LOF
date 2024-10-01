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

hidrocl.prepare_path(hcl.mod13q1_path)

product_path = hcl.mod13q1_path

"""
Load databases
"""
print('Loading databases')

ndvi = hidrocl.HidroCLVariable("ndvi",
                               hcl.veg_o_modis_ndvi_mean_b_d16_p0d,
                               hcl.veg_o_modis_ndvi_mean_pc)

evi = hidrocl.HidroCLVariable("evi",
                              hcl.veg_o_modis_evi_mean_b_d16_p0d,
                              hcl.veg_o_modis_evi_mean_pc)

nbr = hidrocl.HidroCLVariable("nbr",
                              hcl.veg_o_int_nbr_mean_b_d16_p0d,
                              hcl.veg_o_int_nbr_mean_pc)

agr = hidrocl.HidroCLVariable('agr NDVI',
                              hcl.veg_o_modis_agr_mean_b_none_d1_p0d,
                              hcl.veg_o_modis_agr_mean_b_pc)

"""
Get last date of each database
"""
print('Getting last dates')

lastndvi = pd.to_datetime(ndvi.observations.index, format='%Y-%m-%d').sort_values().max()
lastevi = pd.to_datetime(evi.observations.index, format='%Y-%m-%d').sort_values().max()
lastnbr = pd.to_datetime(nbr.observations.index, format='%Y-%m-%d').sort_values().max()
lastagr = pd.to_datetime(agr.observations.index, format='%Y-%m-%d').sort_values().max()

lastdates = [lastndvi, lastevi, lastnbr, lastagr]

start = min(lastdates)
start = start + pd.Timedelta(days=16)

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

hidrocl.download.earthdata_download('vegetation', product_path, start, end)

nfiles = len(os.listdir(product_path))

if nfiles == 0:
    print('No new files to process')
    sys.exit(0)

"""
Extract data
"""
print('Extracting data')

mod13 = hidrocl.Mod13q1(ndvi, evi, nbr,
                        product_path=hcl.mod13q1_path,
                        vector_path=hcl.hidrocl_sinusoidal,
                        ndvi_log=hcl.log_veg_o_modis_ndvi_mean,
                        evi_log=hcl.log_veg_o_modis_evi_mean,
                        nbr_log=hcl.log_veg_o_int_nbr_mean)

agrndvi = hidrocl.Mod13q1agr(ndvi=agr, product_path=hcl.mod13q1_path,
                             vector_path=hcl.hidrocl_agr_sinu,
                             ndvi_log=hcl.log_veg_o_agr_ndvi_mean)

mod13.run_extraction()
agrndvi.run_extraction()

if 'tempdir' in locals():
    if tempdir.name == hidrocl.processing_path:
        shutil.rmtree(product_path)

print('Done')
