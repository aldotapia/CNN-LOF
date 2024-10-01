import glob
import os
import shutil
import sys
from pathlib import Path

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

hidrocl.prepare_path(hcl.era5_pressure_levels_hourly_path)

product_path = hcl.era5_pressure_levels_hourly_path

"""
Load databases
"""
print('Loading databases')

gh = hidrocl.HidroCLVariable("gh",
                             hcl.atm_o_era5_z_mean_b_none_d1_p0d,
                             hcl.atm_o_era5_z_mean_b_pc)

"""
Get last date of each database
"""
print('Getting last dates')

start = pd.to_datetime(gh.observations.index, format='%Y-%m-%d').sort_values().max()
start = start + pd.Timedelta(days=1)

end = today
# minus 5 days to account for the delay in the ERA5 data
end = end - pd.Timedelta(days=6)

if start == end:
    print('No new data to download')
    sys.exit(0)

start = start.strftime('%Y-%m-%d')
end = end.strftime('%Y-%m-%d')

p = pd.period_range(pd.to_datetime(start, format="%Y-%m-%d"),
                    pd.to_datetime(end, format="%Y-%m-%d"), freq='D')

"""
Download data
"""
print('Downloading data')

for i in p:
    print(i)

    year = int(i.strftime('%Y'))
    month = int(i.strftime('%m'))
    day = int(i.strftime('%d'))

    fname = f'era5-pressure_{year:04d}{month:02d}{day:02d}.nc'
    file = Path(os.path.join(product_path, f'{year:04d}', fname))

    if file.is_file():
        print('already downloaded')
    else:
        try:
            print('downloading')
            hidrocl.download.download_era5pressure(year=year,
                                                   month=month,
                                                   day=day,
                                                   path=product_path)
        except Exception:
            print('day out of range')

"""
Move files to subfolders
"""
print('Moving files to subfolders')

os.chdir(product_path)

nfiles = len(os.listdir(product_path))

if nfiles == 0:
    print('No new files to process')
    sys.exit(0)

for file in glob.glob("*.nc"):
    # get future subfolder from name
    folder = file.split("_")[1][:4]
    # copy or create the folder if fails
    os.makedirs(os.path.join(product_path, folder), exist_ok=True)
    shutil.copyfile(file, os.path.join(product_path, folder, file))
    # remove file from old folder
    os.remove(file)
    print(f'Done with {file}')

os.chdir(hidrocl.project_path)

"""
Extract data
"""
print('Extracting data')

era5 = hidrocl.Era5_pressure(z=gh,
                             z_log=hcl.log_atm_o_era5_z_mean,
                             product_path=hcl.era5_pressure_levels_hourly_path,
                             vector_path=hcl.hidrocl_wgs84)

era5.run_extraction()

if 'tempdir' in locals():
    if tempdir.name == hidrocl.processing_path:
        shutil.rmtree(product_path)

print('Done')
