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
imerg_usr = os.getenv('IMERG_USER')
imerg_pwd = os.getenv('IMERG_PWD')

"""
Set the project path and the processing path
"""
print('Setting paths')
ppath = project_path

today = hidrocl.get_today_date()

hidrocl.set_project_path(ppath)

tempdir = hidrocl.temporal_directory()

hidrocl.set_processing_path(tempdir.name)

hidrocl.prepare_path(hcl.imerggis_path)

product_path = hcl.imerggis_path

"""
Load databases
"""
print('Loading databases')

pp = hidrocl.HidroCLVariable("pp",
                             hcl.pp_o_imerg_pp_mean_b_d_p0d,
                             hcl.pp_o_imerg_pp_mean_b_pc)

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

start_ = start.strftime('%Y-%m')
end_ = end.strftime('%Y-%m')

start = start.strftime('%Y-%m-%d')
end = end.strftime('%Y-%m-%d')

"""
Download data
"""
print('Downloading data')

files = hidrocl.download.get_imerg(str(start_), str(end_), imerg_usr,
                                   imerg_pwd, timeout=120)

dates = [val.split('/')[-1].split('.')[4].split('-')[0] for val in files]
dates = pd.to_datetime(dates, format='%Y%m%d')
files = [files[i] for i in range(len(files)) if dates[i] >= pd.to_datetime(start, format='%Y-%m-%d')]
files = [files[i] for i in range(len(files)) if dates[i] < pd.to_datetime(end, format='%Y-%m-%d')]

if len(files) < 48:
    print(f'Insufficient data to download and process. Files: {len(files)})')
    sys.exit(5)

for file in files:

    year = file.split('/')[-1].split('.')[4][:4]
    ffile = Path(os.path.join(product_path, year, file.split('/')[-1]))

    if ffile.is_file():
        print('already downloaded')
    else:
        try:
            hidrocl.download.download_imerg(file, product_path, 'hidrocl@meteo.uv.cl', 'hidrocl@meteo.uv.cl',
                                            timeout=120)
        except:
            print('Error downloading file: ', file)
            continue

"""
Move files to subfolders
"""
print('Moving files to subfolders')

os.chdir(product_path)

nfiles = len(os.listdir(product_path))

if nfiles == 0:
    print('No new files to process')
    sys.exit(0)

for file in glob.glob("*.tif"):
    # get future subfolder from name
    folder = file.split(".")[4][:4]
    hidrocl.prepare_path(os.path.join(product_path, folder))
    # copy file to new folder using shutil.copyfile
    shutil.copyfile(file, os.path.join(folder, file))
    # remove file from old folder
    os.remove(file)
    print(f'Done with {file}')

os.chdir(hidrocl.project_path)

"""
Extract data
"""
print('Extracting data')

imerg = hidrocl.ImergGIS(pp,
                         product_path=product_path,
                         vector_path=hcl.hidrocl_wgs84,
                         pp_log=hcl.log_pp_o_imerg_pp_mean)

imerg.run_extraction()

if 'tempdir' in locals():
    if tempdir.name == hidrocl.processing_path:
        shutil.rmtree(product_path)

print('Done')
