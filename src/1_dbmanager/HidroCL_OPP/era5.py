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

hidrocl.prepare_path(hcl.era5_hourly_path)
hidrocl.prepare_path(hcl.era5_relative_humidity_path)

product_path = hcl.era5_hourly_path

"""
Load databases
"""
print('Loading databases')

pp = hidrocl.HidroCLVariable("pp",
                             hcl.pp_o_era5_pp_mean,
                             hcl.pp_o_era5_pp_pc)

temp = hidrocl.HidroCLVariable("temp",
                               hcl.tmp_o_era5_tmp_mean,
                               hcl.tmp_o_era5_tmp_pc)

tempmin = hidrocl.HidroCLVariable("tempmin",
                                  hcl.tmp_o_era5_tmin_mean,
                                  hcl.tmp_o_era5_tmin_pc)

tempmax = hidrocl.HidroCLVariable("tempmax",
                                  hcl.tmp_o_era5_tmax_mean,
                                  hcl.tmp_o_era5_tmax_pc)

dew = hidrocl.HidroCLVariable("dew",
                              hcl.tmp_o_era5_dew_mean,
                              hcl.tmp_o_era5_dew_pc)

pres = hidrocl.HidroCLVariable("pres",
                               hcl.atm_o_era5_pres_mean,
                               hcl.atm_o_era5_pres_pc)

u = hidrocl.HidroCLVariable("u",
                            hcl.atm_o_era5_uw_mean,
                            hcl.atm_o_era5_uw_pc)

v = hidrocl.HidroCLVariable("v",
                            hcl.atm_o_era5_vw_mean,
                            hcl.atm_o_era5_vw_pc)

pplen = hidrocl.HidroCLVariable("pp",
                                hcl.pp_o_era5_plen_mean_b_d1_p0d,
                                hcl.pp_o_era5_plen_mean_b_pc)

maxpp = hidrocl.HidroCLVariable("maxpp",
                                hcl.pp_o_era5_maxpp_mean,
                                hcl.pp_o_era5_maxpp_pc)

rh = hidrocl.HidroCLVariable("rh",
                             hcl.awc_o_era5_rh_mean_b_none_d1_p0d,
                             hcl.awc_o_era5_rh_mean_b_pc)

"""
Get last date of each database
"""
print('Getting last dates')

lastpp = pd.to_datetime(pp.observations.index, format='%Y-%m-%d').sort_values().max()
lasttemp = pd.to_datetime(temp.observations.index, format='%Y-%m-%d').sort_values().max()
lasttempmin = pd.to_datetime(tempmin.observations.index, format='%Y-%m-%d').sort_values().max()
lasttempmax = pd.to_datetime(tempmax.observations.index, format='%Y-%m-%d').sort_values().max()
lastdew = pd.to_datetime(dew.observations.index, format='%Y-%m-%d').sort_values().max()
lastpres = pd.to_datetime(pres.observations.index, format='%Y-%m-%d').sort_values().max()
lastu = pd.to_datetime(u.observations.index, format='%Y-%m-%d').sort_values().max()
lastv = pd.to_datetime(v.observations.index, format='%Y-%m-%d').sort_values().max()
lastpplen = pd.to_datetime(pplen.observations.index, format='%Y-%m-%d').sort_values().max()
lastmaxpp = pd.to_datetime(maxpp.observations.index, format='%Y-%m-%d').sort_values().max()
lastrh = pd.to_datetime(rh.observations.index, format='%Y-%m-%d').sort_values().max()

lastdates = [lastpp, lasttemp, lasttempmin, lasttempmax, lastdew, lastpres, lastu, lastv, lastpplen, lastmaxpp, lastrh]

start = min(lastdates)
start = start + pd.Timedelta(days=1)

end = today
# minus 5 days to account for the delay in the ERA5 data
end = end - pd.Timedelta(days=6)

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

for i in p:
    print(i)

    year = int(i.strftime('%Y'))
    month = int(i.strftime('%m'))
    day = int(i.strftime('%d'))

    fname = f'era5_{year:04d}{month:02d}{day:02d}.nc'
    file = Path(os.path.join(product_path, f'{year:04d}', fname))

    if file.is_file():
        print('already downloaded')
    else:
        try:
            print('downloading')
            hidrocl.download.download_era5(year=year,
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
Compute RH
"""
print('Computing RH')

era5pre = hidrocl.preprocess.Era5_pre_rh(product_path=hcl.era5_hourly_path,
                                         output_path=hcl.era5_relative_humidity_path)
era5pre.run_extraction()

"""
Extract data
"""
print('Extracting data')

era5 = hidrocl.Era5(pp=pp,
                    temp=temp, tempmin=tempmin, tempmax=tempmax,
                    dew=dew, pres=pres, u=u, v=v,
                    pp_log=hcl.log_pp_o_era5_pp_mean,
                    temp_log=hcl.log_tmp_o_era5_tmp_mean,
                    tempmin_log=hcl.log_tmp_o_era5_tmin_mean,
                    tempmax_log=hcl.log_tmp_o_era5_tmax_mean,
                    dew_log=hcl.log_tmp_o_era5_dew_mean,
                    pres_log=hcl.log_atm_o_era5_pres_mean,
                    u_log=hcl.log_wind_o_era5_u10_mean,
                    v_log=hcl.log_wind_o_era5_v10_mean,
                    product_path=hcl.era5_hourly_path,
                    vector_path=hcl.hidrocl_wgs84)

era5pplen = hidrocl.Era5pplen(pplen=pplen,
                              product_path=hcl.era5_hourly_path,
                              vector_path=hcl.hidrocl_wgs84,
                              pplen_log=hcl.log_pp_o_era5_plen_mean)

era5maxpp = hidrocl.Era5ppmax(ppmax=maxpp, ppmax_log=hcl.log_pp_o_era5_maxpp_mean,
                              product_path=hcl.era5_hourly_path,
                              vector_path=hcl.hidrocl_wgs84)

era5rh = hidrocl.Era5_rh(rh=rh,
                         rh_log=hcl.log_awc_f_gfs_rh_mean_log,
                         product_path=hcl.era5_relative_humidity_path,
                         vector_path=hcl.hidrocl_wgs84)

era5.run_extraction()
era5pplen.run_extraction()
era5maxpp.run_extraction()
era5rh.run_extraction()

if 'tempdir' in locals():
    if tempdir.name == hidrocl.processing_path:
        shutil.rmtree(product_path)

print('Done')
