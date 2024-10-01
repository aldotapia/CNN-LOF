import shutil
import sys

import pandas as pd

import hidrocl
import hidrocl.paths as hcl
#from config import project_path
import os
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

print(tempdir.name)

hidrocl.prepare_path(hcl.gfs)

product_path = hcl.gfs

"""
Load databases and get last date
"""
print('Loading databases and getting last date')

mins = []

"""
Geopotential height
"""

gfs_d0 = hidrocl.HidroCLVariable('test gfs día 0',
                                 hcl.atm_f_gfs_gh_mean_b_none_d1_p0d,
                                 hcl.atm_f_gfs_gh_mean_pc_p0d)
gfs_d1 = hidrocl.HidroCLVariable('test gfs día 1',
                                 hcl.atm_f_gfs_gh_mean_b_none_d1_p1d,
                                 hcl.atm_f_gfs_gh_mean_pc_p1d)
gfs_d2 = hidrocl.HidroCLVariable('test gfs día 2',
                                 hcl.atm_f_gfs_gh_mean_b_none_d1_p2d,
                                 hcl.atm_f_gfs_gh_mean_pc_p2d)
gfs_d3 = hidrocl.HidroCLVariable('test gfs día 3',
                                 hcl.atm_f_gfs_gh_mean_b_none_d1_p3d,
                                 hcl.atm_f_gfs_gh_mean_pc_p3d)
gfs_d4 = hidrocl.HidroCLVariable('test gfs día 4',
                                 hcl.atm_f_gfs_gh_mean_b_none_d1_p4d,
                                 hcl.atm_f_gfs_gh_mean_pc_p4d)

lastgfs_d0 = pd.to_datetime(gfs_d0.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d1 = pd.to_datetime(gfs_d1.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d2 = pd.to_datetime(gfs_d2.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d3 = pd.to_datetime(gfs_d3.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d4 = pd.to_datetime(gfs_d4.observations.index, format='%Y-%m-%d').sort_values().max()

lastdates = [lastgfs_d0, lastgfs_d1, lastgfs_d2, lastgfs_d3, lastgfs_d4]
mins.append(min(lastdates))

gfs_d0 = hidrocl.HidroCLVariable('test gfs día 0',
                                 hcl.pp_f_gfs_plen_mean_b_none_d1_p0d,
                                 hcl.pp_f_gfs_plen_mean_pc_p0d)
gfs_d1 = hidrocl.HidroCLVariable('test gfs día 1',
                                 hcl.pp_f_gfs_plen_mean_b_none_d1_p1d,
                                 hcl.pp_f_gfs_plen_mean_pc_p1d)
gfs_d2 = hidrocl.HidroCLVariable('test gfs día 2',
                                 hcl.pp_f_gfs_plen_mean_b_none_d1_p2d,
                                 hcl.pp_f_gfs_plen_mean_pc_p2d)
gfs_d3 = hidrocl.HidroCLVariable('test gfs día 3',
                                 hcl.pp_f_gfs_plen_mean_b_none_d1_p3d,
                                 hcl.pp_f_gfs_plen_mean_pc_p3d)
gfs_d4 = hidrocl.HidroCLVariable('test gfs día 4',
                                 hcl.pp_f_gfs_plen_mean_b_none_d1_p4d,
                                 hcl.pp_f_gfs_plen_mean_pc_p4d)

lastgfs_d0 = pd.to_datetime(gfs_d0.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d1 = pd.to_datetime(gfs_d1.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d2 = pd.to_datetime(gfs_d2.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d3 = pd.to_datetime(gfs_d3.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d4 = pd.to_datetime(gfs_d4.observations.index, format='%Y-%m-%d').sort_values().max()

lastdates = [lastgfs_d0, lastgfs_d1, lastgfs_d2, lastgfs_d3, lastgfs_d4]
mins.append(min(lastdates))

gfs_d0 = hidrocl.HidroCLVariable('test gfs día 0',
                                 hcl.pp_f_gfs_pp_max_b_none_d1_p0d,
                                 hcl.pp_f_gfs_pp_max_pc_p0d)
gfs_d1 = hidrocl.HidroCLVariable('test gfs día 1',
                                 hcl.pp_f_gfs_pp_max_b_none_d1_p1d,
                                 hcl.pp_f_gfs_pp_max_pc_p1d)
gfs_d2 = hidrocl.HidroCLVariable('test gfs día 2',
                                 hcl.pp_f_gfs_pp_max_b_none_d1_p2d,
                                 hcl.pp_f_gfs_pp_max_pc_p2d)
gfs_d3 = hidrocl.HidroCLVariable('test gfs día 3',
                                 hcl.pp_f_gfs_pp_max_b_none_d1_p3d,
                                 hcl.pp_f_gfs_pp_max_pc_p3d)
gfs_d4 = hidrocl.HidroCLVariable('test gfs día 4',
                                 hcl.pp_f_gfs_pp_max_b_none_d1_p4d,
                                 hcl.pp_f_gfs_pp_max_pc_p4d)

lastgfs_d0 = pd.to_datetime(gfs_d0.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d1 = pd.to_datetime(gfs_d1.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d2 = pd.to_datetime(gfs_d2.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d3 = pd.to_datetime(gfs_d3.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d4 = pd.to_datetime(gfs_d4.observations.index, format='%Y-%m-%d').sort_values().max()

lastdates = [lastgfs_d0, lastgfs_d1, lastgfs_d2, lastgfs_d3, lastgfs_d4]
mins.append(min(lastdates))

gfs_d0 = hidrocl.HidroCLVariable('test gfs día 0',
                                 hcl.pp_f_gfs_pp_mean_b_none_d1_p0d,
                                 hcl.pp_f_gfs_pp_mean_pc_p0d)
gfs_d1 = hidrocl.HidroCLVariable('test gfs día 1',
                                 hcl.pp_f_gfs_pp_mean_b_none_d1_p1d,
                                 hcl.pp_f_gfs_pp_mean_pc_p1d)
gfs_d2 = hidrocl.HidroCLVariable('test gfs día 2',
                                 hcl.pp_f_gfs_pp_mean_b_none_d1_p2d,
                                 hcl.pp_f_gfs_pp_mean_pc_p2d)
gfs_d3 = hidrocl.HidroCLVariable('test gfs día 3',
                                 hcl.pp_f_gfs_pp_mean_b_none_d1_p3d,
                                 hcl.pp_f_gfs_pp_mean_pc_p3d)
gfs_d4 = hidrocl.HidroCLVariable('test gfs día 4',
                                 hcl.pp_f_gfs_pp_mean_b_none_d1_p4d,
                                 hcl.pp_f_gfs_pp_mean_pc_p4d)

lastgfs_d0 = pd.to_datetime(gfs_d0.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d1 = pd.to_datetime(gfs_d1.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d2 = pd.to_datetime(gfs_d2.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d3 = pd.to_datetime(gfs_d3.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d4 = pd.to_datetime(gfs_d4.observations.index, format='%Y-%m-%d').sort_values().max()

lastdates = [lastgfs_d0, lastgfs_d1, lastgfs_d2, lastgfs_d3, lastgfs_d4]
mins.append(min(lastdates))

gfs_d0 = hidrocl.HidroCLVariable('test gfs día 0',
                                 hcl.awc_f_gfs_rh_mean_b_none_d1_p0d,
                                 hcl.awc_f_gfs_rh_mean_pc_p0d)
gfs_d1 = hidrocl.HidroCLVariable('test gfs día 1',
                                 hcl.awc_f_gfs_rh_mean_b_none_d1_p1d,
                                 hcl.awc_f_gfs_rh_mean_pc_p1d)
gfs_d2 = hidrocl.HidroCLVariable('test gfs día 2',
                                 hcl.awc_f_gfs_rh_mean_b_none_d1_p2d,
                                 hcl.awc_f_gfs_rh_mean_pc_p2d)
gfs_d3 = hidrocl.HidroCLVariable('test gfs día 3',
                                 hcl.awc_f_gfs_rh_mean_b_none_d1_p3d,
                                 hcl.awc_f_gfs_rh_mean_pc_p3d)
gfs_d4 = hidrocl.HidroCLVariable('test gfs día 4',
                                 hcl.awc_f_gfs_rh_mean_b_none_d1_p4d,
                                 hcl.awc_f_gfs_rh_mean_pc_p4d)

lastgfs_d0 = pd.to_datetime(gfs_d0.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d1 = pd.to_datetime(gfs_d1.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d2 = pd.to_datetime(gfs_d2.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d3 = pd.to_datetime(gfs_d3.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d4 = pd.to_datetime(gfs_d4.observations.index, format='%Y-%m-%d').sort_values().max()

lastdates = [lastgfs_d0, lastgfs_d1, lastgfs_d2, lastgfs_d3, lastgfs_d4]
mins.append(min(lastdates))

gfs_d0 = hidrocl.HidroCLVariable('test gfs día 0',
                                 hcl.tmp_f_gfs_tmp_max_b_none_d1_p0d,
                                 hcl.tmp_f_gfs_tmp_max_pc_p0d)
gfs_d1 = hidrocl.HidroCLVariable('test gfs día 1',
                                 hcl.tmp_f_gfs_tmp_max_b_none_d1_p1d,
                                 hcl.tmp_f_gfs_tmp_max_pc_p1d)
gfs_d2 = hidrocl.HidroCLVariable('test gfs día 2',
                                 hcl.tmp_f_gfs_tmp_max_b_none_d1_p2d,
                                 hcl.tmp_f_gfs_tmp_max_pc_p2d)
gfs_d3 = hidrocl.HidroCLVariable('test gfs día 3',
                                 hcl.tmp_f_gfs_tmp_max_b_none_d1_p3d,
                                 hcl.tmp_f_gfs_tmp_max_pc_p3d)
gfs_d4 = hidrocl.HidroCLVariable('test gfs día 4',
                                 hcl.tmp_f_gfs_tmp_max_b_none_d1_p4d,
                                 hcl.tmp_f_gfs_tmp_max_pc_p4d)

lastgfs_d0 = pd.to_datetime(gfs_d0.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d1 = pd.to_datetime(gfs_d1.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d2 = pd.to_datetime(gfs_d2.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d3 = pd.to_datetime(gfs_d3.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d4 = pd.to_datetime(gfs_d4.observations.index, format='%Y-%m-%d').sort_values().max()

lastdates = [lastgfs_d0, lastgfs_d1, lastgfs_d2, lastgfs_d3, lastgfs_d4]
mins.append(min(lastdates))

gfs_d0 = hidrocl.HidroCLVariable('test gfs día 0',
                                 hcl.tmp_f_gfs_tmp_mean_b_none_d1_p0d,
                                 hcl.tmp_f_gfs_tmp_mean_pc_p0d)
gfs_d1 = hidrocl.HidroCLVariable('test gfs día 1',
                                 hcl.tmp_f_gfs_tmp_mean_b_none_d1_p1d,
                                 hcl.tmp_f_gfs_tmp_mean_pc_p1d)
gfs_d2 = hidrocl.HidroCLVariable('test gfs día 2',
                                 hcl.tmp_f_gfs_tmp_mean_b_none_d1_p2d,
                                 hcl.tmp_f_gfs_tmp_mean_pc_p2d)
gfs_d3 = hidrocl.HidroCLVariable('test gfs día 3',
                                 hcl.tmp_f_gfs_tmp_mean_b_none_d1_p3d,
                                 hcl.tmp_f_gfs_tmp_mean_pc_p3d)
gfs_d4 = hidrocl.HidroCLVariable('test gfs día 4',
                                 hcl.tmp_f_gfs_tmp_mean_b_none_d1_p4d,
                                 hcl.tmp_f_gfs_tmp_mean_pc_p4d)

lastgfs_d0 = pd.to_datetime(gfs_d0.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d1 = pd.to_datetime(gfs_d1.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d2 = pd.to_datetime(gfs_d2.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d3 = pd.to_datetime(gfs_d3.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d4 = pd.to_datetime(gfs_d4.observations.index, format='%Y-%m-%d').sort_values().max()

lastdates = [lastgfs_d0, lastgfs_d1, lastgfs_d2, lastgfs_d3, lastgfs_d4]
mins.append(min(lastdates))

gfs_d0 = hidrocl.HidroCLVariable('test gfs día 0',
                                 hcl.tmp_f_gfs_tmp_min_b_none_d1_p0d,
                                 hcl.tmp_f_gfs_tmp_min_pc_p0d)
gfs_d1 = hidrocl.HidroCLVariable('test gfs día 1',
                                 hcl.tmp_f_gfs_tmp_min_b_none_d1_p1d,
                                 hcl.tmp_f_gfs_tmp_min_pc_p1d)
gfs_d2 = hidrocl.HidroCLVariable('test gfs día 2',
                                 hcl.tmp_f_gfs_tmp_min_b_none_d1_p2d,
                                 hcl.tmp_f_gfs_tmp_min_pc_p2d)
gfs_d3 = hidrocl.HidroCLVariable('test gfs día 3',
                                 hcl.tmp_f_gfs_tmp_min_b_none_d1_p3d,
                                 hcl.tmp_f_gfs_tmp_min_pc_p3d)
gfs_d4 = hidrocl.HidroCLVariable('test gfs día 4',
                                 hcl.tmp_f_gfs_tmp_min_b_none_d1_p4d,
                                 hcl.tmp_f_gfs_tmp_min_pc_p4d)

lastgfs_d0 = pd.to_datetime(gfs_d0.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d1 = pd.to_datetime(gfs_d1.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d2 = pd.to_datetime(gfs_d2.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d3 = pd.to_datetime(gfs_d3.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d4 = pd.to_datetime(gfs_d4.observations.index, format='%Y-%m-%d').sort_values().max()

lastdates = [lastgfs_d0, lastgfs_d1, lastgfs_d2, lastgfs_d3, lastgfs_d4]
mins.append(min(lastdates))

gfs_d0 = hidrocl.HidroCLVariable('test gfs día 0',
                                 hcl.atm_f_gfs_uw_mean_b_none_d1_p0d,
                                 hcl.atm_f_gfs_uw_mean_pc_p0d)
gfs_d1 = hidrocl.HidroCLVariable('test gfs día 1',
                                 hcl.atm_f_gfs_uw_mean_b_none_d1_p1d,
                                 hcl.atm_f_gfs_uw_mean_pc_p1d)
gfs_d2 = hidrocl.HidroCLVariable('test gfs día 2',
                                 hcl.atm_f_gfs_uw_mean_b_none_d1_p2d,
                                 hcl.atm_f_gfs_uw_mean_pc_p2d)
gfs_d3 = hidrocl.HidroCLVariable('test gfs día 3',
                                 hcl.atm_f_gfs_uw_mean_b_none_d1_p3d,
                                 hcl.atm_f_gfs_uw_mean_pc_p3d)
gfs_d4 = hidrocl.HidroCLVariable('test gfs día 4',
                                 hcl.atm_f_gfs_uw_mean_b_none_d1_p4d,
                                 hcl.atm_f_gfs_uw_mean_pc_p4d)

lastgfs_d0 = pd.to_datetime(gfs_d0.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d1 = pd.to_datetime(gfs_d1.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d2 = pd.to_datetime(gfs_d2.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d3 = pd.to_datetime(gfs_d3.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d4 = pd.to_datetime(gfs_d4.observations.index, format='%Y-%m-%d').sort_values().max()

lastdates = [lastgfs_d0, lastgfs_d1, lastgfs_d2, lastgfs_d3, lastgfs_d4]
mins.append(min(lastdates))

gfs_d0 = hidrocl.HidroCLVariable('test gfs día 0',
                                 hcl.atm_f_gfs_vw_mean_b_none_d1_p0d,
                                 hcl.atm_f_gfs_vw_mean_pc_p0d)
gfs_d1 = hidrocl.HidroCLVariable('test gfs día 1',
                                 hcl.atm_f_gfs_vw_mean_b_none_d1_p1d,
                                 hcl.atm_f_gfs_vw_mean_pc_p1d)
gfs_d2 = hidrocl.HidroCLVariable('test gfs día 2',
                                 hcl.atm_f_gfs_vw_mean_b_none_d1_p2d,
                                 hcl.atm_f_gfs_vw_mean_pc_p2d)
gfs_d3 = hidrocl.HidroCLVariable('test gfs día 3',
                                 hcl.atm_f_gfs_vw_mean_b_none_d1_p3d,
                                 hcl.atm_f_gfs_vw_mean_pc_p3d)
gfs_d4 = hidrocl.HidroCLVariable('test gfs día 4',
                                 hcl.atm_f_gfs_vw_mean_b_none_d1_p4d,
                                 hcl.atm_f_gfs_vw_mean_pc_p4d)

lastgfs_d0 = pd.to_datetime(gfs_d0.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d1 = pd.to_datetime(gfs_d1.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d2 = pd.to_datetime(gfs_d2.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d3 = pd.to_datetime(gfs_d3.observations.index, format='%Y-%m-%d').sort_values().max()
lastgfs_d4 = pd.to_datetime(gfs_d4.observations.index, format='%Y-%m-%d').sort_values().max()

lastdates = [lastgfs_d0, lastgfs_d1, lastgfs_d2, lastgfs_d3, lastgfs_d4]
mins.append(min(lastdates))

start = min(mins)

if start == today:
    print('Database is up to date')
    sys.exit(4)

start = start + pd.Timedelta(days=1)

start = start.strftime('%Y-%m-%d')
end = today.strftime('%Y-%m-%d')

p = pd.period_range(pd.to_datetime(start, format="%Y-%m-%d"),
                    pd.to_datetime(end, format="%Y-%m-%d"), freq='D')

"""
Download data
"""

urls = hidrocl.download.list_gfs()

available_dates = [pd.to_datetime(val.split('/')[-2].split('gfs')[-1], format="%Y%m%d") for val in urls]

"""
Exit code=2 if today's data is not available
"""
if max(available_dates) < today:
    print('No new data to download')
    sys.exit(3)

pdate = [pd.to_datetime(val.strftime('%Y-%m-%d'), format="%Y-%m-%d") for val in p]
missing_dates = [val for val in pdate if val in available_dates]
missing_urls = [val for val in urls if
                pd.to_datetime(val.split('/')[-2].split('gfs')[-1], format="%Y%m%d") in missing_dates]

# print missing_urls for reference
print(missing_urls)

for url in missing_urls:
    date = url.split('/')[-2].replace('gfs', '')
    try:
        hidrocl.download.download_gfs(url, product_path)
        print('Download done')
    except ValueError:
        print("Fail")
        sys.exit(3)
    except OSError:
        print("Server error")
        sys.exit(3)
    except TypeError:
        print("File error")
        sys.exit(3)
    except:
        print("Unknown error")
        sys.exit(3)

"""
Extract data
"""
print('Extracting data')

gfs_d0 = hidrocl.HidroCLVariable('test gfs día 0',
                                 hcl.atm_f_gfs_gh_mean_b_none_d1_p0d,
                                 hcl.atm_f_gfs_gh_mean_pc_p0d)
gfs_d1 = hidrocl.HidroCLVariable('test gfs día 1',
                                 hcl.atm_f_gfs_gh_mean_b_none_d1_p1d,
                                 hcl.atm_f_gfs_gh_mean_pc_p1d)
gfs_d2 = hidrocl.HidroCLVariable('test gfs día 2',
                                 hcl.atm_f_gfs_gh_mean_b_none_d1_p2d,
                                 hcl.atm_f_gfs_gh_mean_pc_p2d)
gfs_d3 = hidrocl.HidroCLVariable('test gfs día 3',
                                 hcl.atm_f_gfs_gh_mean_b_none_d1_p3d,
                                 hcl.atm_f_gfs_gh_mean_pc_p3d)
gfs_d4 = hidrocl.HidroCLVariable('test gfs día 4',
                                 hcl.atm_f_gfs_gh_mean_b_none_d1_p4d,
                                 hcl.atm_f_gfs_gh_mean_pc_p4d)

gfs = hidrocl.products.Gfs(db0=gfs_d0,
                           db1=gfs_d1,
                           db2=gfs_d2,
                           db3=gfs_d3,
                           db4=gfs_d4,
                           db_log=hcl.log_atm_f_gfs_gh_mean_log,
                           variable='gh',
                           aggregation='mean',
                           product_path=hcl.gfs,
                           vectorpath=hcl.hidrocl_wgs84)

gfs.run_extraction()

del gfs

gfs_d0 = hidrocl.HidroCLVariable('test gfs día 0',
                                 hcl.pp_f_gfs_plen_mean_b_none_d1_p0d,
                                 hcl.pp_f_gfs_plen_mean_pc_p0d)
gfs_d1 = hidrocl.HidroCLVariable('test gfs día 1',
                                 hcl.pp_f_gfs_plen_mean_b_none_d1_p1d,
                                 hcl.pp_f_gfs_plen_mean_pc_p1d)
gfs_d2 = hidrocl.HidroCLVariable('test gfs día 2',
                                 hcl.pp_f_gfs_plen_mean_b_none_d1_p2d,
                                 hcl.pp_f_gfs_plen_mean_pc_p2d)
gfs_d3 = hidrocl.HidroCLVariable('test gfs día 3',
                                 hcl.pp_f_gfs_plen_mean_b_none_d1_p3d,
                                 hcl.pp_f_gfs_plen_mean_pc_p3d)
gfs_d4 = hidrocl.HidroCLVariable('test gfs día 4',
                                 hcl.pp_f_gfs_plen_mean_b_none_d1_p4d,
                                 hcl.pp_f_gfs_plen_mean_pc_p4d)

gfs = hidrocl.products.Gfs(db0=gfs_d0,
                           db1=gfs_d1,
                           db2=gfs_d2,
                           db3=gfs_d3,
                           db4=gfs_d4,
                           db_log=hcl.log_pp_f_gfs_plen_mean_log,
                           variable='prate',
                           aggregation='len',
                           product_path=hcl.gfs,
                           vectorpath=hcl.hidrocl_wgs84)

gfs.run_extraction()

del gfs

gfs_d0 = hidrocl.HidroCLVariable('test gfs día 0',
                                 hcl.pp_f_gfs_pp_max_b_none_d1_p0d,
                                 hcl.pp_f_gfs_pp_max_pc_p0d)
gfs_d1 = hidrocl.HidroCLVariable('test gfs día 1',
                                 hcl.pp_f_gfs_pp_max_b_none_d1_p1d,
                                 hcl.pp_f_gfs_pp_max_pc_p1d)
gfs_d2 = hidrocl.HidroCLVariable('test gfs día 2',
                                 hcl.pp_f_gfs_pp_max_b_none_d1_p2d,
                                 hcl.pp_f_gfs_pp_max_pc_p2d)
gfs_d3 = hidrocl.HidroCLVariable('test gfs día 3',
                                 hcl.pp_f_gfs_pp_max_b_none_d1_p3d,
                                 hcl.pp_f_gfs_pp_max_pc_p3d)
gfs_d4 = hidrocl.HidroCLVariable('test gfs día 4',
                                 hcl.pp_f_gfs_pp_max_b_none_d1_p4d,
                                 hcl.pp_f_gfs_pp_max_pc_p4d)

gfs = hidrocl.products.Gfs(db0=gfs_d0,
                           db1=gfs_d1,
                           db2=gfs_d2,
                           db3=gfs_d3,
                           db4=gfs_d4,
                           db_log=hcl.log_pp_f_gfs_pp_max_log,
                           variable='prate',
                           aggregation='max',
                           product_path=hcl.gfs,
                           vectorpath=hcl.hidrocl_wgs84)

gfs.run_extraction()

del gfs

gfs_d0 = hidrocl.HidroCLVariable('test gfs día 0',
                                 hcl.pp_f_gfs_pp_mean_b_none_d1_p0d,
                                 hcl.pp_f_gfs_pp_mean_pc_p0d)
gfs_d1 = hidrocl.HidroCLVariable('test gfs día 1',
                                 hcl.pp_f_gfs_pp_mean_b_none_d1_p1d,
                                 hcl.pp_f_gfs_pp_mean_pc_p1d)
gfs_d2 = hidrocl.HidroCLVariable('test gfs día 2',
                                 hcl.pp_f_gfs_pp_mean_b_none_d1_p2d,
                                 hcl.pp_f_gfs_pp_mean_pc_p2d)
gfs_d3 = hidrocl.HidroCLVariable('test gfs día 3',
                                 hcl.pp_f_gfs_pp_mean_b_none_d1_p3d,
                                 hcl.pp_f_gfs_pp_mean_pc_p3d)
gfs_d4 = hidrocl.HidroCLVariable('test gfs día 4',
                                 hcl.pp_f_gfs_pp_mean_b_none_d1_p4d,
                                 hcl.pp_f_gfs_pp_mean_pc_p4d)

gfs = hidrocl.products.Gfs(db0=gfs_d0,
                           db1=gfs_d1,
                           db2=gfs_d2,
                           db3=gfs_d3,
                           db4=gfs_d4,
                           db_log=hcl.log_pp_f_gfs_pp_mean_log,
                           variable='prate',
                           aggregation='sum',
                           product_path=hcl.gfs,
                           vectorpath=hcl.hidrocl_wgs84)

gfs.run_extraction()

del gfs

gfs_d0 = hidrocl.HidroCLVariable('test gfs día 0',
                                 hcl.awc_f_gfs_rh_mean_b_none_d1_p0d,
                                 hcl.awc_f_gfs_rh_mean_pc_p0d)
gfs_d1 = hidrocl.HidroCLVariable('test gfs día 1',
                                 hcl.awc_f_gfs_rh_mean_b_none_d1_p1d,
                                 hcl.awc_f_gfs_rh_mean_pc_p1d)
gfs_d2 = hidrocl.HidroCLVariable('test gfs día 2',
                                 hcl.awc_f_gfs_rh_mean_b_none_d1_p2d,
                                 hcl.awc_f_gfs_rh_mean_pc_p2d)
gfs_d3 = hidrocl.HidroCLVariable('test gfs día 3',
                                 hcl.awc_f_gfs_rh_mean_b_none_d1_p3d,
                                 hcl.awc_f_gfs_rh_mean_pc_p3d)
gfs_d4 = hidrocl.HidroCLVariable('test gfs día 4',
                                 hcl.awc_f_gfs_rh_mean_b_none_d1_p4d,
                                 hcl.awc_f_gfs_rh_mean_pc_p4d)

gfs = hidrocl.products.Gfs(db0=gfs_d0,
                           db1=gfs_d1,
                           db2=gfs_d2,
                           db3=gfs_d3,
                           db4=gfs_d4,
                           db_log=hcl.log_awc_f_gfs_rh_mean_log,
                           variable='r2',
                           aggregation='mean',
                           product_path=hcl.gfs,
                           vectorpath=hcl.hidrocl_wgs84)

gfs.run_extraction()

del gfs

gfs_d0 = hidrocl.HidroCLVariable('test gfs día 0',
                                 hcl.tmp_f_gfs_tmp_max_b_none_d1_p0d,
                                 hcl.tmp_f_gfs_tmp_max_pc_p0d)
gfs_d1 = hidrocl.HidroCLVariable('test gfs día 1',
                                 hcl.tmp_f_gfs_tmp_max_b_none_d1_p1d,
                                 hcl.tmp_f_gfs_tmp_max_pc_p1d)
gfs_d2 = hidrocl.HidroCLVariable('test gfs día 2',
                                 hcl.tmp_f_gfs_tmp_max_b_none_d1_p2d,
                                 hcl.tmp_f_gfs_tmp_max_pc_p2d)
gfs_d3 = hidrocl.HidroCLVariable('test gfs día 3',
                                 hcl.tmp_f_gfs_tmp_max_b_none_d1_p3d,
                                 hcl.tmp_f_gfs_tmp_max_pc_p3d)
gfs_d4 = hidrocl.HidroCLVariable('test gfs día 4',
                                 hcl.tmp_f_gfs_tmp_max_b_none_d1_p4d,
                                 hcl.tmp_f_gfs_tmp_max_pc_p4d)

gfs = hidrocl.products.Gfs(db0=gfs_d0,
                           db1=gfs_d1,
                           db2=gfs_d2,
                           db3=gfs_d3,
                           db4=gfs_d4,
                           db_log=hcl.log_tmp_f_gfs_tmp_max_log,
                           variable='t2m',
                           aggregation='max',
                           product_path=hcl.gfs,
                           vectorpath=hcl.hidrocl_wgs84)

gfs.run_extraction()

del gfs

gfs_d0 = hidrocl.HidroCLVariable('test gfs día 0',
                                 hcl.tmp_f_gfs_tmp_mean_b_none_d1_p0d,
                                 hcl.tmp_f_gfs_tmp_mean_pc_p0d)
gfs_d1 = hidrocl.HidroCLVariable('test gfs día 1',
                                 hcl.tmp_f_gfs_tmp_mean_b_none_d1_p1d,
                                 hcl.tmp_f_gfs_tmp_mean_pc_p1d)
gfs_d2 = hidrocl.HidroCLVariable('test gfs día 2',
                                 hcl.tmp_f_gfs_tmp_mean_b_none_d1_p2d,
                                 hcl.tmp_f_gfs_tmp_mean_pc_p2d)
gfs_d3 = hidrocl.HidroCLVariable('test gfs día 3',
                                 hcl.tmp_f_gfs_tmp_mean_b_none_d1_p3d,
                                 hcl.tmp_f_gfs_tmp_mean_pc_p3d)
gfs_d4 = hidrocl.HidroCLVariable('test gfs día 4',
                                 hcl.tmp_f_gfs_tmp_mean_b_none_d1_p4d,
                                 hcl.tmp_f_gfs_tmp_mean_pc_p4d)

gfs = hidrocl.products.Gfs(db0=gfs_d0,
                           db1=gfs_d1,
                           db2=gfs_d2,
                           db3=gfs_d3,
                           db4=gfs_d4,
                           db_log=hcl.log_tmp_f_gfs_tmp_mean_log,
                           variable='t2m',
                           aggregation='mean',
                           product_path=hcl.gfs,
                           vectorpath=hcl.hidrocl_wgs84)

gfs.run_extraction()

del gfs

gfs_d0 = hidrocl.HidroCLVariable('test gfs día 0',
                                 hcl.tmp_f_gfs_tmp_min_b_none_d1_p0d,
                                 hcl.tmp_f_gfs_tmp_min_pc_p0d)
gfs_d1 = hidrocl.HidroCLVariable('test gfs día 1',
                                 hcl.tmp_f_gfs_tmp_min_b_none_d1_p1d,
                                 hcl.tmp_f_gfs_tmp_min_pc_p1d)
gfs_d2 = hidrocl.HidroCLVariable('test gfs día 2',
                                 hcl.tmp_f_gfs_tmp_min_b_none_d1_p2d,
                                 hcl.tmp_f_gfs_tmp_min_pc_p2d)
gfs_d3 = hidrocl.HidroCLVariable('test gfs día 3',
                                 hcl.tmp_f_gfs_tmp_min_b_none_d1_p3d,
                                 hcl.tmp_f_gfs_tmp_min_pc_p3d)
gfs_d4 = hidrocl.HidroCLVariable('test gfs día 4',
                                 hcl.tmp_f_gfs_tmp_min_b_none_d1_p4d,
                                 hcl.tmp_f_gfs_tmp_min_pc_p4d)

gfs = hidrocl.products.Gfs(db0=gfs_d0,
                           db1=gfs_d1,
                           db2=gfs_d2,
                           db3=gfs_d3,
                           db4=gfs_d4,
                           db_log=hcl.log_tmp_f_gfs_tmp_min_log,
                           variable='t2m',
                           aggregation='min',
                           product_path=hcl.gfs,
                           vectorpath=hcl.hidrocl_wgs84)

gfs.run_extraction()

del gfs

gfs_d0 = hidrocl.HidroCLVariable('test gfs día 0',
                                 hcl.atm_f_gfs_uw_mean_b_none_d1_p0d,
                                 hcl.atm_f_gfs_uw_mean_pc_p0d)
gfs_d1 = hidrocl.HidroCLVariable('test gfs día 1',
                                 hcl.atm_f_gfs_uw_mean_b_none_d1_p1d,
                                 hcl.atm_f_gfs_uw_mean_pc_p1d)
gfs_d2 = hidrocl.HidroCLVariable('test gfs día 2',
                                 hcl.atm_f_gfs_uw_mean_b_none_d1_p2d,
                                 hcl.atm_f_gfs_uw_mean_pc_p2d)
gfs_d3 = hidrocl.HidroCLVariable('test gfs día 3',
                                 hcl.atm_f_gfs_uw_mean_b_none_d1_p3d,
                                 hcl.atm_f_gfs_uw_mean_pc_p3d)
gfs_d4 = hidrocl.HidroCLVariable('test gfs día 4',
                                 hcl.atm_f_gfs_uw_mean_b_none_d1_p4d,
                                 hcl.atm_f_gfs_uw_mean_pc_p4d)

gfs = hidrocl.products.Gfs(db0=gfs_d0,
                           db1=gfs_d1,
                           db2=gfs_d2,
                           db3=gfs_d3,
                           db4=gfs_d4,
                           db_log=hcl.log_atm_f_gfs_uw_mean_log,
                           variable='u10',
                           aggregation='mean',
                           product_path=hcl.gfs,
                           vectorpath=hcl.hidrocl_wgs84)

gfs.run_extraction()

del gfs

gfs_d0 = hidrocl.HidroCLVariable('test gfs día 0',
                                 hcl.atm_f_gfs_vw_mean_b_none_d1_p0d,
                                 hcl.atm_f_gfs_vw_mean_pc_p0d)
gfs_d1 = hidrocl.HidroCLVariable('test gfs día 1',
                                 hcl.atm_f_gfs_vw_mean_b_none_d1_p1d,
                                 hcl.atm_f_gfs_vw_mean_pc_p1d)
gfs_d2 = hidrocl.HidroCLVariable('test gfs día 2',
                                 hcl.atm_f_gfs_vw_mean_b_none_d1_p2d,
                                 hcl.atm_f_gfs_vw_mean_pc_p2d)
gfs_d3 = hidrocl.HidroCLVariable('test gfs día 3',
                                 hcl.atm_f_gfs_vw_mean_b_none_d1_p3d,
                                 hcl.atm_f_gfs_vw_mean_pc_p3d)
gfs_d4 = hidrocl.HidroCLVariable('test gfs día 4',
                                 hcl.atm_f_gfs_vw_mean_b_none_d1_p4d,
                                 hcl.atm_f_gfs_vw_mean_pc_p4d)

gfs = hidrocl.products.Gfs(db0=gfs_d0,
                           db1=gfs_d1,
                           db2=gfs_d2,
                           db3=gfs_d3,
                           db4=gfs_d4,
                           db_log=hcl.log_atm_f_gfs_vw_mean_log,
                           variable='v10',
                           aggregation='mean',
                           product_path=hcl.gfs,
                           vectorpath=hcl.hidrocl_wgs84)

gfs.run_extraction()

if 'tempdir' in locals():
    if tempdir.name == hidrocl.processing_path:
        shutil.rmtree(product_path)

print('Done')
