import glob
import os
import shutil
import sys
from pathlib import Path
import numpy as np

import pandas as pd

import hidrocl
import hidrocl.paths as hcl
#from config import project_path
import dotenv

dotenv.load_dotenv()
project_path = os.getenv('PROJECT_PATH')
out_path = os.getenv('IMP_OUT_PATH')
wb_path = os.getenv('WB_PATH')
ppath = project_path

hidrocl.set_project_path(ppath)

def simple_imputation(var1, var2, path):
    """
    Impute missing values in var1 using var2.

    Parameters
    ----------

    var1 : path of variable
    var2 : path of pc varaible
    path : path where store imputed data

    """
    var = hidrocl.HidroCLVariable("test",
                                  var1,
                                  var2)
    
    var.observations = var.observations.sort_index()
    var.pcobservations = var.pcobservations.sort_index()
    var.observations = var.observations.drop_duplicates(keep='last')
    var.pcobservations = var.pcobservations.drop_duplicates(keep='last')

    for i in range(2, var.observations.shape[1]):
        var.observations.loc[var.pcobservations.iloc[:, i] <= 700, var.observations.columns[i]] = np.nan
    
    idx = pd.date_range(start=var.observations.index[0], end=pd.Timestamp.today(), freq='D')
    var.observations = var.observations.reindex(idx)

    if var.database.split('/')[-1].split('_')[0] == 'pp':
        var.observations = var.observations.fillna(0)
    else:
        var.observations = var.observations.fillna(method='ffill')
    var.observations = var.observations.fillna(0)
 
    tail = var.database.split('databases/')[-1]
    var.observations = var.observations.reset_index()
    var.observations = var.observations.rename(columns={'index':'date'})
    var.observations.to_csv(os.path.join(path,tail), index=False)

def simple_imputation_onevar(var1, path):
    """
    Impute missing values in var1 using var2.

    Parameters
    ----------

    var1 : path of variable
    path : path where store imputed data

    """
    var = pd.read_csv(var1, index_col='date')
    var.index = pd.to_datetime(var.index, format='%Y-%m-%d')
    var = var.sort_index()
    var = var.drop_duplicates(keep='last')
    
    idx = pd.date_range(start=var.index[0], end=pd.Timestamp.today(), freq='D')
    var = var.reindex(idx)

    var = var.fillna(method='ffill')
 
    tail = var1.split('databases/')[-1]
    var = var.reset_index()
    var = var.rename(columns={'index':'date'})
    var.to_csv(os.path.join(path,tail), index=False)

def dummy(template, newname, path):
    """
    Create a dummy database with the same structure as the template
    """
    df = pd.read_csv(template, index_col='date')
    df = df.iloc[0:1,:]
    df.index = pd.to_datetime(df.index, format='%Y-%m-%d')
    # extend index to today
    idx = pd.date_range(start=df.index[-1], end=pd.Timestamp.today(), freq='D')
    # new index
    idx = idx[1:]
    # create new dataframe with new index
    newidx = df.index.to_list() + idx.to_list()
    # reindex the dataframe
    df = df.reindex(newidx)
    df.iloc[0,:] = np.nan
    df = df.drop(['name_id'], axis=1)
    # rename index
    #df.index.name = 'Date'
    df.to_csv(os.path.join(path, newname + '.csv'), index=True)

simple_imputation(hcl.veg_o_modis_agr_mean_b_none_d1_p0d, hcl.veg_o_modis_agr_mean_b_pc, out_path)
simple_imputation(hcl.snw_o_modis_sca_cum_n_d8_p0d, hcl.snw_o_modis_sca_cum_n_pc, out_path)
simple_imputation(hcl.snw_o_modis_sca_cum_s_d8_p0d, hcl.snw_o_modis_sca_cum_s_pc, out_path)
simple_imputation(hcl.veg_o_modis_lai_mean, hcl.veg_o_modis_lai_pc, out_path)
simple_imputation(hcl.veg_o_modis_fpar_mean, hcl.veg_o_modis_fpar_pc, out_path)
simple_imputation(hcl.veg_o_modis_ndvi_mean_b_d16_p0d, hcl.veg_o_modis_ndvi_mean_pc, out_path)
simple_imputation(hcl.veg_o_modis_evi_mean_b_d16_p0d, hcl.veg_o_modis_evi_mean_pc, out_path)
simple_imputation(hcl.veg_o_int_nbr_mean_b_d16_p0d, hcl.veg_o_int_nbr_mean_pc, out_path)
simple_imputation(hcl.veg_o_modis_agr_mean_b_none_d1_p0d, hcl.veg_o_modis_agr_mean_b_pc, out_path)
simple_imputation(hcl.pp_o_era5_pp_mean, hcl.pp_o_era5_pp_pc, out_path)
simple_imputation(hcl.tmp_o_era5_tmp_mean, hcl.tmp_o_era5_tmp_pc, out_path)
simple_imputation(hcl.tmp_o_era5_tmin_mean, hcl.tmp_o_era5_tmin_pc, out_path)
simple_imputation(hcl.tmp_o_era5_tmax_mean, hcl.tmp_o_era5_tmax_pc, out_path)
simple_imputation(hcl.tmp_o_era5_dew_mean, hcl.tmp_o_era5_dew_pc, out_path)
simple_imputation(hcl.atm_o_era5_pres_mean, hcl.atm_o_era5_pres_pc, out_path)
simple_imputation(hcl.atm_o_era5_uw_mean, hcl.atm_o_era5_uw_pc, out_path)
simple_imputation(hcl.atm_o_era5_vw_mean, hcl.atm_o_era5_vw_pc, out_path)
simple_imputation(hcl.pp_o_era5_plen_mean_b_d1_p0d, hcl.pp_o_era5_plen_mean_b_pc, out_path)
simple_imputation(hcl.pp_o_era5_maxpp_mean, hcl.pp_o_era5_maxpp_pc, out_path)
simple_imputation(hcl.awc_o_era5_rh_mean_b_none_d1_p0d, hcl.awc_o_era5_rh_mean_b_pc, out_path)
simple_imputation(hcl.et_o_era5_eto_mean, hcl.et_o_era5_eto_pc, out_path)
simple_imputation(hcl.et_o_era5_eta_mean, hcl.et_o_era5_eta_pc, out_path)
simple_imputation(hcl.snw_o_era5_sca_mean, hcl.snw_o_era5_sca_pc, out_path)
simple_imputation(hcl.snw_o_era5_sna_mean, hcl.snw_o_era5_sna_pc, out_path)
simple_imputation(hcl.snw_o_era5_snr_mean, hcl.snw_o_era5_snr_pc, out_path)
simple_imputation(hcl.snw_o_era5_snd_mean, hcl.snw_o_era5_snd_pc, out_path)
simple_imputation(hcl.swc_o_era5_sm_mean, hcl.swc_o_era5_sm_pc, out_path)
simple_imputation(hcl.atm_o_era5_z_mean_b_none_d1_p0d, hcl.atm_o_era5_z_mean_b_pc, out_path)
simple_imputation(hcl.atm_f_gfs_gh_mean_b_none_d1_p0d, hcl.atm_f_gfs_gh_mean_pc_p0d, out_path)
simple_imputation(hcl.atm_f_gfs_gh_mean_b_none_d1_p1d, hcl.atm_f_gfs_gh_mean_pc_p1d, out_path)
simple_imputation(hcl.atm_f_gfs_gh_mean_b_none_d1_p2d, hcl.atm_f_gfs_gh_mean_pc_p2d, out_path)
simple_imputation(hcl.atm_f_gfs_gh_mean_b_none_d1_p3d, hcl.atm_f_gfs_gh_mean_pc_p3d, out_path)
simple_imputation(hcl.atm_f_gfs_gh_mean_b_none_d1_p4d, hcl.atm_f_gfs_gh_mean_pc_p4d, out_path)
simple_imputation(hcl.pp_f_gfs_plen_mean_b_none_d1_p0d, hcl.pp_f_gfs_plen_mean_pc_p0d, out_path)
simple_imputation(hcl.pp_f_gfs_plen_mean_b_none_d1_p1d, hcl.pp_f_gfs_plen_mean_pc_p1d, out_path)
simple_imputation(hcl.pp_f_gfs_plen_mean_b_none_d1_p2d, hcl.pp_f_gfs_plen_mean_pc_p2d, out_path)
simple_imputation(hcl.pp_f_gfs_plen_mean_b_none_d1_p3d, hcl.pp_f_gfs_plen_mean_pc_p3d, out_path)
simple_imputation(hcl.pp_f_gfs_plen_mean_b_none_d1_p4d, hcl.pp_f_gfs_plen_mean_pc_p4d, out_path)
simple_imputation(hcl.pp_f_gfs_pp_max_b_none_d1_p0d, hcl.pp_f_gfs_pp_max_pc_p0d, out_path)
simple_imputation(hcl.pp_f_gfs_pp_max_b_none_d1_p1d, hcl.pp_f_gfs_pp_max_pc_p1d, out_path)
simple_imputation(hcl.pp_f_gfs_pp_max_b_none_d1_p2d, hcl.pp_f_gfs_pp_max_pc_p2d, out_path)
simple_imputation(hcl.pp_f_gfs_pp_max_b_none_d1_p3d, hcl.pp_f_gfs_pp_max_pc_p3d, out_path)
simple_imputation(hcl.pp_f_gfs_pp_max_b_none_d1_p4d, hcl.pp_f_gfs_pp_max_pc_p4d, out_path)
simple_imputation(hcl.pp_f_gfs_pp_mean_b_none_d1_p0d, hcl.pp_f_gfs_pp_mean_pc_p0d, out_path)
simple_imputation(hcl.pp_f_gfs_pp_mean_b_none_d1_p1d, hcl.pp_f_gfs_pp_mean_pc_p1d, out_path)
simple_imputation(hcl.pp_f_gfs_pp_mean_b_none_d1_p2d, hcl.pp_f_gfs_pp_mean_pc_p2d, out_path)
simple_imputation(hcl.pp_f_gfs_pp_mean_b_none_d1_p3d, hcl.pp_f_gfs_pp_mean_pc_p3d, out_path)
simple_imputation(hcl.pp_f_gfs_pp_mean_b_none_d1_p4d, hcl.pp_f_gfs_pp_mean_pc_p4d, out_path)
simple_imputation(hcl.awc_f_gfs_rh_mean_b_none_d1_p0d, hcl.awc_f_gfs_rh_mean_pc_p0d, out_path)
simple_imputation(hcl.awc_f_gfs_rh_mean_b_none_d1_p1d, hcl.awc_f_gfs_rh_mean_pc_p1d, out_path)
simple_imputation(hcl.awc_f_gfs_rh_mean_b_none_d1_p2d, hcl.awc_f_gfs_rh_mean_pc_p2d, out_path)
simple_imputation(hcl.awc_f_gfs_rh_mean_b_none_d1_p3d, hcl.awc_f_gfs_rh_mean_pc_p3d, out_path)
simple_imputation(hcl.awc_f_gfs_rh_mean_b_none_d1_p4d, hcl.awc_f_gfs_rh_mean_pc_p4d, out_path)
simple_imputation(hcl.tmp_f_gfs_tmp_max_b_none_d1_p0d, hcl.tmp_f_gfs_tmp_max_pc_p0d, out_path)
simple_imputation(hcl.tmp_f_gfs_tmp_max_b_none_d1_p1d, hcl.tmp_f_gfs_tmp_max_pc_p1d, out_path)
simple_imputation(hcl.tmp_f_gfs_tmp_max_b_none_d1_p2d, hcl.tmp_f_gfs_tmp_max_pc_p2d, out_path)
simple_imputation(hcl.tmp_f_gfs_tmp_max_b_none_d1_p3d, hcl.tmp_f_gfs_tmp_max_pc_p3d, out_path)
simple_imputation(hcl.tmp_f_gfs_tmp_max_b_none_d1_p4d, hcl.tmp_f_gfs_tmp_max_pc_p4d, out_path)
simple_imputation(hcl.tmp_f_gfs_tmp_mean_b_none_d1_p0d, hcl.tmp_f_gfs_tmp_mean_pc_p0d, out_path)
simple_imputation(hcl.tmp_f_gfs_tmp_mean_b_none_d1_p1d, hcl.tmp_f_gfs_tmp_mean_pc_p1d, out_path)
simple_imputation(hcl.tmp_f_gfs_tmp_mean_b_none_d1_p2d, hcl.tmp_f_gfs_tmp_mean_pc_p2d, out_path)
simple_imputation(hcl.tmp_f_gfs_tmp_mean_b_none_d1_p3d, hcl.tmp_f_gfs_tmp_mean_pc_p3d, out_path)
simple_imputation(hcl.tmp_f_gfs_tmp_mean_b_none_d1_p4d, hcl.tmp_f_gfs_tmp_mean_pc_p4d, out_path)
simple_imputation(hcl.tmp_f_gfs_tmp_min_b_none_d1_p0d, hcl.tmp_f_gfs_tmp_min_pc_p0d, out_path)
simple_imputation(hcl.tmp_f_gfs_tmp_min_b_none_d1_p1d, hcl.tmp_f_gfs_tmp_min_pc_p1d, out_path)
simple_imputation(hcl.tmp_f_gfs_tmp_min_b_none_d1_p2d, hcl.tmp_f_gfs_tmp_min_pc_p2d, out_path)
simple_imputation(hcl.tmp_f_gfs_tmp_min_b_none_d1_p3d, hcl.tmp_f_gfs_tmp_min_pc_p3d, out_path)
simple_imputation(hcl.tmp_f_gfs_tmp_min_b_none_d1_p4d, hcl.tmp_f_gfs_tmp_min_pc_p4d, out_path)
simple_imputation(hcl.atm_f_gfs_uw_mean_b_none_d1_p0d, hcl.atm_f_gfs_uw_mean_pc_p0d, out_path)
simple_imputation(hcl.atm_f_gfs_uw_mean_b_none_d1_p1d, hcl.atm_f_gfs_uw_mean_pc_p1d, out_path)
simple_imputation(hcl.atm_f_gfs_uw_mean_b_none_d1_p2d, hcl.atm_f_gfs_uw_mean_pc_p2d, out_path)
simple_imputation(hcl.atm_f_gfs_uw_mean_b_none_d1_p3d, hcl.atm_f_gfs_uw_mean_pc_p3d, out_path)
simple_imputation(hcl.atm_f_gfs_uw_mean_b_none_d1_p4d, hcl.atm_f_gfs_uw_mean_pc_p4d, out_path)
simple_imputation(hcl.atm_f_gfs_vw_mean_b_none_d1_p0d, hcl.atm_f_gfs_vw_mean_pc_p0d, out_path)
simple_imputation(hcl.atm_f_gfs_vw_mean_b_none_d1_p1d, hcl.atm_f_gfs_vw_mean_pc_p1d, out_path)
simple_imputation(hcl.atm_f_gfs_vw_mean_b_none_d1_p2d, hcl.atm_f_gfs_vw_mean_pc_p2d, out_path)
simple_imputation(hcl.atm_f_gfs_vw_mean_b_none_d1_p3d, hcl.atm_f_gfs_vw_mean_pc_p3d, out_path)
simple_imputation(hcl.atm_f_gfs_vw_mean_b_none_d1_p4d, hcl.atm_f_gfs_vw_mean_pc_p4d, out_path)
simple_imputation(hcl.pp_o_imerg_pp_mean_b_d_p0d, hcl.pp_o_imerg_pp_mean_b_pc, out_path)
simple_imputation(hcl.pp_o_pdir_pp_mean_b_none_d1_p0d, hcl.pp_o_pdir_pp_mean_b_pc, out_path)
simple_imputation(hcl.lulc_o_modis_brn_frac_b_none_d1_p0d, hcl.lulc_o_modis_brn_frac_pc, out_path)
simple_imputation(hcl.lulc_o_modis_crp_frac_b_none_d1_p0d, hcl.lulc_o_modis_crp_frac_pc, out_path)
simple_imputation(hcl.lulc_o_modis_csh_frac_b_none_d1_p0d, hcl.lulc_o_modis_csh_frac_pc, out_path)
simple_imputation(hcl.lulc_o_modis_cvm_frac_b_none_d1_p0d, hcl.lulc_o_modis_cvm_frac_pc, out_path)
simple_imputation(hcl.lulc_o_modis_dbf_frac_b_none_d1_p0d, hcl.lulc_o_modis_dbf_frac_pc, out_path)
simple_imputation(hcl.lulc_o_modis_dnf_frac_b_none_d1_p0d, hcl.lulc_o_modis_dnf_frac_pc, out_path)
simple_imputation(hcl.lulc_o_modis_ebf_frac_b_none_d1_p0d, hcl.lulc_o_modis_ebf_frac_pc, out_path)
simple_imputation(hcl.lulc_o_modis_enf_frac_b_none_d1_p0d, hcl.lulc_o_modis_enf_frac_pc, out_path)
simple_imputation(hcl.lulc_o_modis_grs_frac_b_none_d1_p0d, hcl.lulc_o_modis_grs_frac_pc, out_path)
simple_imputation(hcl.lulc_o_modis_mxf_frac_b_none_d1_p0d, hcl.lulc_o_modis_mxf_frac_pc, out_path)
simple_imputation(hcl.lulc_o_modis_osh_frac_b_none_d1_p0d, hcl.lulc_o_modis_osh_frac_pc, out_path)
simple_imputation(hcl.lulc_o_modis_pwt_frac_b_none_d1_p0d, hcl.lulc_o_modis_pwt_frac_pc, out_path)
simple_imputation(hcl.lulc_o_modis_snw_frac_b_none_d1_p0d, hcl.lulc_o_modis_snw_frac_pc, out_path)
simple_imputation(hcl.lulc_o_modis_svn_frac_b_none_d1_p0d, hcl.lulc_o_modis_svn_frac_pc, out_path)
simple_imputation(hcl.lulc_o_modis_urb_frac_b_none_d1_p0d, hcl.lulc_o_modis_urb_frac_pc, out_path)
simple_imputation(hcl.lulc_o_modis_wat_frac_b_none_d1_p0d, hcl.lulc_o_modis_wat_frac_pc, out_path)
simple_imputation(hcl.lulc_o_modis_wsv_frac_b_none_d1_p0d, hcl.lulc_o_modis_wsv_frac_pc, out_path)
simple_imputation(hcl.lulc_o_modis_brn_sum_b_none_d1_p0d, hcl.lulc_o_modis_brn_sum_pc, out_path)
simple_imputation(hcl.lulc_o_modis_crp_sum_b_none_d1_p0d, hcl.lulc_o_modis_crp_sum_pc, out_path)
simple_imputation(hcl.lulc_o_modis_csh_sum_b_none_d1_p0d, hcl.lulc_o_modis_csh_sum_pc, out_path)
simple_imputation(hcl.lulc_o_modis_cvm_sum_b_none_d1_p0d, hcl.lulc_o_modis_cvm_sum_pc, out_path)
simple_imputation(hcl.lulc_o_modis_dbf_sum_b_none_d1_p0d, hcl.lulc_o_modis_dbf_sum_pc, out_path)
simple_imputation(hcl.lulc_o_modis_dnf_sum_b_none_d1_p0d, hcl.lulc_o_modis_dnf_sum_pc, out_path)
simple_imputation(hcl.lulc_o_modis_ebf_sum_b_none_d1_p0d, hcl.lulc_o_modis_ebf_sum_pc, out_path)
simple_imputation(hcl.lulc_o_modis_enf_sum_b_none_d1_p0d, hcl.lulc_o_modis_enf_sum_pc, out_path)
simple_imputation(hcl.lulc_o_modis_grs_sum_b_none_d1_p0d, hcl.lulc_o_modis_grs_sum_pc, out_path)
simple_imputation(hcl.lulc_o_modis_mxf_sum_b_none_d1_p0d, hcl.lulc_o_modis_mxf_sum_pc, out_path)
simple_imputation(hcl.lulc_o_modis_osh_sum_b_none_d1_p0d, hcl.lulc_o_modis_osh_sum_pc, out_path)
simple_imputation(hcl.lulc_o_modis_pwt_sum_b_none_d1_p0d, hcl.lulc_o_modis_pwt_sum_pc, out_path)
simple_imputation(hcl.lulc_o_modis_snw_sum_b_none_d1_p0d, hcl.lulc_o_modis_snw_sum_pc, out_path)
simple_imputation(hcl.lulc_o_modis_svn_sum_b_none_d1_p0d, hcl.lulc_o_modis_svn_sum_pc, out_path)
simple_imputation(hcl.lulc_o_modis_urb_sum_b_none_d1_p0d, hcl.lulc_o_modis_urb_sum_pc, out_path)
simple_imputation(hcl.lulc_o_modis_wat_sum_b_none_d1_p0d, hcl.lulc_o_modis_wat_sum_pc, out_path)
simple_imputation(hcl.lulc_o_modis_wsv_sum_b_none_d1_p0d, hcl.lulc_o_modis_wsv_sum_pc, out_path)
simple_imputation_onevar(wb_path, out_path)

# create dummy files for:
vars = ['caudal_max_p0d', 'caudal_max_p1d', 'caudal_max_p2d','caudal_max_p3d', 
        'caudal_max_p4d','caudal_mean_p0d', 'caudal_mean_p1d', 'caudal_mean_p2d', 
        'caudal_mean_p3d','caudal_mean_p4d','caudal_mask2_p0d', 'caudal_mask2_p1d',
        'caudal_mask2_p2d','caudal_mask2_p3d', 'caudal_mask2_p4d']

for var in vars:
    dummy(hcl.tmp_o_era5_tmp_mean, var, os.path.join(out_path,'caudal'))
