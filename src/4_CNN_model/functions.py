import pandas as pd
import numpy as np
from scipy.stats import boxcox, yeojohnson

def preprocess_data(data,
                    gl,
                    sf,
                    top,
                    norm_pp = True,):
    """
    Preprocess static data.

    Arguments:
    data -- pandas DataFrame containing all data
    gl -- file of glaciar columns weights
    sf -- file of soil features columns weights
    top -- file of topographic columns weights
    """

    # first, backup area
    area = data['top_s_cam_area_tot_b_none_c_c']

    # load weights
    gl = pd.read_csv(gl)
    sf = pd.read_csv(sf)
    top = pd.read_csv(top)

    # apply min max based on gl data
    for i in range(len(gl)):
        data[gl['name'][i]] = (data[gl['name'][i]] - gl['min'][i]) / (gl['max'][i] - gl['min'][i])

    data['gl1'] = (data[gl['name']] * gl['c1'].to_numpy()).sum(axis=1)
    data['gl2'] = (data[gl['name']] * gl['c2'].to_numpy()).sum(axis=1)
    data['gl3'] = (data[gl['name']] * gl['c3'].to_numpy()).sum(axis=1)
    data['gl4'] = (data[gl['name']] * gl['c4'].to_numpy()).sum(axis=1)

    data.loc[:,'gl1'] = boxcox(data['gl1'] + 1, lmbda=-0.878777284)
    data.loc[:,'gl2'] = yeojohnson(data['gl2'] + 1, lmbda=-0.341510353)
    data.loc[:,'gl3'] = boxcox(data['gl3'] + 1, lmbda=-3.020179932)
    data.loc[:,'gl4'] = boxcox(data['gl4'] + 1, lmbda=0.876856395)

    data = data.drop(gl['name'], axis=1)

    # apply min max based on sf data
    for i in range(len(sf)):
        data[sf['name'][i]] = (data[sf['name'][i]] - sf['min'][i]) / (sf['max'][i] - sf['min'][i])

    data['sf1'] = (data[sf['name']] * sf['c1'].to_numpy()).sum(axis=1)
    data['sf2'] = (data[sf['name']] * sf['c2'].to_numpy()).sum(axis=1)
    data['sf3'] = (data[sf['name']] * sf['c3'].to_numpy()).sum(axis=1)
    data['sf4'] = (data[sf['name']] * sf['c4'].to_numpy()).sum(axis=1)
    data['sf5'] = (data[sf['name']] * sf['c5'].to_numpy()).sum(axis=1)

    data['sf1'] = yeojohnson(data['sf1'] + 1, lmbda=0.846214574)
    data['sf2'] = yeojohnson(data['sf2'] + 1, lmbda=0.472563154)
    data['sf3'] = yeojohnson(data['sf3'] + 1, lmbda=-0.355084015)
    data['sf4'] = yeojohnson(data['sf4'] + 1, lmbda=-0.076637491)
    data['sf5'] = yeojohnson(data['sf5'] + 1, lmbda=-0.528213852)

    data = data.drop(sf['name'], axis=1)

    # apply min max based on top data
    data = data.drop(['top_s_cam_lat_mean_b_none_c_c','top_s_cam_lat_none_p_none_c_c',
                      'top_s_cam_lon_mean_b_none_c_c', 'top_s_cam_lon_none_p_none_c_c',
                      'top_s_dga_lat_none_p_none_c_c','top_s_dga_lon_none_p_none_c_c'], axis=1)

    for i in range(len(top)):
        data[top['name'][i]] = (data[top['name'][i]] - top['min'][i]) / (top['max'][i] - top['min'][i])

    data['top1'] = (data[top['name']] * top['c1'].to_numpy()).sum(axis=1)
    data['top2'] = (data[top['name']] * top['c2'].to_numpy()).sum(axis=1)
    data['top3'] = (data[top['name']] * top['c3'].to_numpy()).sum(axis=1)
    data['top4'] = (data[top['name']] * top['c4'].to_numpy()).sum(axis=1)

    data['top1'] = yeojohnson(data['top1'] + 1, lmbda=-0.388477567)
    data['top2'] = yeojohnson(data['top2'] + 1, lmbda=0.020790466)
    data['top3'] = yeojohnson(data['top3'] + 1, lmbda=-0.061433869)
    data['top4'] = yeojohnson(data['top4'] + 1, lmbda=-1.405814619)

    data = data.drop(top['name'], axis=1)

    data['idx_s_cam_arcr2_tot_b_none_c_c'] = boxcox(data['idx_s_cam_arcr2_tot_b_none_c_c'] + 1, lmbda=-0.371882)
    data['idx_s_cam_arcr2_tot_b_none_c_c'] = (data['idx_s_cam_arcr2_tot_b_none_c_c'] - 2.193024)/0.155585
    data['hi_s_cam_gwr_tot_b_none_c_c'] = boxcox(data['hi_s_cam_gwr_tot_b_none_c_c'] + 1, lmbda=-0.08852)
    data['hi_s_cam_gwr_tot_b_none_c_c'] = (data['hi_s_cam_gwr_tot_b_none_c_c'] - 2.664886)/2.365528
    data['hi_s_cam_sr_tot_b_none_c_c'] = boxcox(data['hi_s_cam_sr_tot_b_none_c_c'] + 1, lmbda=0.041372)
    data['hi_s_cam_sr_tot_b_none_c_c'] = (data['hi_s_cam_sr_tot_b_none_c_c'] - 6.800812)/3.965996

    cols = data.columns
    observed_variables = [val for val in cols if '_o_' in val or ('_f_' in val and 'd1_m' in val)]
    observed_variables = [val for val in observed_variables if '_y' not in val]
    forecasted_variables = [val for val in cols if '_f_' in val and 'd1_m' not in val]
    targets = [val for val in cols if 'caudal_mean' in val or 'caudal_max' in val]
    targets = sorted(targets)
    t_p0d = [val for val in targets if 'p0d' in val]
    t_p1d = [val for val in targets if 'p1d' in val]
    t_p2d = [val for val in targets if 'p2d' in val]
    t_p3d = [val for val in targets if 'p3d' in val]
    t_p4d = [val for val in targets if 'p4d' in val]
    id = 'gauge_id'
    date = 'date'

    static = data[["gl1","gl2","gl3","gl4","idx_s_cam_arcr2_tot_b_none_c_c","hi_s_cam_gwr_tot_b_none_c_c","hi_s_cam_sr_tot_b_none_c_c","sf1","sf2","sf3","sf4","sf5","top1","top2","top3","top4"]]
    observed = data[observed_variables]
    forecasted = data[forecasted_variables]
    y = data[t_p0d + t_p1d + t_p2d + t_p3d + t_p4d + [date, id]]

    if norm_pp:
        pp_ob_cols = [val for val in observed.columns if 'pp_' in val]
        for val in pp_ob_cols:
            observed.loc[:,val] = np.log(observed[val] + 1)
        pp_fc_cols = [val for val in forecasted.columns if 'pp_' in val]
        for val in pp_fc_cols:
            forecasted.loc[:,val] = np.log(forecasted[val] + 1)

    # sort columns by name
    static = static.reindex(sorted(static.columns), axis=1)
    observed = observed.reindex(sorted(observed.columns), axis=1)
    forecasted = forecasted.reindex(sorted(forecasted.columns), axis=1)
    # y = y.reindex(sorted(y.columns), axis=1)

    cols = [y.columns,static.columns, observed.columns, forecasted.columns]

    return(y, static, observed, forecasted, area, cols)

def preprocess_data2(dat,
                    gl,
                    sf,
                    top,
                    norm_pp = True,):
    """
    Preprocess static data.

    Arguments:
    data -- pandas DataFrame containing all data
    gl -- file of glaciar columns weights
    sf -- file of soil features columns weights
    top -- file of topographic columns weights
    """

    data = dat.copy()

    # first, backup area
    area = data['top_s_cam_area_tot_b_none_c_c']

    # load weights
    gl = pd.read_csv(gl)
    sf = pd.read_csv(sf)
    top = pd.read_csv(top)

    # apply min max based on gl data
    for i in range(len(gl)):
        data[gl['name'][i]] = (data[gl['name'][i]] - gl['min'][i]) / (gl['max'][i] - gl['min'][i])

    data['gl1'] = (data[gl['name']] * gl['c1'].to_numpy()).sum(axis=1)
    data['gl2'] = (data[gl['name']] * gl['c2'].to_numpy()).sum(axis=1)
    data['gl3'] = (data[gl['name']] * gl['c3'].to_numpy()).sum(axis=1)
    data['gl4'] = (data[gl['name']] * gl['c4'].to_numpy()).sum(axis=1)

    data.loc[:,'gl1'] = boxcox(data['gl1'] + 1, lmbda=-0.878777284)
    data.loc[:,'gl2'] = yeojohnson(data['gl2'] + 1, lmbda=-0.341510353)
    data.loc[:,'gl3'] = boxcox(data['gl3'] + 1, lmbda=-3.020179932)
    data.loc[:,'gl4'] = boxcox(data['gl4'] + 1, lmbda=0.876856395)

    data = data.drop(gl['name'], axis=1)

    # apply min max based on sf data
    for i in range(len(sf)):
        data[sf['name'][i]] = (data[sf['name'][i]] - sf['min'][i]) / (sf['max'][i] - sf['min'][i])

    data['sf1'] = (data[sf['name']] * sf['c1'].to_numpy()).sum(axis=1)
    data['sf2'] = (data[sf['name']] * sf['c2'].to_numpy()).sum(axis=1)
    data['sf3'] = (data[sf['name']] * sf['c3'].to_numpy()).sum(axis=1)
    data['sf4'] = (data[sf['name']] * sf['c4'].to_numpy()).sum(axis=1)
    data['sf5'] = (data[sf['name']] * sf['c5'].to_numpy()).sum(axis=1)

    data['sf1'] = yeojohnson(data['sf1'] + 1, lmbda=0.846214574)
    data['sf2'] = yeojohnson(data['sf2'] + 1, lmbda=0.472563154)
    data['sf3'] = yeojohnson(data['sf3'] + 1, lmbda=-0.355084015)
    data['sf4'] = yeojohnson(data['sf4'] + 1, lmbda=-0.076637491)
    data['sf5'] = yeojohnson(data['sf5'] + 1, lmbda=-0.528213852)

    data = data.drop(sf['name'], axis=1)

    # apply min max based on top data
    data = data.drop(['top_s_cam_lat_mean_b_none_c_c','top_s_cam_lat_none_p_none_c_c',
                      'top_s_cam_lon_mean_b_none_c_c', 'top_s_cam_lon_none_p_none_c_c',
                      'top_s_dga_lat_none_p_none_c_c','top_s_dga_lon_none_p_none_c_c'], axis=1)

    for i in range(len(top)):
        data[top['name'][i]] = (data[top['name'][i]] - top['min'][i]) / (top['max'][i] - top['min'][i])

    data['top1'] = (data[top['name']] * top['c1'].to_numpy()).sum(axis=1)
    data['top2'] = (data[top['name']] * top['c2'].to_numpy()).sum(axis=1)
    data['top3'] = (data[top['name']] * top['c3'].to_numpy()).sum(axis=1)
    data['top4'] = (data[top['name']] * top['c4'].to_numpy()).sum(axis=1)

    data['top1'] = yeojohnson(data['top1'] + 1, lmbda=-0.388477567)
    data['top2'] = yeojohnson(data['top2'] + 1, lmbda=0.020790466)
    data['top3'] = yeojohnson(data['top3'] + 1, lmbda=-0.061433869)
    data['top4'] = yeojohnson(data['top4'] + 1, lmbda=-1.405814619)

    data = data.drop(top['name'], axis=1)

    data['idx_s_cam_arcr2_tot_b_none_c_c'] = boxcox(data['idx_s_cam_arcr2_tot_b_none_c_c'] + 1, lmbda=-0.371882)
    data['idx_s_cam_arcr2_tot_b_none_c_c'] = (data['idx_s_cam_arcr2_tot_b_none_c_c'] - 2.193024)/0.155585
    data['hi_s_cam_gwr_tot_b_none_c_c'] = boxcox(data['hi_s_cam_gwr_tot_b_none_c_c'] + 1, lmbda=-0.08852)
    data['hi_s_cam_gwr_tot_b_none_c_c'] = (data['hi_s_cam_gwr_tot_b_none_c_c'] - 2.664886)/2.365528
    data['hi_s_cam_sr_tot_b_none_c_c'] = boxcox(data['hi_s_cam_sr_tot_b_none_c_c'] + 1, lmbda=0.041372)
    data['hi_s_cam_sr_tot_b_none_c_c'] = (data['hi_s_cam_sr_tot_b_none_c_c'] - 6.800812)/3.965996

    cols = data.columns
    observed_variables = [val for val in cols if '_o_' in val or ('_f_' in val and 'd1_m' in val)]
    observed_variables = [val for val in observed_variables if '_y' not in val]
    forecasted_variables = [val for val in cols if '_f_' in val and 'd1_m' not in val]
    targets = [val for val in cols if 'caudal_mean' in val or 'caudal_max' in val]
    targets = sorted(targets)
    t_p0d = [val for val in targets if 'p0d' in val]
    t_p1d = [val for val in targets if 'p1d' in val]
    t_p2d = [val for val in targets if 'p2d' in val]
    t_p3d = [val for val in targets if 'p3d' in val]
    t_p4d = [val for val in targets if 'p4d' in val]
    id = 'gauge_id'
    date = 'date'

    static = data[["gl1","gl2","gl3","gl4","idx_s_cam_arcr2_tot_b_none_c_c","hi_s_cam_gwr_tot_b_none_c_c","hi_s_cam_sr_tot_b_none_c_c","sf1","sf2","sf3","sf4","sf5","top1","top2","top3","top4"]]
    observed = data[observed_variables]
    forecasted = data[forecasted_variables]
    y = data[t_p0d + t_p1d + t_p2d + t_p3d + t_p4d + [date, id]]

    if norm_pp:
        pp_ob_cols = [val for val in observed.columns if 'pp_' in val]
        for val in pp_ob_cols:
            observed.loc[:,val] = np.log(observed[val] + 1)
        pp_fc_cols = [val for val in forecasted.columns if 'pp_' in val]
        for val in pp_fc_cols:
            forecasted.loc[:,val] = np.log(forecasted[val] + 1)

    # sort columns by name
    static = static.reindex(sorted(static.columns), axis=1)
    observed = observed.reindex(sorted(observed.columns), axis=1)
    forecasted = forecasted.reindex(sorted(forecasted.columns), axis=1)
    # y = y.reindex(sorted(y.columns), axis=1)

    cols = [y.columns,static.columns, observed.columns, forecasted.columns]

    return(y, static, observed, forecasted, area, cols)


def KGE(x,y):
    '''Kling-Gupta Efficiency (KGE)'''
    # x: observed
    # y: simulated
    # x and y must have the same length

    x = np.array(x)
    y = np.array(y)

    # keep only values that are not NaN
    y = y[~np.isnan(x)]
    x = x[~np.isnan(x)]
    

    # Mean
    x_mean = np.mean(x)
    y_mean = np.mean(y)

    # Standard deviation
    x_std = np.std(x)
    y_std = np.std(y)

    # Correlation
    xy = np.sum((x-x_mean)*(y-y_mean))
    corr = xy/((len(x)-1)*x_std*y_std)

    # Alpha
    alpha = y_std/x_std

    # Beta
    beta = y_mean/x_mean

    # KGE
    kge = 1 - np.sqrt((corr-1)**2 + (alpha-1)**2 + (beta-1)**2)

    return kge, corr, alpha, beta
