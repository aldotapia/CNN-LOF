import pandas as pd
from functions import preprocess_data, KGE
import pickle

import gc

import tensorflow as tf
import os

from scipy import stats
import CRPS.CRPS as pscore

# load model
from tensorflow.keras.models import load_model

import tensorflow.compat.v1 as tfc
from tensorflow.keras import backend as K

import random
from tensorflow.random import set_seed

import numpy as np
import gc

import dotenv
dotenv.load_dotenv()

exp = 'CNN' # 'CNN' -> scientific paper, 'All_data' -> operational
path = os.getenv('RF_INPUT_PATH')
datapath = 'operational'
outpath = 'outputs'
runs = os.getenv('RUNS')
runs = int(runs)

print(f'Running stats of {exp}')

def custom_masked_mae_loss_with_penalty(y_true, y_pred, penalty_weight=0.000001):
    y_true = tf.cast(y_true, tf.float32)
    y_pred = tf.cast(y_pred, tf.float32)
    bool_finite1 = tfc.is_finite(y_true[:, 0])
    bool_finite2 = tfc.is_finite(y_true[:, 1])
    mae1 = K.mean(K.abs(tfc.boolean_mask(y_pred[:, 0], bool_finite1) - tfc.boolean_mask(y_true[:, 0], bool_finite1)), axis=-1)
    mae2 = K.mean(K.abs(tfc.boolean_mask(y_pred[:, 1], bool_finite2) - tfc.boolean_mask(y_true[:, 1], bool_finite2)), axis=-1)
    mae_loss = tfc.reduce_mean(mae1 + mae2)
    first_output = tfc.boolean_mask(y_pred[:, 0], bool_finite1)
    second_output = tfc.boolean_mask(y_pred[:, 1], bool_finite2)
    condition = tfc.less(first_output, second_output)
    condition = tfc.cast(condition, tf.float32)
    penalty = penalty_weight * tfc.reduce_mean(condition)
    penalty = tfc.cast(penalty, tf.float32)
    total_loss = mae_loss + penalty
    return total_loss

static_scaler = f"{datapath}/static_scaler.sav"
observed_scaler = f"{datapath}/observed_scaler.sav"
forecasted_scaler = f"{datapath}/forecasted_scaler.sav"

# load data
val_data = pd.read_csv(path)
val_y, val_static, val_observed, val_forecasted, val_area, val_cols = preprocess_data(val_data, f'{datapath}/gl.csv', f'{datapath}/sf.csv', f'{datapath}/top.csv', norm_pp=False)
del val_data

# standardize streamflow data
for i in range(10):
    val_y.iloc[:,i] = val_y.iloc[:,i]*8.64/val_area
    val_y.iloc[:,i] = np.log(val_y.iloc[:,i] + 1)

# load scaler
scaler = pickle.load(open(static_scaler, 'rb'))
val_static = scaler.transform(val_static)

# load scaler
scaler = pickle.load(open(observed_scaler, 'rb'))
val_observed = scaler.transform(val_observed)

# load scaler
scaler = pickle.load(open(forecasted_scaler, 'rb'))
val_forecasted = scaler.transform(val_forecasted)

val_p0d = val_forecasted[:,[val.endswith('p0d') for val in val_cols[3]]]
val_p1d = val_forecasted[:,[val.endswith('p1d') for val in val_cols[3]]]
val_p2d = val_forecasted[:,[val.endswith('p2d') for val in val_cols[3]]]
val_p3d = val_forecasted[:,[val.endswith('p3d') for val in val_cols[3]]]
val_p4d = val_forecasted[:,[val.endswith('p4d') for val in val_cols[3]]]

with tf.device('/cpu:0'):
   val_observed = tf.convert_to_tensor(val_observed, np.float32)
   val_static = tf.convert_to_tensor(val_static, np.float32)
   val_forecasted = tf.convert_to_tensor(val_forecasted, np.float32)
   val_p0d = tf.convert_to_tensor(val_p0d, np.float32)
   val_p1d = tf.convert_to_tensor(val_p1d, np.float32)
   val_p2d = tf.convert_to_tensor(val_p2d, np.float32)
   val_p3d = tf.convert_to_tensor(val_p3d, np.float32)
   val_p4d = tf.convert_to_tensor(val_p4d, np.float32)
   v_y0 = tf.convert_to_tensor(val_y.iloc[:,0:2])
   v_y1 = tf.convert_to_tensor(val_y.iloc[:,2:4])
   v_y2 = tf.convert_to_tensor(val_y.iloc[:,4:6])
   v_y3 = tf.convert_to_tensor(val_y.iloc[:,6:8])
   v_y4 = tf.convert_to_tensor(val_y.iloc[:,8:10])

outl = []

print(f'Running {runs} times')

for i in range(runs):
    print(f'Running {i}')
    np.random.seed(123 + i)
    random.seed(123 + i)
    set_seed(123 + i)
    # load best model
    model = load_model(f'{datapath}/model_{exp}.h5', custom_objects={'custom_masked_mae_loss_with_penalty': custom_masked_mae_loss_with_penalty}, compile=False)
    model.compile(optimizer='adam', loss=custom_masked_mae_loss_with_penalty)
    predicted_y = model.predict([val_observed, val_static, val_p0d, val_p1d, val_p2d, val_p3d, val_p4d], batch_size=2*2*2*2*2*2*2*2*2*2*2*2*2*2)
    predicted_y = np.exp(predicted_y)-1
    predicted_y = predicted_y.clip(min=0, max = 2, out=predicted_y)
    predicted_y = predicted_y.clip(min=0, out=predicted_y)
    predicted_y = pd.DataFrame({'p0dmax':predicted_y[0,:,0].flatten(),
                                'p0dmean':predicted_y[0,:,1].flatten(),
                                'p1dmax':predicted_y[1,:,0].flatten(),
                                'p1dmean':predicted_y[1,:,1].flatten(),
                                'p2dmax':predicted_y[2,:,0].flatten(),
                                'p2dmean':predicted_y[2,:,1].flatten(),
                                'p3dmax':predicted_y[3,:,0].flatten(),
                                'p3dmean':predicted_y[3,:,1].flatten(),
                                'p4dmax':predicted_y[4,:,0].flatten(),
                                'p4dmean':predicted_y[4,:,1].flatten()})
    out = pd.concat([val_y, predicted_y], axis=1)
    out.iloc[:,0:10] = out.iloc[:,0:10]*100
    out.iloc[:,12:] = out.iloc[:,12:]*100
    outl.append(out)
    del predicted_y
    tf.keras.backend.clear_session()
    gc.collect()

df = pd.concat(outl)
#
df.to_csv(f'{outpath}/CNN_{exp}.csv', index=False)
#
df = df.groupby(['date', 'gauge_id'])

crps_p0d_mean = df.apply(lambda x: np.nan if np.isnan(x['caudal_mean_p0d'].to_numpy()[0]) else pscore(x['p0dmean'], x['caudal_mean_p0d'].to_numpy()[0]).compute()[1]).reset_index().set_index(['gauge_id', 'date'])
crps_p1d_mean = df.apply(lambda x: np.nan if np.isnan(x['caudal_mean_p1d'].to_numpy()[0]) else pscore(x['p1dmean'], x['caudal_mean_p1d'].to_numpy()[0]).compute()[1]).reset_index().set_index(['gauge_id', 'date'])
crps_p2d_mean = df.apply(lambda x: np.nan if np.isnan(x['caudal_mean_p2d'].to_numpy()[0]) else pscore(x['p2dmean'], x['caudal_mean_p2d'].to_numpy()[0]).compute()[1]).reset_index().set_index(['gauge_id', 'date'])
crps_p3d_mean = df.apply(lambda x: np.nan if np.isnan(x['caudal_mean_p3d'].to_numpy()[0]) else pscore(x['p3dmean'], x['caudal_mean_p3d'].to_numpy()[0]).compute()[1]).reset_index().set_index(['gauge_id', 'date'])
crps_p4d_mean = df.apply(lambda x: np.nan if np.isnan(x['caudal_mean_p4d'].to_numpy()[0]) else pscore(x['p4dmean'], x['caudal_mean_p4d'].to_numpy()[0]).compute()[1]).reset_index().set_index(['gauge_id', 'date'])
crps_p0d_max = df.apply(lambda x: np.nan if np.isnan(x['caudal_max_p0d'].to_numpy()[0]) else pscore(x['p0dmax'], x['caudal_max_p0d'].to_numpy()[0]).compute()[1]).reset_index().set_index(['gauge_id', 'date'])
crps_p1d_max = df.apply(lambda x: np.nan if np.isnan(x['caudal_max_p1d'].to_numpy()[0]) else pscore(x['p1dmax'], x['caudal_max_p1d'].to_numpy()[0]).compute()[1]).reset_index().set_index(['gauge_id', 'date'])
crps_p2d_max = df.apply(lambda x: np.nan if np.isnan(x['caudal_max_p2d'].to_numpy()[0]) else pscore(x['p2dmax'], x['caudal_max_p2d'].to_numpy()[0]).compute()[1]).reset_index().set_index(['gauge_id', 'date'])
crps_p3d_max = df.apply(lambda x: np.nan if np.isnan(x['caudal_max_p3d'].to_numpy()[0]) else pscore(x['p3dmax'], x['caudal_max_p3d'].to_numpy()[0]).compute()[1]).reset_index().set_index(['gauge_id', 'date'])
crps_p4d_max = df.apply(lambda x: np.nan if np.isnan(x['caudal_max_p4d'].to_numpy()[0]) else pscore(x['p4dmax'], x['caudal_max_p4d'].to_numpy()[0]).compute()[1]).reset_index().set_index(['gauge_id', 'date'])

gen_p0d_mean = df.agg({'p0dmean': [np.mean, np.std, np.median, stats.kurtosis, stats.skew]}).unstack().unstack().reset_index().pivot(index = ['gauge_id','date'], columns = 'level_1', values = 0)
gen_p0d_max = df.agg({'p0dmax': [np.mean, np.std, np.median, stats.kurtosis, stats.skew]}).unstack().unstack().reset_index().pivot(index = ['gauge_id','date'], columns = 'level_1', values = 0)
gen_p1d_mean = df.agg({'p1dmean': [np.mean, np.std, np.median, stats.kurtosis, stats.skew]}).unstack().unstack().reset_index().pivot(index = ['gauge_id','date'], columns = 'level_1', values = 0)
gen_p1d_max = df.agg({'p1dmax': [np.mean, np.std, np.median, stats.kurtosis, stats.skew]}).unstack().unstack().reset_index().pivot(index = ['gauge_id','date'], columns = 'level_1', values = 0)
gen_p2d_mean = df.agg({'p2dmean': [np.mean, np.std, np.median, stats.kurtosis, stats.skew]}).unstack().unstack().reset_index().pivot(index = ['gauge_id','date'], columns = 'level_1', values = 0)
gen_p2d_max = df.agg({'p2dmax': [np.mean, np.std, np.median, stats.kurtosis, stats.skew]}).unstack().unstack().reset_index().pivot(index = ['gauge_id','date'], columns = 'level_1', values = 0)
gen_p3d_mean = df.agg({'p3dmean': [np.mean, np.std, np.median, stats.kurtosis, stats.skew]}).unstack().unstack().reset_index().pivot(index = ['gauge_id','date'], columns = 'level_1', values = 0)
gen_p3d_max = df.agg({'p3dmax': [np.mean, np.std, np.median, stats.kurtosis, stats.skew]}).unstack().unstack().reset_index().pivot(index = ['gauge_id','date'], columns = 'level_1', values = 0)
gen_p4d_mean = df.agg({'p4dmean': [np.mean, np.std, np.median, stats.kurtosis, stats.skew]}).unstack().unstack().reset_index().pivot(index = ['gauge_id','date'], columns = 'level_1', values = 0)
gen_p4d_max = df.agg({'p4dmax': [np.mean, np.std, np.median, stats.kurtosis, stats.skew]}).unstack().unstack().reset_index().pivot(index = ['gauge_id','date'], columns = 'level_1', values = 0)

quans_p0d_mean = df['p0dmean'].quantile([0.05, 0.25, 0.75, 0.95]).unstack().unstack().unstack().reset_index().pivot(index = ['gauge_id','date'], columns = 'level_0', values = 0)
quans_p0d_max = df['p0dmax'].quantile([0.05, 0.25, 0.75, 0.95]).unstack().unstack().unstack().reset_index().pivot(index = ['gauge_id','date'], columns = 'level_0', values = 0)
quans_p1d_mean = df['p1dmean'].quantile([0.05, 0.25, 0.75, 0.95]).unstack().unstack().unstack().reset_index().pivot(index = ['gauge_id','date'], columns = 'level_0', values = 0)
quans_p1d_max = df['p1dmax'].quantile([0.05, 0.25, 0.75, 0.95]).unstack().unstack().unstack().reset_index().pivot(index = ['gauge_id','date'], columns = 'level_0', values = 0)
quans_p2d_mean = df['p2dmean'].quantile([0.05, 0.25, 0.75, 0.95]).unstack().unstack().unstack().reset_index().pivot(index = ['gauge_id','date'], columns = 'level_0', values = 0)
quans_p2d_max = df['p2dmax'].quantile([0.05, 0.25, 0.75, 0.95]).unstack().unstack().unstack().reset_index().pivot(index = ['gauge_id','date'], columns = 'level_0', values = 0)
quans_p3d_mean = df['p3dmean'].quantile([0.05, 0.25, 0.75, 0.95]).unstack().unstack().unstack().reset_index().pivot(index = ['gauge_id','date'], columns = 'level_0', values = 0)
quans_p3d_max = df['p3dmax'].quantile([0.05, 0.25, 0.75, 0.95]).unstack().unstack().unstack().reset_index().pivot(index = ['gauge_id','date'], columns = 'level_0', values = 0)
quans_p4d_mean = df['p4dmean'].quantile([0.05, 0.25, 0.75, 0.95]).unstack().unstack().unstack().reset_index().pivot(index = ['gauge_id','date'], columns = 'level_0', values = 0)
quans_p4d_max = df['p4dmax'].quantile([0.05, 0.25, 0.75, 0.95]).unstack().unstack().unstack().reset_index().pivot(index = ['gauge_id','date'], columns = 'level_0', values = 0)

df_p0d_mean = pd.concat([gen_p0d_mean, quans_p0d_mean, crps_p0d_mean, df.first()[['caudal_mean_p0d']].reset_index().set_index(['gauge_id', 'date'])], axis=1).reset_index()
df_p0d_mean['mask'] = np.nan
df_p0d_mean.columns = ['ID','date', 'kurtosis', 'mean', 'median', 'skewness',  'std_dev', 'per_5', 'per_25', 'per_75','per_95', 'crps','Qobs', 'mask']
df_p0d_mean.loc[:,'Qsim'] = df_p0d_mean['mean']
df_p0d_mean = df_p0d_mean[['ID','date', 'mean', 'std_dev', 'median', 'kurtosis', 'skewness', 'per_5', 'per_25', 'per_75','per_95','Qsim','Qobs','crps', 'mask']]

df_p0d_max = pd.concat([gen_p0d_max, quans_p0d_max, crps_p0d_max, df.first()[['caudal_max_p0d']].reset_index().set_index(['gauge_id', 'date'])], axis=1).reset_index()
df_p0d_max['mask'] = np.nan
df_p0d_max.columns = ['ID','date', 'kurtosis', 'mean', 'median', 'skewness',  'std_dev', 'per_5', 'per_25', 'per_75','per_95', 'crps','Qobs', 'mask']
df_p0d_max.loc[:,'Qsim'] = df_p0d_max['mean']
df_p0d_max = df_p0d_max[['ID','date', 'mean', 'std_dev', 'median', 'kurtosis', 'skewness', 'per_5', 'per_25', 'per_75','per_95','Qsim','Qobs','crps', 'mask']]

df_p1d_mean = pd.concat([gen_p1d_mean, quans_p1d_mean, crps_p1d_mean, df.first()[['caudal_mean_p1d']].reset_index().set_index(['gauge_id', 'date'])], axis=1).reset_index()
df_p1d_mean['mask'] = np.nan
df_p1d_mean.columns = ['ID','date', 'kurtosis', 'mean', 'median', 'skewness',  'std_dev', 'per_5', 'per_25', 'per_75','per_95', 'crps','Qobs', 'mask']
df_p1d_mean.loc[:,'Qsim'] = df_p1d_mean['mean']
df_p1d_mean = df_p1d_mean[['ID','date', 'mean', 'std_dev', 'median', 'kurtosis', 'skewness', 'per_5', 'per_25', 'per_75','per_95','Qsim','Qobs','crps', 'mask']]

df_p1d_max = pd.concat([gen_p1d_max, quans_p1d_max, crps_p1d_max, df.first()[['caudal_max_p1d']].reset_index().set_index(['gauge_id', 'date'])], axis=1).reset_index()
df_p1d_max['mask'] = np.nan
df_p1d_max.columns = ['ID','date', 'kurtosis', 'mean', 'median', 'skewness',  'std_dev', 'per_5', 'per_25', 'per_75','per_95', 'crps','Qobs', 'mask']
df_p1d_max.loc[:,'Qsim'] = df_p1d_max['mean']
df_p1d_max = df_p1d_max[['ID','date', 'mean', 'std_dev', 'median', 'kurtosis', 'skewness', 'per_5', 'per_25', 'per_75','per_95','Qsim','Qobs','crps', 'mask']]

df_p2d_mean = pd.concat([gen_p2d_mean, quans_p2d_mean, crps_p2d_mean, df.first()[['caudal_mean_p2d']].reset_index().set_index(['gauge_id', 'date'])], axis=1).reset_index()
df_p2d_mean['mask'] = np.nan
df_p2d_mean.columns = ['ID','date', 'kurtosis', 'mean', 'median', 'skewness',  'std_dev', 'per_5', 'per_25', 'per_75','per_95', 'crps','Qobs', 'mask']
df_p2d_mean.loc[:,'Qsim'] = df_p2d_mean['mean']
df_p2d_mean = df_p2d_mean[['ID','date', 'mean', 'std_dev', 'median', 'kurtosis', 'skewness', 'per_5', 'per_25', 'per_75','per_95','Qsim','Qobs','crps', 'mask']]

df_p2d_max = pd.concat([gen_p2d_max, quans_p2d_max, crps_p2d_max, df.first()[['caudal_max_p2d']].reset_index().set_index(['gauge_id', 'date'])], axis=1).reset_index()
df_p2d_max['mask'] = np.nan
df_p2d_max.columns = ['ID','date', 'kurtosis', 'mean', 'median', 'skewness',  'std_dev', 'per_5', 'per_25', 'per_75','per_95', 'crps','Qobs', 'mask']
df_p2d_max.loc[:,'Qsim'] = df_p2d_max['mean']
df_p2d_max = df_p2d_max[['ID','date', 'mean', 'std_dev', 'median', 'kurtosis', 'skewness', 'per_5', 'per_25', 'per_75','per_95','Qsim','Qobs','crps', 'mask']]

df_p3d_mean = pd.concat([gen_p3d_mean, quans_p3d_mean, crps_p3d_mean, df.first()[['caudal_mean_p3d']].reset_index().set_index(['gauge_id', 'date'])], axis=1).reset_index()
df_p3d_mean['mask'] = np.nan
df_p3d_mean.columns = ['ID','date', 'kurtosis', 'mean', 'median', 'skewness',  'std_dev', 'per_5', 'per_25', 'per_75','per_95', 'crps','Qobs', 'mask']
df_p3d_mean.loc[:,'Qsim'] = df_p3d_mean['mean']
df_p3d_mean = df_p3d_mean[['ID','date', 'mean', 'std_dev', 'median', 'kurtosis', 'skewness', 'per_5', 'per_25', 'per_75','per_95','Qsim','Qobs','crps', 'mask']]

df_p3d_max = pd.concat([gen_p3d_max, quans_p3d_max, crps_p3d_max, df.first()[['caudal_max_p3d']].reset_index().set_index(['gauge_id', 'date'])], axis=1).reset_index()
df_p3d_max['mask'] = np.nan
df_p3d_max.columns = ['ID','date', 'kurtosis', 'mean', 'median', 'skewness',  'std_dev', 'per_5', 'per_25', 'per_75','per_95', 'crps','Qobs', 'mask']
df_p3d_max.loc[:,'Qsim'] = df_p3d_max['mean']
df_p3d_max = df_p3d_max[['ID','date', 'mean', 'std_dev', 'median', 'kurtosis', 'skewness', 'per_5', 'per_25', 'per_75','per_95','Qsim','Qobs','crps', 'mask']]

df_p4d_mean = pd.concat([gen_p4d_mean, quans_p4d_mean, crps_p4d_mean, df.first()[['caudal_mean_p4d']].reset_index().set_index(['gauge_id', 'date'])], axis=1).reset_index()
df_p4d_mean['mask'] = np.nan
df_p4d_mean.columns = ['ID','date', 'kurtosis', 'mean', 'median', 'skewness',  'std_dev', 'per_5', 'per_25', 'per_75','per_95', 'crps','Qobs', 'mask']
df_p4d_mean.loc[:,'Qsim'] = df_p4d_mean['mean']
df_p4d_mean = df_p4d_mean[['ID','date', 'mean', 'std_dev', 'median', 'kurtosis', 'skewness', 'per_5', 'per_25', 'per_75','per_95','Qsim','Qobs','crps', 'mask']]

df_p4d_max = pd.concat([gen_p4d_max, quans_p4d_max, crps_p4d_max, df.first()[['caudal_max_p4d']].reset_index().set_index(['gauge_id', 'date'])], axis=1).reset_index()
df_p4d_max['mask'] = np.nan
df_p4d_max.columns = ['ID','date', 'kurtosis', 'mean', 'median', 'skewness',  'std_dev', 'per_5', 'per_25', 'per_75','per_95', 'crps','Qobs', 'mask']
df_p4d_max.loc[:,'Qsim'] = df_p4d_max['mean']
df_p4d_max = df_p4d_max[['ID','date', 'mean', 'std_dev', 'median', 'kurtosis', 'skewness', 'per_5', 'per_25', 'per_75','per_95','Qsim','Qobs','crps', 'mask']]

df_p0d_mean.to_csv(f'{outpath}/Qmean_CNN_p0d.csv', index=False)
df_p1d_mean.to_csv(f'{outpath}/Qmean_CNN_p1d.csv', index=False)
df_p2d_mean.to_csv(f'{outpath}/Qmean_CNN_p2d.csv', index=False)
df_p3d_mean.to_csv(f'{outpath}/Qmean_CNN_p3d.csv', index=False)
df_p4d_mean.to_csv(f'{outpath}/Qmean_CNN_p4d.csv', index=False)
df_p0d_max.to_csv(f'{outpath}/Qmax_CNN_p0d.csv', index=False)
df_p1d_max.to_csv(f'{outpath}/Qmax_CNN_p1d.csv', index=False)
df_p2d_max.to_csv(f'{outpath}/Qmax_CNN_p2d.csv', index=False)
df_p3d_max.to_csv(f'{outpath}/Qmax_CNN_p3d.csv', index=False)
df_p4d_max.to_csv(f'{outpath}/Qmax_CNN_p4d.csv', index=False)
















