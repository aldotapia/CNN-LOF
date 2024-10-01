import os
import dotenv
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')
import pickle
import json



# Cargar el archivo .env específico
load_dotenv()

# Variables de entorno
HIDROCL_ROOT_PATH = os.getenv('HIDROCL_ROOT_PATH')
DICCIONARIO_PATH = os.getenv('DICCIONARIO_PATH')
BASE_DATOS_PATH = os.getenv('BASE_DATOS_PATH')
OUTPUT_PATH = os.getenv('OUTPUT_PATH')
CATCHMENTS = os.getenv('CATCHMENTS')
dir_path = OUTPUT_PATH # os.path.join(HIDROCL_ROOT_PATH, OUTPUT_PATH)  #dir_path = '~/LSTM_model/IMPUTs_MODELs'

#########################################################
###################### DICCIONARIO ######################
#########################################################

class HidroCL_Dictionary:
    def __init__(self):
        self.data = {} # Crea una variable de instancia vacía llamada "data" para almacenar el diccionario

    def readDiccionario(self, filename): # Lee el archivo CSV especificado utilizando Pandas
        df = pd.read_csv(filename, sep=';', skiprows=2, header=None, encoding='latin-1')
        keys = ["Group", "Type", "Source", "Variable", "SpatialAgg", "Coverage", "TemporalAgg", "Period", "ValidTime"] # Define una lista de nombres de columnas
        for i in range(0, len(keys)): # Para cada columna, almacena las claves y valores en el diccionario "data"
            self.data[keys[i]] = {'key': df[2*i + 1], 'value': df[2*i]}
            #self.data[keys[i]]['key'] = self.data[keys[i]]['key'].fillna(method='ffill')  # rellenar los valores NaN con el valor de la fila anterior
            #self.data[keys[i]]['value'] = self.data[keys[i]]['value'].fillna(method='ffill')
            self.data[keys[i]]['key'] = self.data[keys[i]]['key'].ffill()
            self.data[keys[i]]['value'] = self.data[keys[i]]['value'].ffill()
            self.data[keys[i]] = dict(zip(self.data[keys[i]]["key"], self.data[keys[i]]["value"])) # combina las claves y valores en un solo diccionario

    def __getitem__(self, key):
        return self.data[key]


##############################################################
###################### CONVERSIÓN A DF  ######################
##############################################################

class HidroCL_Variable:
    def __init__(self, varcod, pathdicc = DICCIONARIO_PATH, varpath = BASE_DATOS_PATH):
        self.start_date =  '2021-01-01'  #'2015-01-01'
        # Utilizar las variables de entorno para las rutas
        self.varpath = varpath    #    os.path.join(HIDROCL_ROOT_PATH, varpath)
        self.varcod = varcod
        self.pathdicc =   pathdicc # os.path.join(HIDROCL_ROOT_PATH, pathdicc)
        self.data = None
        self.data_list = []
        self.fields = {}
        fields_list = ['Group', 'Type', 'Source', 'Variable', 'SpatialAgg', 'Coverage', 'TemporalAgg', 'Period', 'ValidTime']
        for i, val in enumerate(varcod.split('_')):
            self.fields[fields_list[i]] = val
        self.path = self.__getMetadata__('path')
        self.Latency = self.__getMetadata__('Latency')
        self.Scale_Factor = self.__getMetadata__('Scale_Factor')
        self.Unit = self.__getMetadata__('Unit')
        self.__dicc__ = HidroCL_Dictionary()
        self.__dicc__.readDiccionario(pathdicc)

    # Obtiene los metadatos asociados con la variable
    def __getMetadata__(self, attribute):
      if attribute in self.fields:
        return self.__dicc__[attribute].get(self.fields[attribute])
      else:
        Dic_Var = pd.read_csv(self.varpath)
        if self.varcod in Dic_Var['New_code'].values:
            row = Dic_Var[Dic_Var['New_code'] == self.varcod]
            if attribute == 'New_code':
                return row['New_code'].values[0]
            elif attribute == 'path':
                return row['Relative_path'].values[0]
            elif attribute == 'Latency':
                return row['Latencia'].values[0]
            elif attribute == 'Scale_Factor':
                return row['Scale_Factor'].values[0]
            elif attribute == 'Unit':
                return row['Unit'].values[0]
            else:
                print(f'El atributo {attribute} no existe')
                return None
        else:
          if (self.fields["TemporalAgg"] in ["sum", "mean"] or
             (self.fields["Period"].startswith("d") and int(self.fields["Period"][1:]) > 1) or
             (self.fields["ValidTime"].startswith("p") and int(self.fields["ValidTime"][1:].rstrip("d")) in range(0,5)) or
             (self.fields["ValidTime"].startswith("m") and int(self.fields["ValidTime"][1:].rstrip("d")) in range(1,int(self.fields["ValidTime"][1:].rstrip("d")) + 1)) or
             (self.fields["ValidTime"].isdigit() and int(self.fields["ValidTime"]) > 1)):
             fields_mod = self.fields.copy()
             fields_mod["TemporalAgg"] = "none"
             fields_mod["Period"] = "d1"
             fields_mod["ValidTime"] = "p0d"
             varcod_mod = "_".join(fields_mod.values())
             #print(f"Variable actual: {self.varcod}, variable modificada: {varcod_mod}")
             return HidroCL_Variable(varcod_mod, pathdicc=self.pathdicc, varpath=self.varpath).__getMetadata__(attribute)
          else:
                print(f'La variable {self.varcod} no se encuentra en el archivo')
                return None

    # Lee los datos de la variable, se le agregó un if para filtrar las fechas desde un punto en especifico
    #start_date = '2000-01-01'
    def __readData__(self):
        print(self.varcod)
        if self.path is not None:
            self.path = os.path.join(os.path.dirname(self.varpath), self.path)
            try:
                self.data = pd.read_csv(self.path)
                # Filtrar por fechas mayores o iguales a '2015-01-01'
                if 'date' in self.data.columns:
                    self.data['date'] = pd.to_datetime(self.data['date'])
                    self.data.sort_values('date', inplace=True)
                    self.data = self.data.loc[self.data['date'] >= self.start_date]

                # Para que lea excepciones
                if self.varcod in ['centroides']:
                    return self.data

                if self.fields['Type'] in ['o', 'f'] and 'name_id' in self.data.columns:
                    self.data = self.data.drop(columns=['name_id'])
                return self.data

            except FileNotFoundError:
                print(f"No se pudo encontrar el archivo en la ruta {self.path}")
                return None
        else:
            print(f"No se puede leer el archivo debido a que no se encontró la ruta para la variable {self.varcod}")
            return None

     #  Comprueba si las fechas de los datos de la variable están en orden y si faltan algunas fechas (EN EXTRICTO RIGOR DEBEN ESTAR COMPLETAS LAS FECHAS)
    def __checkDates__(self):
     #if self.fields['Type'] == 's':
     if ('Type' in self.fields and self.fields['Type'] == 's') or self.varcod == 'p_mean_cr2met_1979_2010':
        print("Las fechas no necesitan ser ordenadas para el tipo 's' o la variable 'p_mean_cr2met_1979_2010'.")
        return self.data
     else:
        self.data['date'] = pd.to_datetime(self.data['date'])
        if not self.data['date'].is_monotonic_increasing:
            self.data = self.data.sort_values(by='date')
        date_range = pd.date_range(start = self.start_date, end =  pd.Timestamp.now().normalize(),freq='D')#end_date= self.data['date'].max()
        #date_range = pd.date_range(start = self.start_date, end = '2021-12-31', freq='D')
        missing_dates = set(date_range) - set(self.data['date'])
        if missing_dates:
            nan_array = np.empty((len(missing_dates), self.data.shape[1] - 1))
            nan_array[:] = np.nan
            new_data = pd.DataFrame(np.column_stack((list(missing_dates), nan_array)), columns=self.data.columns)
            self.data = pd.concat([self.data, new_data], sort=False).sort_values(by='date').reset_index(drop=True)
            print(f"Variable '{self.varcod}': {len(missing_dates)} fechas han sido agregadas con valores NaN.")
           # print(f"{len(missing_dates)} fechas han sido agregadas con valores NaN a la variable {self.varcod}.")
        return self.data

    #### OPERACIONES MOVILES ####
    def __getMovil__(self):
       if self.data is not None:
           if self.fields['Type'] == 's' or self.fields['TemporalAgg'] == 'none' or self.fields['Period'] == 'd1' or self.varcod == 'p_mean_cr2met_1979_2010':
               return self.data
           columns_to_exclude = self.data.iloc[:, :1]
           #self.data = self.data.iloc[:, 1:]
           self.data = self.data.drop(columns=self.data.columns[0]) # Excluir columna date
           if self.fields["TemporalAgg"] == "sum":
               self.data = self.data.rolling(int(self.fields["Period"][1:])).sum()
               self.data = pd.concat([columns_to_exclude, self.data], axis=1)
               return self.data
           elif self.fields["TemporalAgg"] == "mean":
               self.data = self.data.rolling(int(self.fields["Period"][1:])).mean()
               self.data = pd.concat([columns_to_exclude, self.data], axis=1)
               return self.data
           elif self.fields["TemporalAgg"] == "max":
               self.data = self.data.rolling(int(self.fields["Period"][1:])).max()
               self.data = pd.concat([columns_to_exclude, self.data], axis=1)
               return self.data

    #### REALIZA EL LAG A LAS VARIABLES ####
    def __getLag__(self):
       if self.fields['Type'] == 's' or self.varcod == 'p_mean_cr2met_1979_2010':
           return self.data
       columns_to_exclude = self.data.iloc[:, :1]
       self.data = self.data.iloc[:, 1:]  # Excluir columna date
       if self.fields["ValidTime"].startswith("m"):
           shift = int(''.join(filter(str.isdigit, self.fields["ValidTime"])))
           self.data = self.data.shift(shift)
           self.data = pd.concat([columns_to_exclude, self.data], axis=1)
           return self.data
       elif self.fields["ValidTime"] == 'p0d':
          self.data = pd.concat([columns_to_exclude, self.data], axis=1)
          return self.data

    #### FORMATEA LOS DATOS PARA QUE SEAN COMPATIBLES CON OTROS MÉTODOS DE LA CLASE ####
    def __getFormat__(self):
        if self.data is None:
                print(f"No hay datos disponibles para formatear la variable '{self.varcod}'. Verifique si los datos se leyeron correctamente.")
                return None
        if self.varcod == 'p_mean_cr2met_1979_2010':
            return self.data
        elif self.fields['Type'] == 's':
            self.data = self.data[['gauge_id', self.varcod]]
            self.data['gauge_id'] = self.data['gauge_id'].astype('int64')
            self.data.sort_values(by='gauge_id', inplace=True)
        else:
            self.data = self.data.melt(id_vars=['date'], var_name="gauge_id", value_name=self.varcod)
            self.data = self.data.groupby(['gauge_id', 'date'], as_index=False).first()
            self.data['gauge_id'] = self.data['gauge_id'].astype('int64')
            self.data.sort_values(by=['date', 'gauge_id'], inplace=True)
        return self.data


    #### Obtiene todos los datos de la variable en un solo DataFrame y se aplican diferentes transformaciones a los datos según los metadatos asociados con la variable
    def __getAllData__(self):
      self.data = self.__readData__()
      if self.varcod in ['caudal_max_p0d', 'caudal_max_p1d','caudal_max_p2d','caudal_max_p3d','caudal_max_p4d',
                         'caudal_mean_p0d', 'caudal_mean_p1d', 'caudal_mean_p2d', 'caudal_mean_p3d','caudal_mean_p4d',
                         'caudal_mask2_p0d', 'caudal_mask2_p1d', 'caudal_mask2_p2d', 'caudal_mask2_p3d', 'caudal_mask2_p4d']:
          self.__checkDates__()
          return self.__getFormat__()

      if self.fields['Type'] == 's'  or self.varcod == 'p_mean_cr2met_1979_2010':
          return self.__getFormat__()
      self.__checkDates__()
      if self.fields['Type'] != 'f' and  self.fields['Type'] != 's' and self.varcod != 'p_mean_cr2met_1979_2010': #and self.fields['Period'] != 'd1' and self.fields['TemporalAgg'] != 'none':
          self.__getMovil__()
          self.__getLag__()
      if self.fields['Type'] == 'f' and self.fields["ValidTime"].startswith("m"):
          self.__getLag__()
      return self.__getFormat__()


    #### GENERA EL CSV Y LO GUARDA CON EL NOMBRE RESPECTIVO ####
    def getMerge(self, varcod_list):
        first_variable = True
        for varcod in varcod_list:
            variable = HidroCL_Variable(varcod, self.pathdicc, self.varpath)
            data = variable.__getAllData__()
            if first_variable:
                df = data
                first_variable = False
            else:
                if 'date' in data.columns:
                    df = pd.merge(df, data, on=['gauge_id', 'date'], how='left')
                else:
                    df = pd.merge(df, data, on='gauge_id', how='left')

        return df



HidroCL = HidroCL_Variable('pp_f_gfs_pp_max_b_none_d1_p0d')
print("Ruta completa del directorio de salida:", dir_path)
#HidroCL = HidroCL_Variable('pp_f_gfs_pp_max_b_none_d1_p0d', '~/LSTM_model/DFs_IMPUTADOS/Diccionario.csv', '~/LSTM_model/DFs_IMPUTADOS/BaseDatos2024.csv')

#####################################################################################################################################
####################################################################################################################################
'''
lista_RF = ['tmp_f_gfs_tmp_min_b_none_d1_p0d', 'tmp_f_gfs_tmp_min_b_none_d1_p1d', 'tmp_f_gfs_tmp_min_b_none_d1_p2d', 'tmp_f_gfs_tmp_min_b_none_d1_p3d', 'tmp_f_gfs_tmp_min_b_none_d1_p4d',
 'tmp_f_gfs_tmp_max_b_none_d1_p0d', 'tmp_f_gfs_tmp_max_b_none_d1_p1d', 'tmp_f_gfs_tmp_max_b_none_d1_p2d', 'tmp_f_gfs_tmp_max_b_none_d1_p3d', 'tmp_f_gfs_tmp_max_b_none_d1_p4d',
 'pp_f_gfs_pp_max_b_none_d1_p0d', 'pp_f_gfs_pp_max_b_none_d1_p1d', 'pp_f_gfs_pp_max_b_none_d1_p2d', 'pp_f_gfs_pp_max_b_none_d1_p3d', 'pp_f_gfs_pp_max_b_none_d1_p4d',
 'pp_o_era5_plen_mean_b_none_d1_p0d', 'pp_f_gfs_plen_mean_b_none_d1_p0d', 'pp_f_gfs_plen_mean_b_none_d1_p1d', 'pp_f_gfs_plen_mean_b_none_d1_p2d', 'pp_f_gfs_plen_mean_b_none_d1_p3d',
 'pp_f_gfs_plen_mean_b_none_d1_p4d', 'pp_f_gfs_pp_mean_b_none_d1_p0d', 'pp_f_gfs_pp_mean_b_none_d1_p1d', 'pp_f_gfs_pp_mean_b_none_d1_p2d', 'pp_f_gfs_pp_mean_b_none_d1_p3d',
 'pp_f_gfs_pp_mean_b_none_d1_p4d', 'pp_o_imerg_pp_mean_b_none_d1_m1d', 'pp_o_imerg_pp_mean_b_none_d1_m2d', 'pp_o_imerg_pp_mean_b_none_d1_m3d', 'pp_o_imerg_pp_mean_b_none_d1_m4d',
 'pp_o_imerg_pp_mean_b_none_d1_m5d', 'pp_o_imerg_pp_mean_b_none_d1_m6d', 'pp_o_imerg_pp_mean_b_none_d1_m7d', 'pp_o_imerg_pp_mean_b_none_d1_m8d', 'pp_o_imerg_pp_mean_b_none_d1_m9d',
 'pp_o_imerg_pp_mean_b_none_d1_m10d', 'pp_o_imerg_pp_mean_b_none_d1_m11d', 'pp_o_imerg_pp_mean_b_none_d1_m12d', 'pp_o_imerg_pp_mean_b_none_d1_m13d', 'pp_o_imerg_pp_mean_b_none_d1_m14d',
 'pp_o_imerg_pp_mean_b_none_d1_m15d', 'pp_o_imerg_pp_mean_b_none_d1_m16d', 'pp_o_imerg_pp_mean_b_sum_d30_m1d', 'pp_o_imerg_pp_mean_b_sum_d90_m1d', 'pp_o_imerg_pp_mean_b_sum_d180_m1d',
 'pp_o_imerg_pp_mean_b_sum_d365_m1d', 'pp_o_pdir_pp_mean_b_none_d1_m1d', 'pp_o_pdir_pp_mean_b_none_d1_m2d', 'pp_o_pdir_pp_mean_b_none_d1_m3d', 'pp_o_pdir_pp_mean_b_none_d1_m4d',
 'pp_o_pdir_pp_mean_b_none_d1_m5d', 'pp_o_pdir_pp_mean_b_none_d1_m6d', 'pp_o_pdir_pp_mean_b_none_d1_m7d', 'pp_o_pdir_pp_mean_b_none_d1_m8d', 'pp_o_pdir_pp_mean_b_none_d1_m9d',
 'pp_o_pdir_pp_mean_b_none_d1_m10d', 'pp_o_pdir_pp_mean_b_none_d1_m11d', 'pp_o_pdir_pp_mean_b_none_d1_m12d', 'pp_o_pdir_pp_mean_b_none_d1_m13d', 'pp_o_pdir_pp_mean_b_none_d1_m14d',
 'pp_o_pdir_pp_mean_b_none_d1_m15d', 'pp_o_pdir_pp_mean_b_none_d1_m16d', 'pp_o_pdir_pp_mean_b_sum_d30_m1d', 'pp_o_pdir_pp_mean_b_sum_d90_m1d', 'pp_o_pdir_pp_mean_b_sum_d180_m1d',
 'pp_o_pdir_pp_mean_b_sum_d365_m1d', 'pp_o_era5_pp_mean_b_none_d1_m5d', 'pp_o_era5_pp_mean_b_none_d1_m6d', 'pp_o_era5_pp_mean_b_none_d1_m7d', 'pp_o_era5_pp_mean_b_none_d1_m8d',
 'pp_o_era5_pp_mean_b_none_d1_m9d', 'pp_o_era5_pp_mean_b_none_d1_m10d', 'pp_o_era5_pp_mean_b_none_d1_m11d', 'pp_o_era5_pp_mean_b_none_d1_m12d', 'pp_o_era5_pp_mean_b_none_d1_m13d',
 'pp_o_era5_pp_mean_b_none_d1_m14d', 'pp_o_era5_pp_mean_b_none_d1_m15d', 'pp_o_era5_pp_mean_b_none_d1_m16d', 'pp_o_era5_pp_mean_b_sum_d30_m5d', 'pp_o_era5_pp_mean_b_sum_d90_m5d',
 'pp_o_era5_pp_mean_b_sum_d180_m5d', 'pp_o_era5_pp_mean_b_sum_d365_m5d', 'snw_o_modis_sca_tot_n_none_d1_m15d', 'snw_o_modis_sca_tot_n_max_d180_m15d', 'snw_o_modis_sca_tot_n_none_d1_m23d',
 'snw_o_modis_sca_tot_s_none_d1_m15d', 'snw_o_modis_sca_tot_s_max_d180_m15d', 'snw_o_era5_snr_mean_b_none_d1_m5d', 'snw_o_era5_snd_mean_b_none_d1_m5d', 'snw_o_era5_sca_mean_b_none_d1_m5d',
 'snw_o_era5_sna_mean_b_none_d1_m5d', 'atm_f_gfs_uw_mean_b_none_d1_p0d', 'atm_f_gfs_vw_mean_b_none_d1_p0d', 'atm_f_gfs_gh_mean_b_none_d1_p0d', 'atm_f_gfs_gh_mean_b_none_d1_p1d',
 'atm_f_gfs_gh_mean_b_none_d1_p2d', 'atm_f_gfs_gh_mean_b_none_d1_p3d', 'atm_f_gfs_gh_mean_b_none_d1_p4d', 'atm_o_era5_pres_mean_b_mean_d10_m5d', 'atm_o_era5_z_mean_b_mean_d10_m5d',
 'atm_o_era5_uw_mean_b_mean_d10_m5d', 'atm_o_era5_vw_mean_b_mean_d10_m5d', 'awc_f_gfs_rh_mean_b_none_d1_p0d', 'awc_f_gfs_rh_mean_b_none_d1_p1d', 'awc_f_gfs_rh_mean_b_none_d1_p2d',
 'awc_f_gfs_rh_mean_b_none_d1_p3d', 'awc_f_gfs_rh_mean_b_none_d1_p4d', 'awc_o_era5_rh_mean_b_mean_d10_m5d', 'gl_s_dga_ga_tot_n_none_c_c', 'p_mean_cr2met_1979_2010',
 'gl_s_dga_rgla_tot_n_none_c_c', 'gl_s_dga_gta_tot_n_none_c_c', 'gl_s_dga_gwe_tot_n_none_c_c', 'gl_s_dga_rglwe_tot_n_none_c_c', 'gl_s_dga_gtwe_tot_n_none_c_c',
 'gl_s_dga_galt_mean_n_none_c_c', 'gl_s_dga_rglalt_mean_n_none_c_c', 'gl_s_dga_gtalt_mean_n_none_c_c', 'gl_s_dga_ga_tot_s_none_c_c', 'gl_s_dga_rgla_tot_s_none_c_c',
 'gl_s_dga_gta_tot_s_none_c_c', 'gl_s_dga_gwe_tot_s_none_c_c', 'gl_s_dga_rglwe_tot_s_none_c_c', 'gl_s_dga_gtwe_tot_s_none_c_c', 'gl_s_dga_galt_mean_s_none_c_c',
 'gl_s_dga_rglalt_mean_s_none_c_c', 'gl_s_dga_gtalt_mean_s_none_c_c', 'tmp_o_era5_tmp_mean_b_mean_d10_m5d', 'tmp_o_era5_tmp_mean_b_mean_d30_m5d', 'tmp_o_era5_tmp_mean_b_mean_d90_m5d',
 'tmp_o_era5_tmp_mean_b_mean_d180_m5d', 'tmp_o_era5_tmp_mean_b_mean_d365_m5d', 'tmp_o_era5_tmp_mean_b_none_d1_m5d', 'tmp_o_era5_tmp_mean_b_none_d1_m6d', 'tmp_o_era5_tmp_mean_b_none_d1_m7d',
 'tmp_o_era5_tmp_mean_b_none_d1_m8d', 'tmp_o_era5_tmp_mean_b_none_d1_m9d', 'tmp_o_era5_tmp_mean_b_none_d1_m10d', 'tmp_o_era5_tmp_mean_b_none_d1_m11d', 'tmp_o_era5_tmp_mean_b_none_d1_m12d',
 'tmp_o_era5_tmp_mean_b_none_d1_m13d', 'tmp_o_era5_tmp_mean_b_none_d1_m14d', 'tmp_o_era5_tmp_mean_b_none_d1_m15d', 'tmp_o_era5_tmp_mean_b_none_d1_m16d', 'tmp_o_era5_tmin_mean_b_none_d1_m5d',
 'tmp_o_era5_tmin_mean_b_none_d1_m6d', 'tmp_o_era5_tmin_mean_b_none_d1_m7d', 'tmp_o_era5_tmin_mean_b_none_d1_m8d', 'tmp_o_era5_tmin_mean_b_none_d1_m9d', 'tmp_o_era5_tmin_mean_b_none_d1_m10d',
 'tmp_o_era5_tmin_mean_b_none_d1_m11d', 'tmp_o_era5_tmin_mean_b_none_d1_m12d', 'tmp_o_era5_tmin_mean_b_none_d1_m13d', 'tmp_o_era5_tmin_mean_b_none_d1_m14d', 'tmp_o_era5_tmin_mean_b_none_d1_m15d',
 'tmp_o_era5_tmin_mean_b_none_d1_m16d', 'tmp_o_era5_tmin_mean_b_mean_d10_m5d', 'tmp_o_era5_tmin_mean_b_mean_d30_m5d', 'tmp_o_era5_tmin_mean_b_mean_d90_m5d', 'tmp_o_era5_tmin_mean_b_mean_d180_m5d',
 'tmp_o_era5_tmin_mean_b_mean_d365_m5d', 'tmp_o_era5_tmax_mean_b_none_d1_m5d', 'tmp_o_era5_tmax_mean_b_none_d1_m6d', 'tmp_o_era5_tmax_mean_b_none_d1_m7d', 'tmp_o_era5_tmax_mean_b_none_d1_m8d',
 'tmp_o_era5_tmax_mean_b_none_d1_m9d', 'tmp_o_era5_tmax_mean_b_none_d1_m10d', 'tmp_o_era5_tmax_mean_b_none_d1_m11d', 'tmp_o_era5_tmax_mean_b_none_d1_m12d', 'tmp_o_era5_tmax_mean_b_none_d1_m13d',
 'tmp_o_era5_tmax_mean_b_none_d1_m14d', 'tmp_o_era5_tmax_mean_b_none_d1_m15d', 'tmp_o_era5_tmax_mean_b_none_d1_m16d', 'tmp_o_era5_tmax_mean_b_mean_d10_m5d', 'tmp_o_era5_tmax_mean_b_mean_d30_m5d',
 'tmp_o_era5_tmax_mean_b_mean_d90_m5d', 'tmp_o_era5_tmax_mean_b_mean_d180_m5d', 'tmp_o_era5_tmax_mean_b_mean_d365_m5d', 'tmp_o_era5_dew_mean_b_mean_d10_m5d', 'tmp_f_gfs_tmp_mean_b_none_d1_p0d',
 'tmp_f_gfs_tmp_mean_b_none_d1_p1d', 'tmp_f_gfs_tmp_mean_b_none_d1_p2d', 'tmp_f_gfs_tmp_mean_b_none_d1_p3d', 'tmp_f_gfs_tmp_mean_b_none_d1_p4d', 'tmp_f_gfs_tmp_mean_b_none_d1_m1d',
 'tmp_f_gfs_tmp_mean_b_none_d1_m2d', 'tmp_f_gfs_tmp_mean_b_none_d1_m3d', 'tmp_f_gfs_tmp_mean_b_none_d1_m4d', 'tmp_f_gfs_tmp_mean_b_none_d1_m5d', 'et_o_era5_eta_mean_b_mean_d10_m5d',
 'et_o_era5_eta_mean_b_mean_d30_m5d', 'et_o_era5_eta_mean_b_mean_d90_m5d', 'et_o_era5_eta_mean_b_mean_d180_m5d', 'et_o_era5_eta_mean_b_mean_d365_m5d', 'et_o_era5_eto_mean_b_mean_d10_m5d',
 'sf_s_isric_socc_mean_b_none_c_c', 'sf_s_isric_socc_p10_b_none_c_c', 'sf_s_isric_socc_p25_b_none_c_c', 'sf_s_isric_socc_p50_b_none_c_c', 'sf_s_isric_socc_p75_b_none_c_c',
 'sf_s_isric_socc_p90_b_none_c_c', 'sf_s_isric_sbd_mean_b_none_c_c', 'sf_s_isric_sbd_p10_b_none_c_c', 'sf_s_isric_sbd_p25_b_none_c_c', 'sf_s_isric_sbd_p50_b_none_c_c',
 'sf_s_isric_sbd_p75_b_none_c_c', 'sf_s_isric_sbd_p90_b_none_c_c', 'sf_s_isric_scf_mean_b_none_c_c', 'sf_s_isric_scf_p10_b_none_c_c', 'sf_s_isric_scf_p25_b_none_c_c',
 'sf_s_isric_scf_p50_b_none_c_c', 'sf_s_isric_scf_p75_b_none_c_c', 'sf_s_isric_scf_p90_b_none_c_c', 'sf_s_isric_sand_mean_b_none_c_c', 'sf_s_isric_sand_p10_b_none_c_c',
 'sf_s_isric_sand_p25_b_none_c_c', 'sf_s_isric_sand_p50_b_none_c_c', 'sf_s_isric_sand_p75_b_none_c_c', 'sf_s_isric_sand_p90_b_none_c_c', 'sf_s_isric_silt_mean_b_none_c_c',
 'sf_s_isric_silt_p10_b_none_c_c', 'sf_s_isric_silt_p25_b_none_c_c', 'sf_s_isric_silt_p50_b_none_c_c', 'sf_s_isric_silt_p75_b_none_c_c', 'sf_s_isric_silt_p90_b_none_c_c',
 'sf_s_isric_clay_mean_b_none_c_c', 'sf_s_isric_clay_p10_b_none_c_c', 'sf_s_isric_clay_p25_b_none_c_c', 'sf_s_isric_clay_p50_b_none_c_c', 'sf_s_isric_clay_p75_b_none_c_c',
 'sf_s_isric_clay_p90_b_none_c_c', 'sf_s_ornl_brd_p10_b_none_c_c', 'sf_s_ornl_brd_p25_b_none_c_c', 'sf_s_ornl_brd_p50_b_none_c_c', 'sf_s_ornl_brd_p75_b_none_c_c',
 'sf_s_ornl_brd_p90_b_none_c_c', 'sf_s_ornl_brd_mean_b_none_c_c', 'sf_s_ornl_brd_tot_p_none_c_c', 'sf_s_glhym_perm_mean_b_none_c_c', 'sf_s_glhym_por_mean_b_none_c_c',
 'swc_o_era5_sm_mean_b_none_d1_m5d', 'top_s_cam_area_tot_b_none_c_c', 'top_s_cam_elev_mean_b_none_c_c', 'top_s_cam_elev_p50_b_none_c_c', 'top_s_cam_elev_max_b_none_c_c',
 'top_s_cam_elev_min_b_none_c_c', 'top_s_cam_slo_mean_b_none_c_c', 'top_s_dem_com_none_b_none_c_c', 'top_s_dem_cc_none_b_none_c_c', 'top_s_dem_ti_none_b_none_c_c',
 'veg_o_modis_lai_mean_b_none_d1_m15d', 'veg_o_modis_fpar_mean_b_none_d1_m15d', 'veg_o_modis_ndvi_mean_b_none_d1_m21d', 'veg_o_modis_agr_mean_b_none_d1_m21d',
 'veg_o_modis_evi_mean_b_none_d1_m21d', 'veg_o_modis_nbr_mean_b_none_d1_m21d', 'lulc_o_modis_enf_sum_b_none_d1_p0d', 'lulc_o_modis_ebf_sum_b_none_d1_p0d', 'lulc_o_modis_dnf_sum_b_none_d1_p0d',
 'lulc_o_modis_dbf_sum_b_none_d1_p0d', 'lulc_o_modis_mxf_sum_b_none_d1_p0d', 'lulc_o_modis_csh_sum_b_none_d1_p0d', 'lulc_o_modis_osh_sum_b_none_d1_p0d', 'lulc_o_modis_wsv_sum_b_none_d1_p0d',
 'lulc_o_modis_svn_sum_b_none_d1_p0d', 'lulc_o_modis_grs_sum_b_none_d1_p0d', 'lulc_o_modis_pwt_sum_b_none_d1_p0d', 'lulc_o_modis_crp_sum_b_none_d1_p0d', 'lulc_o_modis_urb_sum_b_none_d1_p0d',
 'lulc_o_modis_cvm_sum_b_none_d1_p0d', 'lulc_o_modis_snw_sum_b_none_d1_p0d', 'lulc_o_modis_brn_sum_b_none_d1_p0d', 'lulc_o_modis_wat_sum_b_none_d1_p0d', 'hi_s_cam_sr_tot_b_none_c_c',
 'hi_s_cam_gwr_tot_b_none_c_c', 'idx_s_cam_arcr2_tot_b_none_c_c', 'hi_o_gww_rs_tot_b_none_d1_m45d', 'top_s_cam_lon_mean_b_none_c_c', 'top_s_cam_lat_mean_b_none_c_c',
 'top_s_cam_lon_none_p_none_c_c', 'top_s_cam_lat_none_p_none_c_c', 'top_s_dga_lon_none_p_none_c_c', 'top_s_dga_lat_none_p_none_c_c', 'pp_o_era5_maxpp_mean_b_none_d1_m5d',
 'lulc_o_modis_brn_frac_b_none_d1_p0d', 'lulc_o_modis_cvm_frac_b_none_d1_p0d', 'lulc_o_modis_csh_frac_b_none_d1_p0d', 'lulc_o_modis_grs_frac_b_none_d1_p0d', 'lulc_o_modis_urb_frac_b_none_d1_p0d',
 'lulc_o_modis_pwt_frac_b_none_d1_p0d', 'lulc_o_modis_crp_frac_b_none_d1_p0d', 'lulc_o_modis_snw_frac_b_none_d1_p0d', 'lulc_o_modis_wsv_frac_b_none_d1_p0d', 'lulc_o_modis_enf_frac_b_none_d1_p0d',
 'lulc_o_modis_ebf_frac_b_none_d1_p0d', 'lulc_o_modis_wat_frac_b_none_d1_p0d', 'lulc_o_modis_svn_frac_b_none_d1_p0d', 'lulc_o_modis_dnf_frac_b_none_d1_p0d', 'lulc_o_modis_dbf_frac_b_none_d1_p0d',
 'lulc_o_modis_osh_frac_b_none_d1_p0d', 'lulc_o_modis_mxf_frac_b_none_d1_p0d', 'caudal_max_p0d', 'caudal_max_p1d', 'caudal_max_p2d',
 'caudal_max_p3d', 'caudal_max_p4d', 'caudal_mean_p0d', 'caudal_mean_p1d', 'caudal_mean_p2d', 'caudal_mean_p3d', 'caudal_mean_p4d', 'caudal_mask2_p0d', 'caudal_mask2_p1d',
 'caudal_mask2_p2d', 'caudal_mask2_p3d', 'caudal_mask2_p4d', 'atm_f_gfs_uw_mean_b_none_d1_p1d', 'atm_f_gfs_uw_mean_b_none_d1_p2d', 'atm_f_gfs_uw_mean_b_none_d1_p3d',
 'atm_f_gfs_uw_mean_b_none_d1_p4d', 'atm_f_gfs_vw_mean_b_none_d1_p1d', 'atm_f_gfs_vw_mean_b_none_d1_p2d', 'atm_f_gfs_vw_mean_b_none_d1_p3d', 'atm_f_gfs_vw_mean_b_none_d1_p4d'] 


print(len(lista_RF))
df1 = HidroCL.getMerge(lista_RF)
if 'date' in df1.columns:
    fecha_mas_reciente = df1['date'].max()
    print(fecha_mas_reciente)
    df1 = df1[df1['date'] == fecha_mas_reciente]

    # Guardar el dataframe en un archivo CSV
    try:
        output_file_path = os.path.join(dir_path, 'RF.csv')
        df1.to_csv(output_file_path, index=False)
        print(f"Archivo guardado correctamente en {output_file_path}")
    except Exception as e:
        print(f"Error al guardar el archivo: {e}")
        raise Exception("No se pudo guardar el archivo debido a un error de entrada/salida.") from e  
    print('RF:', df1.head())  # Muestra solo las primeras filas para evitar sobrecarga de información
    print(len(df1.columns.tolist()))
else:
    raise ValueError("Error: No se encontró la columna 'date' en el dataframe.")

#fecha_mas_reciente = df1['date'].max()
#print(fecha_mas_reciente)
#df1 = df1[df1['date'] == fecha_mas_reciente]
#df1.to_csv(f'{dir_path}/RF.csv', index = False)

'''
######################################################
############## VARIABLES ACTUALIZADAS ################
######################################################

#HidroCL = HidroCL_Variable('pp_f_gfs_pp_max_b_none_d1_p0d', '~/LSTM_model/DFs_IMPUTADOS/Diccionario.csv', '~/LSTM_model/DFs_IMPUTADOS/BaseDatos2024.csv')
#dir_path = '~/LSTM_model/Retrain'

list_RF = [  'tmp_f_gfs_tmp_min_b_none_d1_p0d', 'tmp_f_gfs_tmp_min_b_none_d1_p1d', 'tmp_f_gfs_tmp_min_b_none_d1_p2d', 'tmp_f_gfs_tmp_min_b_none_d1_p3d', 'tmp_f_gfs_tmp_min_b_none_d1_p4d', 'tmp_f_gfs_tmp_min_b_none_d1_m1d', 'tmp_f_gfs_tmp_min_b_none_d1_m2d', 'tmp_f_gfs_tmp_min_b_none_d1_m3d', 'tmp_f_gfs_tmp_min_b_none_d1_m4d',
 'tmp_f_gfs_tmp_min_b_none_d1_m5d', 'tmp_f_gfs_tmp_min_b_none_d1_m6d', 'tmp_f_gfs_tmp_max_b_none_d1_p0d', 'tmp_f_gfs_tmp_max_b_none_d1_p1d', 'tmp_f_gfs_tmp_max_b_none_d1_p2d', 'tmp_f_gfs_tmp_max_b_none_d1_p3d', 'tmp_f_gfs_tmp_max_b_none_d1_p4d', 'tmp_f_gfs_tmp_max_b_none_d1_m1d', 'tmp_f_gfs_tmp_max_b_none_d1_m2d',
 'tmp_f_gfs_tmp_max_b_none_d1_m3d', 'tmp_f_gfs_tmp_max_b_none_d1_m4d', 'tmp_f_gfs_tmp_max_b_none_d1_m5d', 'tmp_f_gfs_tmp_max_b_none_d1_m6d', 'tmp_f_gfs_tmp_mean_b_none_d1_p0d', 'tmp_f_gfs_tmp_mean_b_none_d1_p1d', 'tmp_f_gfs_tmp_mean_b_none_d1_p2d', 'tmp_f_gfs_tmp_mean_b_none_d1_p3d', 'tmp_f_gfs_tmp_mean_b_none_d1_p4d',
 'tmp_f_gfs_tmp_mean_b_none_d1_m1d', 'tmp_f_gfs_tmp_mean_b_none_d1_m2d', 'tmp_f_gfs_tmp_mean_b_none_d1_m3d', 'tmp_f_gfs_tmp_mean_b_none_d1_m4d', 'tmp_f_gfs_tmp_mean_b_none_d1_m5d', 'tmp_f_gfs_tmp_mean_b_none_d1_m6d', 'tmp_o_era5_tmp_mean_b_none_d1_m7d', 'tmp_o_era5_tmp_mean_b_none_d1_m8d', 'tmp_o_era5_tmp_mean_b_none_d1_m9d',
 'tmp_o_era5_tmp_mean_b_none_d1_m10d', 'tmp_o_era5_tmp_mean_b_none_d1_m11d', 'tmp_o_era5_tmp_mean_b_none_d1_m12d', 'tmp_o_era5_tmp_mean_b_none_d1_m13d', 'tmp_o_era5_tmp_mean_b_none_d1_m14d', 'tmp_o_era5_tmp_mean_b_none_d1_m15d', 'tmp_o_era5_tmp_mean_b_none_d1_m16d',  'tmp_o_era5_tmp_mean_b_mean_d30_m7d',
 'tmp_o_era5_tmp_mean_b_mean_d90_m7d', 'tmp_o_era5_tmp_mean_b_mean_d180_m7d', 'tmp_o_era5_tmp_mean_b_mean_d365_m7d',  'tmp_o_era5_tmin_mean_b_none_d1_m7d', 'tmp_o_era5_tmin_mean_b_none_d1_m8d', 'tmp_o_era5_tmin_mean_b_none_d1_m9d', 'tmp_o_era5_tmin_mean_b_none_d1_m10d', 'tmp_o_era5_tmin_mean_b_none_d1_m11d',
 'tmp_o_era5_tmin_mean_b_none_d1_m12d', 'tmp_o_era5_tmin_mean_b_none_d1_m13d', 'tmp_o_era5_tmin_mean_b_none_d1_m14d', 'tmp_o_era5_tmin_mean_b_none_d1_m15d', 'tmp_o_era5_tmin_mean_b_none_d1_m16d',  'tmp_o_era5_tmin_mean_b_mean_d30_m7d', 'tmp_o_era5_tmin_mean_b_mean_d90_m7d', 'tmp_o_era5_tmin_mean_b_mean_d180_m7d',
 'tmp_o_era5_tmin_mean_b_mean_d365_m7d',  'tmp_o_era5_tmax_mean_b_none_d1_m7d', 'tmp_o_era5_tmax_mean_b_none_d1_m8d', 'tmp_o_era5_tmax_mean_b_none_d1_m9d', 'tmp_o_era5_tmax_mean_b_none_d1_m10d', 'tmp_o_era5_tmax_mean_b_none_d1_m11d', 'tmp_o_era5_tmax_mean_b_none_d1_m12d', 'tmp_o_era5_tmax_mean_b_none_d1_m13d',
 'tmp_o_era5_tmax_mean_b_none_d1_m14d', 'tmp_o_era5_tmax_mean_b_none_d1_m15d', 'tmp_o_era5_tmax_mean_b_none_d1_m16d',  'tmp_o_era5_tmax_mean_b_mean_d30_m7d', 'tmp_o_era5_tmax_mean_b_mean_d90_m7d', 'tmp_o_era5_tmax_mean_b_mean_d180_m7d', 'tmp_o_era5_tmax_mean_b_mean_d365_m7d', 'tmp_o_era5_dew_mean_b_mean_d10_m7d',
 'pp_f_gfs_pp_max_b_none_d1_p0d', 'pp_f_gfs_pp_max_b_none_d1_p1d', 'pp_f_gfs_pp_max_b_none_d1_p2d', 'pp_f_gfs_pp_max_b_none_d1_p3d', 'pp_f_gfs_pp_max_b_none_d1_p4d', 'pp_f_gfs_pp_max_b_none_d1_m1d', 'pp_f_gfs_pp_max_b_none_d1_m2d', 'pp_f_gfs_pp_max_b_none_d1_m3d',  'pp_f_gfs_plen_mean_b_none_d1_p0d',
 'pp_f_gfs_plen_mean_b_none_d1_p1d', 'pp_f_gfs_plen_mean_b_none_d1_p2d', 'pp_f_gfs_plen_mean_b_none_d1_p3d', 'pp_f_gfs_plen_mean_b_none_d1_p4d', 'pp_f_gfs_plen_mean_b_none_d1_m1d', 'pp_f_gfs_plen_mean_b_none_d1_m2d', 'pp_f_gfs_plen_mean_b_none_d1_m3d',  'pp_f_gfs_pp_mean_b_none_d1_p0d',
 'pp_f_gfs_pp_mean_b_none_d1_p1d', 'pp_f_gfs_pp_mean_b_none_d1_p2d', 'pp_f_gfs_pp_mean_b_none_d1_p3d', 'pp_f_gfs_pp_mean_b_none_d1_p4d', 'pp_f_gfs_pp_mean_b_none_d1_m1d', 'pp_f_gfs_pp_mean_b_none_d1_m2d', 'pp_f_gfs_pp_mean_b_none_d1_m3d', 'pp_f_gfs_pp_mean_b_none_d1_m4d', 'pp_f_gfs_pp_mean_b_none_d1_m5d',
 'pp_f_gfs_pp_mean_b_none_d1_m6d',  'pp_o_imerg_pp_mean_b_none_d1_m2d', 'pp_o_imerg_pp_mean_b_none_d1_m3d', 'pp_o_imerg_pp_mean_b_none_d1_m4d', 'pp_o_imerg_pp_mean_b_none_d1_m5d', 'pp_o_imerg_pp_mean_b_none_d1_m6d', 'pp_o_imerg_pp_mean_b_none_d1_m7d', 'pp_o_imerg_pp_mean_b_none_d1_m8d',
 'pp_o_imerg_pp_mean_b_none_d1_m9d', 'pp_o_imerg_pp_mean_b_none_d1_m10d', 'pp_o_imerg_pp_mean_b_none_d1_m11d', 'pp_o_imerg_pp_mean_b_none_d1_m12d', 'pp_o_imerg_pp_mean_b_none_d1_m13d', 'pp_o_imerg_pp_mean_b_none_d1_m14d', 'pp_o_imerg_pp_mean_b_none_d1_m15d', 'pp_o_imerg_pp_mean_b_none_d1_m16d',
 'pp_o_imerg_pp_mean_b_sum_d30_m2d', 'pp_o_imerg_pp_mean_b_sum_d90_m2d', 'pp_o_imerg_pp_mean_b_sum_d180_m2d', 'pp_o_imerg_pp_mean_b_sum_d365_m2d',  'pp_o_pdir_pp_mean_b_none_d1_m2d', 'pp_o_pdir_pp_mean_b_none_d1_m3d', 'pp_o_pdir_pp_mean_b_none_d1_m4d', 'pp_o_pdir_pp_mean_b_none_d1_m5d', 'pp_o_pdir_pp_mean_b_none_d1_m6d', 'pp_o_pdir_pp_mean_b_none_d1_m7d', 'pp_o_pdir_pp_mean_b_none_d1_m8d', 'pp_o_pdir_pp_mean_b_none_d1_m9d', 'pp_o_pdir_pp_mean_b_none_d1_m10d', 'pp_o_pdir_pp_mean_b_none_d1_m11d',
 'pp_o_pdir_pp_mean_b_none_d1_m12d', 'pp_o_pdir_pp_mean_b_none_d1_m13d', 'pp_o_pdir_pp_mean_b_none_d1_m14d', 'pp_o_pdir_pp_mean_b_none_d1_m15d', 'pp_o_pdir_pp_mean_b_none_d1_m16d', 'pp_o_pdir_pp_mean_b_sum_d30_m2d', 'pp_o_pdir_pp_mean_b_sum_d90_m2d', 'pp_o_pdir_pp_mean_b_sum_d180_m2d',
 'pp_o_pdir_pp_mean_b_sum_d365_m2d',  'pp_o_era5_pp_mean_b_none_d1_m7d', 'pp_o_era5_pp_mean_b_none_d1_m8d', 'pp_o_era5_pp_mean_b_none_d1_m9d', 'pp_o_era5_pp_mean_b_none_d1_m10d', 'pp_o_era5_pp_mean_b_none_d1_m11d', 'pp_o_era5_pp_mean_b_none_d1_m12d', 'pp_o_era5_pp_mean_b_none_d1_m13d',
 'pp_o_era5_pp_mean_b_none_d1_m14d', 'pp_o_era5_pp_mean_b_none_d1_m15d', 'pp_o_era5_pp_mean_b_none_d1_m16d', 'pp_o_era5_pp_mean_b_sum_d30_m7d', 'pp_o_era5_pp_mean_b_sum_d90_m7d', 'pp_o_era5_pp_mean_b_sum_d180_m7d', 'pp_o_era5_pp_mean_b_sum_d365_m7d',  'snw_o_modis_sca_tot_n_none_d1_m22d',
 'snw_o_modis_sca_tot_n_max_d180_m22d', 'snw_o_modis_sca_tot_n_none_d1_m29d', 'snw_o_modis_sca_tot_s_none_d1_m22d', 'snw_o_modis_sca_tot_s_max_d180_m22d', 'snw_o_modis_sca_tot_s_none_d1_m29d', 'snw_o_era5_snr_mean_b_none_d1_m7d', 'snw_o_era5_snd_mean_b_none_d1_m7d', 'snw_o_era5_sca_mean_b_none_d1_m7d',
 'snw_o_era5_sna_mean_b_none_d1_m7d', 'atm_f_gfs_uw_mean_b_none_d1_p0d', 'atm_f_gfs_uw_mean_b_none_d1_p1d', 'atm_f_gfs_uw_mean_b_none_d1_p2d', 'atm_f_gfs_uw_mean_b_none_d1_p3d', 'atm_f_gfs_uw_mean_b_none_d1_p4d', 'atm_f_gfs_vw_mean_b_none_d1_p0d', 'atm_f_gfs_vw_mean_b_none_d1_p1d',
 'atm_f_gfs_vw_mean_b_none_d1_p2d', 'atm_f_gfs_vw_mean_b_none_d1_p3d', 'atm_f_gfs_vw_mean_b_none_d1_p4d', 'atm_f_gfs_gh_mean_b_none_d1_p0d', 'atm_f_gfs_gh_mean_b_none_d1_p1d', 'atm_f_gfs_gh_mean_b_none_d1_p2d', 'atm_f_gfs_gh_mean_b_none_d1_p3d', 'atm_f_gfs_gh_mean_b_none_d1_p4d',  'awc_f_gfs_rh_mean_b_none_d1_p0d',
 'awc_f_gfs_rh_mean_b_none_d1_p1d', 'awc_f_gfs_rh_mean_b_none_d1_p2d', 'awc_f_gfs_rh_mean_b_none_d1_p3d', 'awc_f_gfs_rh_mean_b_none_d1_p4d',  'gl_s_dga_ga_tot_n_none_c_c', 'p_mean_cr2met_1979_2010', 'gl_s_dga_rgla_tot_n_none_c_c', 'gl_s_dga_gta_tot_n_none_c_c', 'gl_s_dga_gwe_tot_n_none_c_c', 'gl_s_dga_rglwe_tot_n_none_c_c',
 'gl_s_dga_gtwe_tot_n_none_c_c', 'gl_s_dga_galt_mean_n_none_c_c', 'gl_s_dga_rglalt_mean_n_none_c_c', 'gl_s_dga_gtalt_mean_n_none_c_c', 'gl_s_dga_ga_tot_s_none_c_c', 'gl_s_dga_rgla_tot_s_none_c_c', 'gl_s_dga_gta_tot_s_none_c_c', 'gl_s_dga_gwe_tot_s_none_c_c', 'gl_s_dga_rglwe_tot_s_none_c_c', 'gl_s_dga_gtwe_tot_s_none_c_c',
 'gl_s_dga_galt_mean_s_none_c_c', 'gl_s_dga_rglalt_mean_s_none_c_c', 'gl_s_dga_gtalt_mean_s_none_c_c',  'et_o_era5_eta_mean_b_mean_d10_m7d', 'et_o_era5_eta_mean_b_mean_d30_m7d', 'et_o_era5_eta_mean_b_mean_d90_m7d', 'et_o_era5_eta_mean_b_mean_d180_m7d', 'et_o_era5_eta_mean_b_mean_d365_m7d', 'et_o_era5_eto_mean_b_mean_d10_m7d',
 'et_o_era5_eto_mean_b_mean_d30_m7d', 'et_o_era5_eto_mean_b_mean_d90_m7d', 'et_o_era5_eto_mean_b_mean_d180_m7d', 'et_o_era5_eto_mean_b_mean_d365_m7d', 'sf_s_isric_socc_mean_b_none_c_c', 'sf_s_isric_socc_p10_b_none_c_c', 'sf_s_isric_socc_p25_b_none_c_c', 'sf_s_isric_socc_p50_b_none_c_c', 'sf_s_isric_socc_p75_b_none_c_c',
 'sf_s_isric_socc_p90_b_none_c_c', 'sf_s_isric_sbd_mean_b_none_c_c', 'sf_s_isric_sbd_p10_b_none_c_c', 'sf_s_isric_sbd_p25_b_none_c_c', 'sf_s_isric_sbd_p50_b_none_c_c', 'sf_s_isric_sbd_p75_b_none_c_c', 'sf_s_isric_sbd_p90_b_none_c_c', 'sf_s_isric_scf_mean_b_none_c_c', 'sf_s_isric_scf_p10_b_none_c_c', 'sf_s_isric_scf_p25_b_none_c_c',
 'sf_s_isric_scf_p50_b_none_c_c', 'sf_s_isric_scf_p75_b_none_c_c', 'sf_s_isric_scf_p90_b_none_c_c', 'sf_s_isric_sand_mean_b_none_c_c', 'sf_s_isric_sand_p10_b_none_c_c', 'sf_s_isric_sand_p25_b_none_c_c', 'sf_s_isric_sand_p50_b_none_c_c', 'sf_s_isric_sand_p75_b_none_c_c', 'sf_s_isric_sand_p90_b_none_c_c', 'sf_s_isric_silt_mean_b_none_c_c',
 'sf_s_isric_silt_p10_b_none_c_c', 'sf_s_isric_silt_p25_b_none_c_c', 'sf_s_isric_silt_p50_b_none_c_c', 'sf_s_isric_silt_p75_b_none_c_c', 'sf_s_isric_silt_p90_b_none_c_c', 'sf_s_isric_clay_mean_b_none_c_c', 'sf_s_isric_clay_p10_b_none_c_c', 'sf_s_isric_clay_p25_b_none_c_c', 'sf_s_isric_clay_p50_b_none_c_c',
 'sf_s_isric_clay_p75_b_none_c_c', 'sf_s_isric_clay_p90_b_none_c_c', 'sf_s_ornl_brd_p10_b_none_c_c', 'sf_s_ornl_brd_p25_b_none_c_c', 'sf_s_ornl_brd_p50_b_none_c_c', 'sf_s_ornl_brd_p75_b_none_c_c', 'sf_s_ornl_brd_p90_b_none_c_c', 'sf_s_ornl_brd_mean_b_none_c_c', 'sf_s_ornl_brd_tot_p_none_c_c', 'sf_s_glhym_perm_mean_b_none_c_c',
 'sf_s_glhym_por_mean_b_none_c_c', 'swc_o_era5_sm_mean_b_none_d1_m7d', 'top_s_cam_area_tot_b_none_c_c', 'top_s_cam_elev_mean_b_none_c_c', 'top_s_cam_elev_p50_b_none_c_c', 'top_s_cam_elev_max_b_none_c_c', 'top_s_cam_elev_min_b_none_c_c', 'top_s_cam_slo_mean_b_none_c_c', 'top_s_dem_com_none_b_none_c_c',
 'top_s_dem_cc_none_b_none_c_c', 'top_s_dem_ti_none_b_none_c_c', 'veg_o_modis_lai_mean_b_none_d1_m22d', 'veg_o_modis_fpar_mean_b_none_d1_m22d', 'veg_o_modis_ndvi_mean_b_none_d1_m38d', 'veg_o_modis_agr_mean_b_none_d1_m38d', 'veg_o_modis_evi_mean_b_none_d1_m38d', 'veg_o_modis_nbr_mean_b_none_d1_m38d', 'lulc_o_modis_enf_sum_b_none_d1_p0d',
 'lulc_o_modis_ebf_sum_b_none_d1_p0d', 'lulc_o_modis_dnf_sum_b_none_d1_p0d', 'lulc_o_modis_dbf_sum_b_none_d1_p0d', 'lulc_o_modis_mxf_sum_b_none_d1_p0d', 'lulc_o_modis_csh_sum_b_none_d1_p0d', 'lulc_o_modis_osh_sum_b_none_d1_p0d', 'lulc_o_modis_wsv_sum_b_none_d1_p0d', 'lulc_o_modis_svn_sum_b_none_d1_p0d',
 'lulc_o_modis_grs_sum_b_none_d1_p0d', 'lulc_o_modis_pwt_sum_b_none_d1_p0d', 'lulc_o_modis_crp_sum_b_none_d1_p0d', 'lulc_o_modis_urb_sum_b_none_d1_p0d', 'lulc_o_modis_cvm_sum_b_none_d1_p0d', 'lulc_o_modis_snw_sum_b_none_d1_p0d', 'lulc_o_modis_brn_sum_b_none_d1_p0d', 'lulc_o_modis_wat_sum_b_none_d1_p0d',
 'hi_s_cam_sr_tot_b_none_c_c', 'hi_s_cam_gwr_tot_b_none_c_c', 'idx_s_cam_arcr2_tot_b_none_c_c', 'hi_o_gww_rs_tot_b_none_d1_p0d', 'top_s_cam_lon_mean_b_none_c_c', 'top_s_cam_lat_mean_b_none_c_c', 'top_s_cam_lon_none_p_none_c_c', 'top_s_cam_lat_none_p_none_c_c', 'top_s_dga_lon_none_p_none_c_c',
 'top_s_dga_lat_none_p_none_c_c', 'lulc_o_modis_brn_frac_b_none_d1_p0d', 'lulc_o_modis_cvm_frac_b_none_d1_p0d', 'lulc_o_modis_csh_frac_b_none_d1_p0d', 'lulc_o_modis_grs_frac_b_none_d1_p0d', 'lulc_o_modis_urb_frac_b_none_d1_p0d', 'lulc_o_modis_pwt_frac_b_none_d1_p0d', 'lulc_o_modis_crp_frac_b_none_d1_p0d',
 'lulc_o_modis_snw_frac_b_none_d1_p0d', 'lulc_o_modis_wsv_frac_b_none_d1_p0d', 'lulc_o_modis_enf_frac_b_none_d1_p0d', 'lulc_o_modis_ebf_frac_b_none_d1_p0d', 'lulc_o_modis_wat_frac_b_none_d1_p0d', 'lulc_o_modis_svn_frac_b_none_d1_p0d', 'lulc_o_modis_dnf_frac_b_none_d1_p0d', 'lulc_o_modis_dbf_frac_b_none_d1_p0d',
 'lulc_o_modis_osh_frac_b_none_d1_p0d', 'lulc_o_modis_mxf_frac_b_none_d1_p0d', 'caudal_max_p0d', 'caudal_max_p1d', 'caudal_max_p2d', 'caudal_max_p3d', 'caudal_max_p4d', 'caudal_mean_p0d', 'caudal_mean_p1d', 'caudal_mean_p2d', 'caudal_mean_p3d', 'caudal_mean_p4d', 'caudal_mask2_p0d', 'caudal_mask2_p1d', 'caudal_mask2_p2d',
 'caudal_mask2_p3d', 'caudal_mask2_p4d']


'''
print('Número de variables que entran a RF:', len(list_RF))
df1 = HidroCL.getMerge(list_RF)
df1 = df1[df1['date'] >='2001-01-07']
df1.to_csv(f'{dir_path}/RF11.csv', index = False)
print(' ')
print('RF:', df1)
print(len(df1.columns.tolist()))
'''

print(len(list_RF))
df1 = HidroCL.getMerge(list_RF)
fecha_mas_reciente = df1['date'].max()
print(fecha_mas_reciente)
df1 = df1[df1['date'] == fecha_mas_reciente]
df1.to_csv(f'{dir_path}/RF_act.csv', index = False)
print(' ')
print('RF_act:', df1)
print(len(df1.columns.tolist()))

CATCHMENTS = [int(val) for val in  CATCHMENTS.split(',')]

df1 = df1.loc[df1['gauge_id'].isin(CATCHMENTS),:]
df1.to_csv(f'{dir_path}/RF_actF.csv', index = False)