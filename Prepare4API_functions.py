import numpy  as np
import pandas as pd
import pytz
import requests
from   datetime                import date
from   bs4                     import BeautifulSoup
import os
import sys
datapath_toolbox   = os.path.join('W:\\','ESG','DOW_MAQ','MAQ_Archive','loobos_archive','zz_Python')
sys.path.insert(1, datapath_toolbox)
from   Loobos_Toolbox_NewTower import *

def dateparse_AD                  (date,time):
    t_index_          = pd.to_datetime(date + ' ' + time, format='%Y-%m-%d %H:%M:%S')       # TIMESTAMP format
    return t_index_

#---
def dateparse_VK                  (date,time):
    t_index_          = pd.to_datetime(date + ' ' + time,format='%Y-%m-%d %H:%M')
    return t_index_

#---
def dateparse_VK_BC               (date,time):
    t_index_          = pd.to_datetime(date + ' ' + time,format='%d-%b-%y %H:%M')
    return t_index_

#---
def Prepare4API_Loobos_BM         (t1,t2,datapath):

    #--- TIMESTAMP string Changed
    term_date_1          = date(2022, 8,31)
    term_date_2          = date(2022,10, 5)
    if ((t1 > term_date_1) & (t1 <= term_date_2)):
        timestamp_str    = 'Time_Stamp'
    else:
        timestamp_str    = 'TIMESTAMP'
    all_keys             = [timestamp_str, 'SW_IN_1_1_1', 'SW_OUT_1_1_1', 'LW_IN_1_1_1', 'LW_OUT_1_1_1', 'LW_T_BODY_1_1_1', 'PPFD_IN_1_1_1', 'PPFD_OUT_1_1_1', 'TA_1_1_1', 'RH_1_1_1', 'TA_2_1_1', 'TA_2_2_1', 'TA_2_3_1', 'TA_2_4_1', 'TA_2_5_1', 'WS_2_1_1', 'WS_2_2_1', 'WS_2_3_1', 'WS_2_4_1','WS_2_5_1', 'WD_2_1_1', 'WD_2_2_1', 'WD_2_3_1', 'WD_2_4_1', 'WD_2_5_1', 'PA_1_1_1', 'NaN_1_1_1']
    
    #--- Read infile
    bm      = Loobos_Read_NL_Loo_BM(       t1,t2,keys=None, API=True)
    
    #--- UTC
    CET           = pytz.timezone('Etc/GMT-1')
    bm.index = bm.index.tz_localize(CET).tz_convert(pytz.utc)
    
    #--- Define units
    units_dict_bm = {'TIMESTAMP'    : 'yyyy-mm-dd HH:MM:SS UTC',  'SW_IN_1_1_1'     : 'W m-2',      'SW_OUT_1_1_1'  : 'W m-2',           'LW_IN_1_1_1'    : 'W m-2' , 
                     'LW_OUT_1_1_1' : 'W m-2',                    'LW_T_BODY_1_1_1' : '°C',         'PPFD_IN_1_1_1' : 'umol m-2 s-1',    'PPFD_OUT_1_1_1' : 'umol m-2 s-1', 
                     'TA_1_1_1'     : '°C',                       'RH_1_1_1'        : '%',          'TA_2_1_1'      : '°C',              'TA_2_2_1'       : '°C', 
                     'TA_2_3_1'     : '°C',                       'TA_2_4_1'        : '°C',         'TA_2_5_1'      : '°C',              'WS_2_1_1'       : 'm s-1', 
                     'WS_2_2_1'     : 'm s-1',                    'WS_2_3_1'        : 'm s-1',      'WS_2_4_1'      : 'm s-1',           'WS_2_5_1'       : 'm s-1', 
                     'WD_2_1_1'     : 'Deg',                      'WD_2_2_1'        : 'Deg',        'WD_2_3_1'      : 'Deg',             'WD_2_4_1'       : 'Deg', 
                     'WD_2_5_1'     : 'Deg',                      'PA_1_1_1'        : 'kPa',        'NaN_1_1_1'     : '-'}
    
    #--- Write outfile
    filename = os.path.join(datapath,'LB_BM','LB_BM%4d%02d%02d.csv'%(t1.astype(object).year, t1.astype(object).month, t1.astype(object).day))#(daytime.year,daytime.month,daytime.day)
    units  = [units_dict_bm[key] for key in all_keys]
    with open(filename, 'w') as fp:
        fp.write(','.join(all_keys) + '\n')
        fp.write(','.join(units   ) + '\n')
    bm['NaN_1_1_1'] = np.nan        #ESG_SB_20240614+ Fix pressure int() error by adding NaN column
    bm.to_csv(filename,sep=',',header=None,mode='a',na_rep='NaN',date_format='%Y-%m-%d %H:%M:%S')
    if np.isnan(bm.values).all() == True:
        print(filename)
        os.remove(filename)
    return

#---
def Prepare4API_Loobos_BM_Backup         (t1,t2,datapath):

    all_keys             = ['TIMESTAMP', 'SW_IN_2_1_1', 'TA_3_1_1', 'RH_2_1_1', 'P_2_1_1']
    
    #--- Read infile
    bm_backup      = Loobos_Read_NL_Loo_BM_Backup(       t1,t2,keys=None, API=True)
        
    #--- UTC
    CET           = pytz.timezone('Etc/GMT-1')
    bm_backup.index = bm_backup.index.tz_localize(CET).tz_convert(pytz.utc)
    
    #--- Define units
    units_dict_bm_backup = {'TIMESTAMP'    : 'yyyy-mm-dd HH:MM:SS UTC',  'SW_IN_2_1_1'     : 'W m-2',      'TA_3_1_1'  : '°C',           'RH_2_1_1'    : '%',  'P_2_1_1'        : 'mm'}
    
    #--- Write outfile
    filename = os.path.join(datapath,'LB_BM-Backup','LB_BM-Backup%4d%02d%02d.csv'%(t1.astype(object).year, t1.astype(object).month, t1.astype(object).day))#(daytime.year,daytime.month,daytime.day)
    units  = [units_dict_bm_backup[key] for key in all_keys]
    with open(filename, 'w') as fp:
        fp.write(','.join(all_keys) + '\n')
        fp.write(','.join(units   ) + '\n')
    bm_backup.to_csv(filename,sep=',',header=None,mode='a',na_rep='NaN',date_format='%Y-%m-%d %H:%M:%S')
    if np.isnan(bm_backup.values).all() == True:
        print(filename)
        os.remove(filename)
    return



#---
def Prepare4API_Loobos_BM_Precip  (t1,t2,datapath):

    all_keys        = ["TIMESTAMP","P_1_1_1"]
    
    #--- Read infile
    bm_precip         = Loobos_Read_NL_Loo_BM_Precip(  t1,t2,keys=None,API=True)
        
    #--- UTC
    CET               = pytz.timezone('Etc/GMT-1')
    bm_precip.index   = bm_precip.index.tz_localize(CET).tz_convert(pytz.utc)
    
    units_dict_precip =   {'TIMESTAMP'    : 'yyyy-mm-dd HH:MM:SS UTC',    'P_1_1_1': 'mm'}
    
    # Write outfile
    filename = os.path.join(datapath,'LB_BM-Precipitation','LB_PRECIP%4d%02d%02d.csv'%(t1.astype(object).year, t1.astype(object).month, t1.astype(object).day))
    units    = [units_dict_precip[key] for key in all_keys]
    with open(filename, 'w') as fp:
        fp.write(','.join(all_keys) + '\n')
        fp.write(','.join(units   ) + '\n')
    bm_precip.to_csv(filename,sep=',',header=None,mode='a',na_rep='NaN',date_format='%Y-%m-%d %H:%M:%S')
    if np.isnan(bm_precip.values).all() == True:
        print(filename)
        os.remove(filename)
    return

#---
def Prepare4API_Loobos_BM_Soil    (t1,t2,datapath):

    all_keys        = ["TIMESTAMP","TS_1_1_1","TS_1_2_1","TS_1_2_2","TS_1_3_1","TS_1_4_1","TS_1_5_1","TS_1_6_1","T_WTD_1_7_1","TS_2_1_1","TS_2_2_1","TS_2_2_2","TS_2_3_1","TS_2_4_1","TS_2_5_1","TS_2_6_1","T_WTD_2_7_1","TS_3_1_1","TS_3_2_1","TS_3_2_2","TS_4_1_1","TS_4_2_1","TS_4_2_2","SWC_1_1_1","SWC_1_2_1","SWC_1_3_1","SWC_1_4_1","SWC_1_5_1","SWC_2_1_1","SWC_2_2_1","SWC_2_3_1","SWC_2_4_1","SWC_2_5_1","SWC_3_1_1","SWC_4_1_1","G_1_1_1","G_ISCAL_1_1_1","G_2_1_1","G_ISCAL_2_1_1","G_3_1_1","G_ISCAL_3_1_1","G_4_1_1","G_ISCAL_4_1_1","WTD_1_1_1","WTD_2_1_1","SWC_IU_1_1_1","SWC_IU_1_2_1","SWC_IU_1_3_1","SWC_IU_1_4_1","SWC_IU_1_5_1","SWC_IU_2_1_1","SWC_IU_2_2_1","SWC_IU_2_3_1","SWC_IU_2_4_1","SWC_IU_2_5_1","SWC_IU_3_1_1","SWC_IU_4_1_1","G_IU_1_1_1","G_IU_2_1_1","G_IU_3_1_1","G_IU_4_1_1","G_SF_1_1_1","G_SF_2_1_1","G_SF_3_1_1","G_SF_4_1_1","WCP_1_1_1","WCP_2_1_1"]
    
    #--- Read infile
    bm_soil         = Loobos_Read_NL_Loo_BM_Soil(  t1,t2,keys=None,API=True)
        
    #--- UTC
    CET             = pytz.timezone('Etc/GMT-1')
    bm_soil.index   = bm_soil.index.tz_localize(CET).tz_convert(pytz.utc)
    
    units_dict_soil =     {'TIMESTAMP'    : 'yyyy-mm-dd HH:MM:SS UTC',
                           'TS_1_1_1': '°C',              'TS_1_2_1': '°C',          'TS_1_2_2': '°C',          'TS_1_3_1': '°C',       'TS_1_4_1': '°C',              'TS_1_5_1': '°C',          'TS_1_6_1': '°C',          'T_WTD_1_7_1': '°C',          
                           'TS_2_1_1': '°C',              'TS_2_2_1': '°C',          'TS_2_2_2': '°C',          'TS_2_3_1': '°C',       'TS_2_4_1': '°C',              'TS_2_5_1': '°C',          'TS_2_6_1': '°C',          'T_WTD_2_7_1': '°C',          
                           'TS_3_1_1': '°C',              'TS_3_2_1': '°C',          'TS_3_2_2': '°C',          'TS_4_1_1': '°C',       'TS_4_2_1': '°C',              'TS_4_2_2': '°C',          'SWC_1_1_1' : '%',          'SWC_1_2_1': '%',         
                           'SWC_1_3_1': '%',              'SWC_1_4_1': '%',          'SWC_1_5_1': '%',          'SWC_2_1_1': '%',       'SWC_2_2_1': '%',              'SWC_2_3_1': '%',          'SWC_2_4_1': '%',          'SWC_2_5_1': '%',          
                           'SWC_3_1_1': '%',              'SWC_4_1_1': '%',          'G_1_1_1' : 'W m-2',       'G_ISCAL_1_1_1' : '-',  'G_2_1_1' : 'W m-2',           'G_ISCAL_2_1_1' : '-',     'G_3_1_1' : 'W m-2',                                  
                           'G_ISCAL_3_1_1' : '-',         'G_4_1_1' : 'W m-2',       'G_ISCAL_4_1_1' : '-',     'WTD_1_1_1' : 'm',      'WTD_2_1_1' : 'm',             'SWC_IU_1_1_1' : '-',      'SWC_IU_1_2_1' : '-',      'SWC_IU_1_3_1' : '-',      
                           'SWC_IU_1_4_1' : '-',          'SWC_IU_1_5_1' : '-',      'SWC_IU_2_1_1' : '-',      'SWC_IU_2_2_1' : '-',   'SWC_IU_2_3_1' : '-',          'SWC_IU_2_4_1' : '-',      'SWC_IU_2_5_1' : '-',      'SWC_IU_3_1_1' : '-',      
                           'SWC_IU_4_1_1' : '-',          'G_IU_1_1_1' : 'mV',       'G_IU_2_1_1': 'mV',        'G_IU_3_1_1': 'mV',     'G_IU_4_1_1': 'mV',            'G_SF_1_1_1' : 'uV W-1 m2','G_SF_2_1_1': 'uV W-1 m2', 'G_SF_3_1_1': 'uV W-1 m2', 
                           'G_SF_4_1_1': 'uV W-1 m2',     'WCP_1_1_1' : 'Pa',        'WCP_2_1_1' : 'Pa'}
    
    # Write outfile
    filename = os.path.join(datapath,'LB_BM-Soil','LB_SOIL%4d%02d%02d.csv'%(t1.astype(object).year, t1.astype(object).month, t1.astype(object).day))
    units    = [units_dict_soil[key] for key in all_keys]
    with open(filename, 'w') as fp:
        fp.write(','.join(all_keys) + '\n')
        fp.write(','.join(units   ) + '\n')

    #Remove calibration of soil heat fluxes
    bm_soil['G_1_1_1'][bm_soil['G_ISCAL_1_1_1'] == 1.0] = np.nan
    bm_soil['G_2_1_1'][bm_soil['G_ISCAL_2_1_1'] == 1.0] = np.nan
    bm_soil['G_3_1_1'][bm_soil['G_ISCAL_3_1_1'] == 1.0] = np.nan
    bm_soil['G_4_1_1'][bm_soil['G_ISCAL_4_1_1'] == 1.0] = np.nan
            
    bm_soil.to_csv(filename,sep=',',header=None,mode='a',na_rep='NaN',date_format='%Y-%m-%d %H:%M:%S')
    if np.isnan(bm_soil.values).all() == True:
        print(filename)
        os.remove(filename)
    return

#---
def Prepare4API_Loobos_EC         (t1,t2,datapath):
    all_keys = ['filename','date','time','DOY','daytime','file_records','used_records','Tau','qc_Tau','rand_err_Tau','H','qc_H','rand_err_H','LE','qc_LE','rand_err_LE','co2_flux','qc_co2_flux','rand_err_co2_flux','h2o_flux','qc_h2o_flux','rand_err_h2o_flux','ch4_flux','qc_ch4_flux','rand_err_ch4_flux','none_flux','qc_none_flux','rand_err_none_flux','H_strg','LE_strg','co2_strg','h2o_strg','ch4_strg','none_strg','co2_v-adv','h2o_v-adv','ch4_v-adv','none_v-adv','co2_molar_density','co2_mole_fraction','co2_mixing_ratio','co2_time_lag','co2_def_timelag','h2o_molar_density','h2o_mole_fraction','h2o_mixing_ratio','h2o_time_lag','h2o_def_timelag','ch4_molar_density','ch4_mole_fraction','ch4_mixing_ratio','ch4_time_lag','ch4_def_timelag','none_molar_density','none_mole_fraction','none_mixing_ratio','none_time_lag','none_def_timelag','sonic_temperature','air_temperature','air_pressure','air_density','air_heat_capacity','air_molar_volume','ET','water_vapor_density','e','es','specific_humidity','RH','VPD','Tdew','u_unrot','v_unrot','w_unrot','u_rot','v_rot','w_rot','wind_speed','max_wind_speed','wind_dir','yaw','pitch','roll','u*','TKE','L','(z-d)/L','bowen_ratio','T*','model','x_peak','x_offset','x_10%','x_30%','x_50%','x_70%','x_90%','un_Tau','Tau_scf','un_H','H_scf','un_LE','LE_scf','un_co2_flux','co2_scf','un_h2o_flux','h2o_scf','un_ch4_flux','ch4_scf','un_none_flux','un_none_scf','spikes_hf','amplitude_resolution_hf','drop_out_hf','absolute_limits_hf','skewness_kurtosis_hf','skewness_kurtosis_sf','discontinuities_hf','discontinuities_sf','timelag_hf','timelag_sf','attack_angle_hf','non_steady_wind_hf','u_spikes','v_spikes','w_spikes','ts_spikes','co2_spikes','h2o_spikes','ch4_spikes','none_spikes','head_detect_LI-7200','t_out_LI-7200','t_in_LI-7200','aux_in_LI-7200','delta_p_LI-7200','chopper_LI-7200','detector_LI-7200','pll_LI-7200','sync_LI-7200','chopper_LI-7500','detector_LI-7500','pll_LI-7500','sync_LI-7500','not_ready_LI-7700','no_signal_LI-7700','re_unlocked_LI-7700','bad_temp_LI-7700','laser_temp_unregulated_LI-7700','block_temp_unregulated_LI-7700','motor_spinning_LI-7700','pump_on_LI-7700','top_heater_on_LI-7700','bottom_heater_on_LI-7700','calibrating_LI-7700','motor_failure_LI-7700','bad_aux_tc1_LI-7700','bad_aux_tc2_LI-7700','bad_aux_tc3_LI-7700','box_connected_LI-7700','mean_value_RSSI_LI-7200','mean_value_LI-7500','u_var','v_var','w_var','ts_var','co2_var','h2o_var','ch4_var','none_var','w/ts_cov','w/co2_cov','w/h2o_cov','w/ch4_cov','w/none_cov','co2_mean','h2o_mean','co2_mean_a','h2o_mean_a','flowrate_mean','hit_power_mean','hit_vin_mean']
    
    #--- Read infile
    ec      = Loobos_Read_GHG_EddyPro_full(t1,t2,keys=None)
    ec.drop(columns=ec.columns[0], axis= 1 , inplace= True)
    all_keys.remove('filename')
    all_keys[0] = 'TIMESTAMP'
    all_keys.remove('time')
    
    #UTC
    CET           = pytz.timezone('Etc/GMT-1')
    ec.index      = ec.index.tz_localize(CET).tz_convert(pytz.utc)
    
    units_dict_ec = {'TIMESTAMP'            : 'yyyy-mm-dd HH:MM:SS UTC',    
                     'DOY'                  : 'ddd.ddd',                    'daytime'           : '1=daytime',      'file_records'      : '-',              'used_records'         : '-',
                     'Tau'                  : 'kg m-1 s-2',                 'qc_Tau'            : '-',              'rand_err_Tau'      : 'kg m-1 s-2',     'H'                    : 'W m-2',
                     'qc_H'                 : '-',                          'rand_err_H'        : 'W m-2',          'LE'                : 'W m-2',          'qc_LE'                : '-',
                     'rand_err_LE'          : 'W m-2',                      'co2_flux'          : 'µmol s-1 m-2',   'qc_co2_flux'       : '-',              'rand_err_co2_flux'    : 'µmol s-1 m-2',
                     'h2o_flux'             : 'mmol s-1 m-2',               'qc_h2o_flux'       : '-',              'rand_err_h2o_flux' : 'mmol s-1 m-2',   'ch4_flux'             : 'µmol s-1 m-2',               'qc_ch4_flux'       : '-',              'rand_err_ch4_flux' : 'µmol s-1 m-2',   
                     'none_flux'            : 'µmol s-1 m-2',               'qc_none_flux'      : '-',              'rand_err_none_flux': 'µmol s-1 m-2',   'H_strg'               : 'W m-2',                      'LE_strg'           : 'W m-2',          'co2_strg'          : 'µmol s-1 m-2',   
                     'h2o_strg'             : 'mmol s-1 m-2',               'ch4_strg'          : 'µmol s-1 m-2',   'none_strg'         : 'µmol s-1 m-2',   'co2_v-adv'            : 'µmol s-1 m-2',
                     'h2o_v-adv'            : 'mmol s-1 m-2',               'ch4_v-adv'         : 'µmol s-1 m-2',   'none_v-adv'        : 'µmol s-1 m-2',   'co2_molar_density'    : 'mmol m-3',                   'co2_mole_fraction' : 'µmol mol_a-1',
                     'co2_mixing_ratio'     : 'µmol mol_d-1',               'co2_time_lag'      : 's',              'co2_def_timelag'   : '1=default',      'h2o_molar_density'    : 'mmol m-3',                   'h2o_mole_fraction' : 'mmol mol_a-1',                                           
                     'h2o_mixing_ratio'     : 'mmol mol_d-1',               'h2o_time_lag'      : 's',              'h2o_def_timelag'   : '1=default',      'ch4_molar_density'    : 'mmol m-3',                   'ch4_mole_fraction' : 'µmol mol_a-1',                                           
                     'ch4_mixing_ratio'     : 'µmol mol_d-1',               'ch4_time_lag'      : 's',              'ch4_def_timelag'   : '1=default',      'none_molar_density'   : 'mmol m-3',                   'none_mole_fraction': 'µmol mol_a-1',                                           
                     'none_mixing_ratio'    : 'µmol mol_d-1',               'none_time_lag'     : 's',              'none_def_timelag'  : '1=default',      'sonic_temperature'    : 'K',                          'air_temperature'   : 'K',              'air_pressure'      :'Pa',              
                     'air_density'          : 'kg m-3',                     'air_heat_capacity' : 'J kg-1 K-1',                                             'air_molar_volume'     : 'm3 mol-1',                   'ET'                : 'mm hour-1',      'water_vapor_density': 'kg m-3',        
                     'e'                    : 'Pa',                         'es'                : 'Pa',             'specific_humidity' : 'kg kg-1',        'RH'                   : '%',                          'VPD'               : 'Pa',             'Tdew'              : 'K',              
                     'u_unrot'              : 'm s-1',                      'v_unrot'           : 'm s-1',          'w_unrot'           : 'm s-1',          'u_rot'                : 'm s-1',                      'v_rot'             : 'm s-1',          'w_rot'             : 'm s-1',          
                     'wind_speed'           : 'm s-1',                      'max_wind_speed'    : 'm s-1',          'wind_dir'          : 'deg_from_north', 'yaw'                  : 'deg',                        'pitch'             : 'deg',            'roll'              : 'deg',            
                     'u*'                   : 'm s-1',                      'TKE'               : 'm2 s-2',         'L'                 : 'm',              '(z-d)/L'              : '-',                          'bowen_ratio'       : '-',              'T*'                : 'K',              
                     'model'                : '0=KJ/1=KM/2=HS',             'x_peak'            : 'm',              'x_offset'          : 'm',              'x_10%'                : 'm',                          'x_30%'             : 'm',              'x_50%'             : 'm',              
                     'x_70%'                : 'm',                          'x_90%'             : 'm',              'un_Tau'            : 'kg m-1 s-2',     'Tau_scf'              : '-',                          'un_H'              : 'W m-2',          'H_scf'             : '-',              
                     'un_LE'                : 'W m-2',                      'LE_scf'            : '-',              'un_co2_flux'       : 'µmol s-1 m-2',   'co2_scf'              : '-',                          'un_h2o_flux'       : 'mmol s-1 m-2',   'h2o_scf'           : '-',              
                     'un_ch4_flux'          : 'µmol s-1 m-2',               'ch4_scf'           : '-',              'un_none_flux'      : 'µmol s-1 m-2',   'un_none_scf'          : '-',                                   
                     'spikes_hf'            : '8u/v/w/ts/co2/h2o/ch4/none', 'amplitude_resolution_hf'  : '8u/v/w/ts/co2/h2o/ch4/none', 'drop_out_hf'              : '8u/v/w/ts/co2/h2o/ch4/none', 'absolute_limits_hf'       : '8u/v/w/ts/co2/h2o/ch4/none', 'skewness_kurtosis_hf'     : '8u/v/w/ts/co2/h2o/ch4/none', 'skewness_kurtosis_sf'     : '8u/v/w/ts/co2/h2o/ch4/none', 'discontinuities_hf'       : '8u/v/w/ts/co2/h2o/ch4/none', 'discontinuities_sf'       : '8u/v/w/ts/co2/h2o/ch4/none', 'timelag_hf' : '8co2/h2o/ch4/none', 'timelag_sf'               : '8co2/h2o/ch4/none','attack_angle_hf' : '8aa', 'non_steady_wind_hf'       : '8U',
                     'u_spikes'             : '-',                          'v_spikes': '-', 'w_spikes': '-', 'ts_spikes': '-',  'co2_spikes': '-',  'h2o_spikes': '-', 'ch4_spikes': '-', 'none_spikes': '-',
                     'head_detect_LI-7200'  : '#_flagged_recs',             't_out_LI-7200' : '#_flagged_recs',                     't_in_LI-7200' : '#_flagged_recs',              'aux_in_LI-7200' : '#_flagged_recs', 'delta_p_LI-7200' : '#_flagged_recs',           'chopper_LI-7200' : '#_flagged_recs', 'detector_LI-7200' : '#_flagged_recs',          'pll_LI-7200' : '#_flagged_recs', 'sync_LI-7200' : '#_flagged_recs',              'chopper_LI-7500' : '#_flagged_recs', 'detector_LI-7500' : '#_flagged_recs',          'pll_LI-7500' : '#_flagged_recs', 'sync_LI-7500' : '#_flagged_recs',              'not_ready_LI-7700' : '#_flagged_recs', 'no_signal_LI-7700' : '#_flagged_recs',         're_unlocked_LI-7700' : '#_flagged_recs', 'bad_temp_LI-7700' : '#_flagged_recs',          'laser_temp_unregulated_LI-7700' : '#_flagged_recs', 'block_temp_unregulated_LI-7700' : '#_flagged_recs', 'motor_spinning_LI-7700' : '#_flagged_recs',    'pump_on_LI-7700' : '#_flagged_recs', 'top_heater_on_LI-7700' : '#_flagged_recs',     'bottom_heater_on_LI-7700' : '#_flagged_recs', 'calibrating_LI-7700' : '#_flagged_recs',       'motor_failure_LI-7700' : '#_flagged_recs', 'bad_aux_tc1_LI-7700' : '#_flagged_recs',       'bad_aux_tc2_LI-7700' : '#_flagged_recs',  'bad_aux_tc3_LI-7700' : '#_flagged_recs',       'box_connected_LI-7700' : '#_flagged_recs', 'mean_value_RSSI_LI-7200' : '-',                'mean_value_LI-7500' : '-',                      'u_var'        : 'm2 s-2',  'v_var': 'm2 s-2',                     'w_var': 'm2 s-2',                              'ts_var' : 'K2',
                     'co2_var'              : '-',       'h2o_var' : '-',                     'ch4_var' : '-',                                'none_var' : '-',
                     'w/ts_cov'             : 'm s-1 K', 'w/co2_cov'  : '-', 'w/h2o_cov'    : '-',       'w/ch4_cov' : '-', 'w/none_cov'   : '-',       'co2_mean' : '-', 'h2o_mean'     : '-',       'co2_mean_a' : '-', 'h2o_mean_a'   : '-',       'flowrate_mean' : '-', 'hit_power_mean' : '-',     'hit_vin_mean' : '-'}
 
    # Write outfile
    filename = os.path.join(datapath,'LB_EC','LB_EC%4d%02d%02d.csv'%(t1.astype(object).year, t1.astype(object).month, t1.astype(object).day))#(daytime.year,daytime.month,daytime.day))
    units    = [units_dict_ec[key] for key in all_keys]
    with open(filename, 'w') as fp:
        fp.write(','.join(all_keys) + '\n')
        fp.write(','.join(units   ) + '\n')
        
    ec.to_csv(filename,sep=',',header=None,mode='a',na_rep='NaN',date_format='%Y-%m-%d %H:%M:%S')
    return

#---
def Prepare4API_Loobos_ST_Cal     (t1,t2,datapath):
    all_keys      = ["TIMESTAMP","CO2","H2O","LEVEL","FLOW_VOLRATE","T_CELL","PRESS_CELL"]

    #--- Read infile
    st, hdr       = Loobos_Read_NL_Loo_ST_Cal(t1,t2,keys=None, getCalLine=True)

    # --- Storage profile data analysis
    t_st          = st.index
    eq_seconds    = [15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59]
    st_avg        = st.loc[t_st.second.isin(eq_seconds)].resample('30S', label='right').mean()
    cal_date1     = t1
    cal_date2     = t1 + np.timedelta64(12,'h')
    cal_co2_1_a,cal_co2_1_b, cal_co2_2_a, cal_co2_2_b = 1.0,0.0,1.0,0.0
    cal_h2o_a,  cal_h2o_b = 1.0,0.0
    try:
        cal_date1     = np.datetime64(hdr[ 62: 81])
        cal_co2_1_b   =         float(hdr[124:132])
        cal_co2_1_a   =         float(hdr[136:144])
    except:
        print('ST: could not find 1st CO2 calibration coefficients. Using (1,0).')
        pass
    try:
        cal_date2     = np.datetime64(hdr[146:165])
        cal_co2_2_b   =         float(hdr[208:216])
        cal_co2_2_a   =         float(hdr[220:228])
    except:
        print('ST: could not find 2nd CO2 calibration coefficients. Using (1,0).')
        pass
    try:
        i             = hdr.index('H2O_obs')
        cal_h2o_a     =         float(hdr[i+10:i+18])
        cal_h2o_b     =         float(hdr[i+21:i+29])
    except:
        print('ST: could not find H2O calibration coefficients. Using (1,0).')
        pass
    
   #print('cal_date1  : ',cal_date1  )
   #print('cal_co2_1_a: ',cal_co2_1_a) # slope
   #print('cal_co2_1_b: ',cal_co2_1_b) # offset
   #print('cal_date2  : ',cal_date2  )
   #print('cal_co2_2_a: ',cal_co2_2_a) # slope
   #print('cal_co2_2_b: ',cal_co2_2_b) # offset 
   #print('cal_h2o_a  : ',cal_h2o_a  ) # slope 
   #print('cal_h2o_b  : ',cal_h2o_b  ) # offset

    st_avg['CO2_cal_slope' ]        = np.nan
    st_avg['CO2_cal_offset']        = np.nan
    st_avg['H2O_cal_slope' ]        = np.nan
    st_avg['H2O_cal_offset']        = np.nan
    I                               = (st_avg.index  <  cal_date2)
    st_avg.loc[I,'CO2_cal_slope' ]  = cal_co2_1_a
    st_avg.loc[I,'CO2_cal_offset']  = cal_co2_1_b
    I                               = (st_avg.index >=  cal_date2)
    st_avg.loc[I,'CO2_cal_slope' ]  = cal_co2_2_a
    st_avg.loc[I,'CO2_cal_offset']  = cal_co2_2_b
    st_avg    [  'H2O_cal_slope' ]  = cal_h2o_a
    st_avg    [  'H2O_cal_offset']  = cal_h2o_b
    
    st_avg.index.name = 'TIMESTAMP'
    converters = {key: 'int' for key in all_keys if key in ["LEVEL"]}
    st_avg     = st_avg.astype(converters, errors='ignore')

    all_keys      = all_keys + ['CO2_cal_slope','CO2_cal_offset','H2O_cal_slope','H2O_cal_offset']
#   units_dict_st = {"TIMESTAMP":"yyyy-mm-dd HH:MM:SS UTC", "CO2":"\µmol mol-1" ,"H2O":"mmol mol-1" ,"LEVEL":"#" ,"FLOW_VOLRATE":"litres min-1" ,"T_CELL":"°C" ,"PRESS_CELL":"kPa",'CO2_cal_slope':"-",'CO2_cal_offset':"\µmol mol-1", 'H2O_cal_slope':"-",'H2O_cal_offset':"mmol mol-1"}
    units_dict_st = {"TIMESTAMP":"yyyy-mm-dd HH:MM:SS UTC", "CO2":"µmol mol-1" ,"H2O":"mmol mol-1" ,"LEVEL":"#" ,"FLOW_VOLRATE":"litres min-1" ,"T_CELL":"°C" ,"PRESS_CELL":"kPa",'CO2_cal_slope':"-",'CO2_cal_offset':"µmol mol-1", 'H2O_cal_slope':"-",'H2O_cal_offset':"mmol mol-1"}

    #--- UTC
    CET           = pytz.timezone('Etc/GMT-1')
    st_avg.index  = st_avg.index.tz_localize(CET).tz_convert(pytz.utc)
       
    #--- Write outfile
    filename      = os.path.join(datapath,'LB_ST','LB_ST%4d%02d%02d.csv'%(t1.astype(object).year, t1.astype(object).month, t1.astype(object).day))#(daytime.year,daytime.month,daytime.day)
    units         = [units_dict_st[key] for key in all_keys]
    with open(filename, 'w') as fp:
        fp.write(','.join(all_keys) + '\n')
        fp.write(','.join(units   ) + '\n')
    st_avg.to_csv(filename,sep=',',header=None,mode='a',na_rep='NaN',date_format='%Y-%m-%d %H:%M:%S',float_format="%.4f")

    return

#---
def Prepare4API_Veenkampen_Flux   (t1,t2,datapath):
    datapathin = os.path.join ('W:\\','ESG','DOW_MAQ','MAQ_Archive','Veenkampen_archive','veenkampen_data')
    
    units_dict = {'TIMESTAMP'    : 'yyyy-mm-dd HH:MM:SS UTC',
                  'DOY' : 'ddd.ddd',                             'daytime' : '1=daytime',       'file_records' : '-',  'used_records' : '-',   'Tau' : 'kg m-1 s-2',                          'qc_Tau' : '-',                'H' : 'W m-2',                    'qc_H' : '-',                                  'LE' : 'W m-2',          'qc_LE' : '-',                                  'co2_flux' : 'µmol s-1 m-2',   'qc_co2_flux': '-',                                                                         
                  'h2o_flux' : 'mmol s-1 m-2',                   'qc_h2o_flux' : '-',                                                          'H_strg' : 'W m-2',                            'LE_strg' : 'W m-2',          'co2_strg' : 'µmol s-1 m-2',       'h2o_strg' : 'mmol s-1 m-2',                    'co2_v-adv' : 'µmol s-1 m-2',                                                'h2o_v-adv' : 'mmol s-1 m-2',                   'co2_molar_density' : 'mmol m-3',                                          
                  'co2_mole_fraction' : 'µmol mol_a-1',                                                                                        'co2_mixing_ratio' : 'µmol mol_d-1',           'co2_time_lag' : 's',          'co2_def_timelag' : '1=default',  'h2o_molar_density' : 'mmol m-3',              'h2o_mole_fraction' : 'mmol mol_a-1',                                         'h2o_mixing_ratio' : 'mmol mol_d-1',           'h2o_time_lag' : 's',          'h2o_def_timelag' : '1=default',             
                  'sonic_temperature' : 'K',                      'air_temperature' : 'K',        'air_pressure' :'Pa',                        'air_density' : 'kg m-3',                      'air_heat_capacity' : 'J kg-1 K-1',                              'air_molar_volume' : 'm3 mol-1',                'ET'  : 'mm hour-1',           'water_vapor_density' : 'kg m-3',             'e' : 'Pa',                                     'es' : 'Pa',                    'specific_humidity' : 'kg kg-1',           
                  'RH' : '%',                                     'VPD' : 'Pa',                   'Tdew' : 'K',                                'u_unrot' : 'm s-1',                           'v_unrot' : 'm s-1',           'w_unrot' : 'm s-1',              'u_rot' : 'm s-1',                             'v_rot' : 'm s-1',             'w_rot' : 'm s-1',                             'wind_speed' : 'm s-1',                        'max_wind_speed' : 'm s-1',    'wind_dir' : 'deg_from_north',               
                  'yaw' : 'deg',                                  'pitch' : 'deg',                'roll' : 'deg',                              'u*' : 'm s-1',                                'TKE' : 'm2 s-2',               'L' : 'm',                       '(z-d)/L' : '-',                                'bowen_ratio' : '-',            'T*' : 'K',                                  'model' : '0=KJ/1=KM/2=HS',                     'x_peak' : 'm',                 'x_offset' : 'm',                          
                  'x_10%' : 'm',                                  'x_30%' : 'm',                  'x_50%' : 'm',                               'x_70%' : 'm',                                  'x_90%' : 'm',                  'un_Tau' : 'kg m-1 s-2',        'Tau_scf' : '-',                                'un_H' : 'W m-2',              'H_scf' : '-',                                'un_LE' : 'W m-2',                             'LE_scf' : '-',                 'un_co2_flux' : 'µmol s-1 m-2',             
                  'co2_scf' : '-',                                'un_h2o_flux' : 'mmol s-1 m-2', 'h2o_scf' : '-',                             'spikes_hf' : '8u/v/w/ts/co2/h2o/ch4/none',                                                                     'amplitude_resolution_hf' : '8u/v/w/ts/co2/h2o/ch4/none',                                                                    'drop_out_hf': '8u/v/w/ts/co2/h2o/ch4/none',                                                                               
                  'absolute_limits_hf': '8u/v/w/ts/co2/h2o/ch4/none',                                                                          'skewness_kurtosis_hf': '8u/v/w/ts/co2/h2o/ch4/none',                                                           'skewness_kurtosis_sf': '8u/v/w/ts/co2/h2o/ch4/none',                                                                        'discontinuities_hf': '8u/v/w/ts/co2/h2o/ch4/none',                                                                        
                  'discontinuities_sf': '8u/v/w/ts/co2/h2o/ch4/none',                                                                          'timelag_hf': '8co2/h2o/ch4/none',              'timelag_sf': '8co2/h2o/ch4/none','attack_angle_hf' : '8aa',    'non_steady_wind_hf' : '8U',                    'u_spikes' : '-',                 'v_spikes': '-',                           'w_spikes': '-',                                'ts_spikes': '-',                 'co2_spikes': '-',                       
                  'h2o_spikes': '-',                              'chopper_LI-7500' : '#_flagged_recs',                                        'detector_LI-7500' : '#_flagged_recs',          'pll_LI-7500' : '#_flagged_recs',                               'sync_LI-7500' : '#_flagged_recs',              'mean_value_AGC_LI-7500' : '-',                                              'u_var' : 'm2 s-2',                             'v_var': 'm2 s-2',                                                         
                  'w_var': 'm2 s-2',                              'ts_var' : 'K2',                                                             'co2_var' : '-',                                'h2o_var' : '-',                                                'w/ts_cov' : 'm s-1 K',                         'w/co2_cov'  : '-',                                                            'w/h2o_cov'  : '-'}                                                                                                        
    
    all_keys   = ['filename', 'date', 'time', 'DOY', 'daytime', 'file_records', 'used_records', 'Tau', 'qc_Tau', 'H','qc_H', 'LE', 'qc_LE', 'co2_flux', 'qc_co2_flux', 'h2o_flux', 'qc_h2o_flux', 'H_strg', 'LE_strg', 'co2_strg', 'h2o_strg', 'co2_v-adv', 'h2o_v-adv', 'co2_molar_density', 'co2_mole_fraction',
                  'co2_mixing_ratio', 'co2_time_lag',	'co2_def_timelag',	'h2o_molar_density',	'h2o_mole_fraction',	'h2o_mixing_ratio',	'h2o_time_lag',	'h2o_def_timelag',	'sonic_temperature', 'air_temperature', 'air_pressure',	'air_density',	'air_heat_capacity',	'air_molar_volume',	'ET',	'water_vapor_density',
                  'e',	'es',	'specific_humidity',	'RH',	'VPD',	'Tdew',	'u_unrot',	'v_unrot',	'w_unrot',	'u_rot',	'v_rot',	'w_rot',	'wind_speed',	'max_wind_speed', 'wind_dir', 'yaw', 'pitch', 'roll',	'u*',	'TKE',	'L',	'(z-d)/L',	'bowen_ratio',	'T*',	'model',	'x_peak',	'x_offset',	'x_10%',	
                  'x_30%',	'x_50%',	'x_70%',	'x_90%',	'un_Tau',	'Tau_scf',	'un_H',	'H_scf',	'un_LE',	'LE_scf',	'un_co2_flux',	'co2_scf',	'un_h2o_flux',	'h2o_scf', 'spikes_hf',	'amplitude_resolution_hf',	'drop_out_hf',	'absolute_limits_hf',	'skewness_kurtosis_hf',	'skewness_kurtosis_sf',	'discontinuities_hf',	
                  'discontinuities_sf',	'timelag_hf',	'timelag_sf',	'attack_angle_hf',	'non_steady_wind_hf',	'u_spikes',	'v_spikes',	'w_spikes',	'ts_spikes',	'co2_spikes',	'h2o_spikes',	'chopper_LI-7500',	'detector_LI-7500',	'pll_LI-7500',	'sync_LI-7500',	'mean_value_AGC_LI-7500',	'u_var',	'v_var',	'w_var',	'ts_var',	'co2_var',	'h2o_var',	'w/ts_cov',	'w/co2_cov',	'w/h2o_cov']

    if t1 < np.datetime64('today'):
        #url           = 'https://veenkampen.nl/data/%4d/%02d/'%(t1.astype(object).year,t1.astype(object).month)          #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
        #infilename    = 'flux_%4d%02d%02d.txt'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day)    #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
        infilename    = os.path.join(datapathin,'%4d/%02d/flux_%4d%02d%02d.txt')%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).year,t1.astype(object).month,t1.astype(object).day)     #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
    else:
        #url           = 'https://veenkampen.nl/data/'                              #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
        #infilename    = 'flux_current.txt'                                         #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
        infilename    = os.path.join(datapathin,'flux/flux_current.txt')            #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
    print('---->>>>', infilename)

    #vk_flux           = pd.read_csv(os.path.join(url,infilename),sep=',',names=all_keys,date_format='%Y-%m-%d %H:%M',parse_dates=[['date','time']],index_col='date_time',na_values=[-999,-9999])    #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
    vk_flux           = pd.read_csv(infilename,sep=',',names=all_keys,date_format='%Y-%m-%d %H:%M',parse_dates=[['date','time']],index_col='date_time',na_values=[-999,-9999])    #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
    vk_flux.drop(columns=vk_flux.columns[0], axis= 1 , inplace= True) # delete column "filename"
    all_keys          = ['TIMESTAMP',]
    for key in vk_flux.keys(): all_keys.append(key)
    units             = [units_dict[key] for key in all_keys]
    timeIndex         = np.arange(t1,t2,np.timedelta64(30,'m')).astype(np.datetime64)
    timeIndex         = pd.DatetimeIndex(timeIndex) + pd.Timedelta(minutes=30)
    vk_flux = (vk_flux.reset_index().drop_duplicates(subset='date_time', keep='last').set_index('date_time').sort_index())
    vk_flux           = vk_flux.reindex(timeIndex)

    outfilename       = os.path.join(datapath,'VK_FLUX','VK_flux%4s%02d%02d.csv'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day))
    with open(outfilename, 'w') as fp:
        fp.write(','.join(all_keys) + '\n')
        fp.write(','.join(units   ) + '\n')
        
    vk_flux.to_csv(outfilename,sep=',',header=None,mode='a',na_rep='NaN',date_format='%Y-%m-%d %H:%M:%S')
    
    return
    
#---
def Prepare4API_Loobos_LFW       (t1,t2,datapath):
    datapathin = os.path.join ('W:\\','ESG','DOW_MAQ','MAQ_Archive','loobos_archive','NL-Loo-LF-leafwetness')
        
    units_dict = {'TIMESTAMP': 'yyyy-mm-dd HH:MM:SS UTC', 'LWmV1': 'mV', 'LWmV2': 'mV', 'LWmV3': 'mV', 'LWmV4': 'mV', 'LWmV5': 'mV', 'LWmV6': 'mV', 'LWmV7': 'mV', 'LWmV8': 'mV'}    
        
    
    if t1 < np.datetime64('today'):
        all_keys = ['Time Stamp (UTC)','LWmV1','LWmV2','LWmV3','LWmV4','LWmV5','LWmV6','LWmV7','LWmV8']
        'C_%4d%02d%02d.txt'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day)
        datapath_historical = os.path.join(datapathin,'%4d','')%(t1.astype(object).year)
        infilename = 'NL-Loo_LF_%4d%02d%02d_L12_F11.csv'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day)
        leafwetness_data = pd.read_csv(datapath_historical+infilename,sep=',',names=all_keys,skiprows=4,date_format='%y%m%d%H%M%S', index_col='Time Stamp (UTC)',na_values=[np.nan,"NaN"],low_memory=False)
        leafwetness_data.index = pd.to_datetime(leafwetness_data.index, format='%Y%m%d%H%M%S')
        timeIndex         = np.arange(t1+np.timedelta64(1,'m'),t2+np.timedelta64(1,'m'),np.timedelta64(1,'m')).astype(np.datetime64)
        timeIndex         = pd.DatetimeIndex(timeIndex)
        leafwetness_data     = leafwetness_data.reindex(timeIndex)
    elif t1 == np.datetime64('today'):
        all_keys = ['Time Stamp (UTC)','RECORD','LWmV1','LWmV2','LWmV3','LWmV4','LWmV5','LWmV6','LWmV7','LWmV8']
        infilename = os.path.join(datapathin,'raw','NL-Loo_BM_leafwetness.txt')
        leafwetness_data = pd.read_csv(infilename,sep=',',names=all_keys,skiprows=4,date_format='%y-%m-%d %H:%M:%S', index_col='Time Stamp (UTC)',na_values=[np.nan,"NaN"],low_memory=False)
        leafwetness_data.drop(columns=leafwetness_data.columns[0], axis= 1 , inplace= True)               # delete columns
        all_keys.remove('RECORD')
    else: return
           
    print('---->>>>', infilename + '    ' + str(t1))
                         
    all_keys = ['LWmV1','LWmV2','LWmV3','LWmV4','LWmV5','LWmV6','LWmV7','LWmV8']
    all_keys = ['TIMESTAMP',]

    #--- Write outfile
    for key in leafwetness_data.keys(): all_keys.append(key)
    units  = [units_dict[key] for key in all_keys]
    
    outfilename    = 'LB_LFW%4d%02d%02d.csv'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day)
    outfilename    = os.path.join(datapath,'LB_LFW',outfilename)
    with open(outfilename, 'w') as fp:
        fp.write(','.join(all_keys) + '\n')
        fp.write(','.join(units   ) + '\n')
    leafwetness_data.to_csv(outfilename,sep=',',header=None,mode='a',na_rep='NaN',date_format='%Y-%m-%d %H:%M:%S')
    if np.isnan(leafwetness_data.values).all() == True:
        print(outfilename)
        os.remove(outfilename)

    return
    


#---
def Prepare4API_Veenkampen_Meteo  (t1,t2,datapath):
    datapathin = os.path.join ('W:\\','ESG','DOW_MAQ','MAQ_Archive','Veenkampen_archive','veenkampen_data')
    
    units_dict = {'TIMESTAMP'    : 'yyyy-mm-dd HH:MM:SS UTC',
                  'TA_2_1_1'     : '°C',      'TW_2_1_1'     : '°C',      'TA_1_1_1'     : '°C',      'TW_1_1_1'     : '°C',            'TA_1_2_1'     : '°C',      'TA_1_1_2'     : '°C',      'RH_1_1_1'     : '%',       'SW_IN_1_1_1'  : 'W m-2',         
                  'SW_OUT_1_1_1' : 'W m-2',    'LW_IN_1_1_1'  : 'W m-2',    'LW_OUT_1_1_1' : 'W m-2',    'RN_1_1_1'     : 'W m-2',      'SW_DIF_1_1_1' : 'W m-2',    'SW_DIR_1_1_1' : 'W m-2',    'LW_IN_2_1_1'  : 'W m-2',    'SW_DUR_1_1_1' : 'second',     
                  'VIS_1_1_1'    : 'm',       'P_1_1_7'      : 'mm',      'D_SNOW'       : 'cm',      'PA_1_1_1'     : 'kPa',            'WS_2_1_1'     : 'm s-1',     'WX_2_1_1'     : 'm s-1',     'WS_1_1_1'     : 'm s-1',     'WX_1_1_1'     : 'm s-1',      
                  'WD_1_1_1'     : 'degrees', 'WS_1_2_1'     : 'm s-1',     'WX_1_2_1'     : 'm s-1',     'WD_1_2_1'     : 'degrees',   'WTD_1_1_1'    : 'm',       'TS_1_1_1'     : '°C',      'TS_1_2_1'     : '°C',      'TS_1_3_1'     : '°C',            
                  'TS_1_4_1'     : '°C',      'TS_1_5_1'     : '°C',      'TS_1_6_1'     : '°C',      'TS_2_1_1'     : '°C',            'TS_2_2_1'     : '°C',      'TS_2_3_1'     : '°C',      'TS_2_4_1'     : '°C',      'G_1_1_1'      : 'W m-2',         
                  'G_2_1_1'      : 'W m-2',    'G_3_1_1'      : 'W m-2',    'G_4_1_1'      : 'W m-2',    'VWC_1_1_1'    : 'm3 m-3',          'VWC_1_2_1'    : 'm3 m-3',       'VWC_1_3_1'    : 'm3 m-3',       'VWC_1_4_1'    : 'm3 m-3',       'VWC_2_1_1'    : 'm3 m-3',             
                  'VWC_2_2_1'    : 'm3 m-3',       'VWC_2_3_1'    : 'm3 m-3',       'VWC_2_4_1'    : 'm3 m-3',       'VWC_3_1_1'    : 'm3 m-3','empty':'-', 'dummy'        :'-',        'P_1_1_2'      : 'mm',      'P_1_1_3'      : 'mm',      'P_1_1_1'      : 'mm',            
                  'P_1_1_4'      : 'mm',      'P_1_1_5'      : 'mm',      'P_1_1_6'      : 'mm'}                                        
    
    all_keys = ['Date','Time','Temp vent dry','Temp vent wet','Temp unvent dry','Temp unvent wet','T+10cm shielded','T Vaisala','Humidity','Q Glb      in',      'Q Glb   out','Q Long   in','Q Long out','Qnet','Q diffuse tracker',
                'Q beam tracker','Qlongwav tracker','Sunshine','Visibilty','Precipitation','Raw Grass/ Snow height','Pressure',       'Wind speed cup 10m mean','Wind speed cup 10m max','Wind speed sonic 10m mean','Wind speed sonic 10m max','Direction sonic 10m',
                'Wind speed sonic 2m mean','Wind speed sonic 2m max', 'Direction sonic 2m','Groundwater level','Temp grass  5cm','Temp grass  10cm','Temp grass  20cm','Temp grass  50cm',
                'Temp grass  100cm','Temp grass  150cm', 'Temp bare   5cm','Temp bare  10cm','Temp bare  20cm','Temp bare  50cm',   'Heat flux grass   a 6cm','Heat flux grass   b 6cm','Heat flux grass  c  6cm','Heat flux bare   6cm',
                'WVC a 65mm','WVC a 125mm','WVC a 250mm','WVC a 500mm','WVC b 65mm','WVC b 125mm','WVC b 250mm','WVC b 500mm','WVC bare 65mm','empty', 'dummy',   'Precip (1)','Precip (2)','Precip (3)','Precip (4)','Precip (5)','Precip (6)']
    #           '"realtime if>0.1 mm/min.For rain warning only, not for daily totals."','"realtime if> 0.03mm/h, otherwise 1h output delay"','"5 min output delay, accurate."',
    #            'no loss of data if disrupted data communication. Stores totals','"bucket content, non filtered. Not usefull for rain."','"for bucket content and evaporation, not for rain!"' )


    if t1 < np.datetime64('today'):
        #url         = 'https://veenkampen.nl/data/%4d/%02d/'%(t1.astype(object).year,t1.astype(object).month)          #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
        #infilename  = 'C_%4d%02d%02d.txt'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day)    #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
        infilename    = os.path.join(datapathin,'%4d/%02d/C_%4d%02d%02d.txt')%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).year,t1.astype(object).month,t1.astype(object).day)     #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
    else:
        #url         = 'https://veenkampen.nl/data/'                            #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
        #infilename  = 'C_current.txt'                                         #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
        infilename    = os.path.join(datapathin,'C_current.txt')            #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
        
    #infilename      = os.path.join(url,infilename)
    print('---->>>>', infilename)

    #vk_meteo = pd.read_csv(os.path.join(url,infilename),sep=',',names=all_keys,date_format='%Y-%m-%d %H:%M',parse_dates=[['Date','Time']],index_col='Date_Time',na_values=[-999,-9999])  #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
    vk_meteo = pd.read_csv(infilename,sep=',',names=all_keys,date_format='%Y-%m-%d %H:%M',parse_dates=[['Date','Time']],index_col='Date_Time',na_values=[-999,-9999])     #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
    vk_meteo.drop(['empty', 'dummy'], axis= 1 , inplace= True)               # delite column "?","??"
    
    # Rename columns
    vk_meteo = vk_meteo.rename(columns={  'Date'                     : 'TIMESTAMP',
                                          'Temp vent dry'            : 'TA_2_1_1',     'Temp vent wet'            : 'TW_2_1_1',     'Temp unvent dry'          : 'TA_1_1_1',     'Temp unvent wet'          : 'TW_1_1_1',                                               'T+10cm shielded'          : 'TA_1_2_1',     'T Vaisala'                : 'TA_1_1_2',     'Humidity'                 : 'RH_1_1_1',     'Q Glb      in'            : 'SW_IN_1_1_1',  
                                          'Q Glb   out'              : 'SW_OUT_1_1_1', 'Q Long   in'              : 'LW_IN_1_1_1',  'Q Long out'               : 'LW_OUT_1_1_1', 'Qnet'                     : 'RN_1_1_1',                                               'Q diffuse tracker'        : 'SW_DIF_1_1_1', 'Q beam tracker'           : 'SW_DIR_1_1_1', 'Qlongwav tracker'         : 'LW_IN_2_1_1',  'Sunshine'                 : 'SW_DUR_1_1_1', 
                                          'Visibilty'                : 'VIS_1_1_1',    'Precipitation'            : 'P_1_1_7',      'Raw Grass/ Snow height'   : 'D_SNOW',       'Pressure'                 : 'PA_1_1_1',                                               'Wind speed cup 10m mean'  : 'WS_2_1_1',     'Wind speed cup 10m max'   : 'WX_2_1_1',     'Wind speed sonic 10m mean': 'WS_1_1_1',     'Wind speed sonic 10m max' : 'WX_1_1_1',     
                                          'Direction sonic 10m'      : 'WD_1_1_1',     'Wind speed sonic 2m mean' : 'WS_1_2_1',     'Wind speed sonic 2m max'  : 'WX_1_2_1',     'Direction sonic 2m'       : 'WD_1_2_1',                                               'Groundwater level'        : 'WTD_1_1_1',    'Temp grass  5cm'          : 'TS_1_1_1',     'Temp grass  10cm'         : 'TS_1_2_1',     'Temp grass  20cm'         : 'TS_1_3_1',     
                                          'Temp grass  50cm'         : 'TS_1_4_1',     'Temp grass  100cm'        : 'TS_1_5_1',     'Temp grass  150cm'        : 'TS_1_6_1',     'Temp bare   5cm'          : 'TS_2_1_1',                                               'Temp bare  10cm'          : 'TS_2_2_1',     'Temp bare  20cm'          : 'TS_2_3_1',     'Temp bare  50cm'          : 'TS_2_4_1',     'Heat flux grass   a 6cm'  : 'G_1_1_1',      
                                          'Heat flux grass   b 6cm'  : 'G_2_1_1',      'Heat flux grass  c  6cm'  : 'G_3_1_1',      'Heat flux bare   6cm'     : 'G_4_1_1',                                                                                             'WVC a 65mm'               : 'VWC_1_1_1',    'WVC a 125mm'              : 'VWC_1_2_1',    'WVC a 250mm'              : 'VWC_1_3_1',    'WVC a 500mm'              : 'VWC_1_4_1',    
                                          'WVC b 65mm'               : 'VWC_2_1_1',    'WVC b 125mm'              : 'VWC_2_2_1',    'WVC b 250mm'              : 'VWC_2_3_1',    'WVC b 500mm'              : 'VWC_2_4_1',                                              'WVC bare 65mm'            : 'VWC_3_1_1',    
                                          'Precip (1)'               : 'P_1_1_2',      # 'realtime if>0.1 mm/min.For rain warning only, not for daily totals.'     
                                          'Precip (2)'               : 'P_1_1_3',      # 'realtime if> 0.03mm/h, otherwise 1h output delay'                        
                                          'Precip (3)'               : 'P_1_1_1',      # '5 min output delay, accurate.'                                           
                                          'Precip (4)'               : 'P_1_1_4',      # 'no loss of data if disrupted data communication. Stores totals'          
                                          'Precip (5)'               : 'P_1_1_5',      # 'bucket content, non filtered. Not usefull for rain.'                     
                                          'Precip (6)'               : 'P_1_1_6'})     # 'for bucket content and evaporation, not for rain!'                       
    all_keys = ['TIMESTAMP',]
    for key in vk_meteo.keys(): all_keys.append(key)
    units  = [units_dict[key] for key in all_keys]
    timeIndex         = np.arange(t1+np.timedelta64(1,'m'),t2+np.timedelta64(1,'m'),np.timedelta64(1,'m')).astype(np.datetime64)
    timeIndex         = pd.DatetimeIndex(timeIndex)
    vk_meteo          = vk_meteo.drop_duplicates(ignore_index=False)
    vk_meteo          = vk_meteo.reindex(timeIndex)

    outfilename = 'VK_meteo%4s%02d%02d.csv'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day)
    outfilename = os.path.join(datapath,'VK_METEO',outfilename)
    with open(outfilename, 'w') as fp:
        fp.write(','.join(all_keys) + '\n')
        fp.write(','.join(units   ) + '\n')
    vk_meteo.to_csv(outfilename,sep=',',header=None,mode='a',na_rep='NaN',date_format='%Y-%m-%d %H:%M:%S')

    return

#---
def Prepare4API_Veenkampen_BC     (t1,t2,datapath):
    datapathin = os.path.join ('W:\\','ESG','DOW_MAQ','MAQ_Archive','Veenkampen_archive','veenkampen_data')
    
    units_dict = {'TIMESTAMP':  'yyyy-mm-dd HH:MM:SS UTC',  'BC' : 'ng m-3',   'BC_UV' : 'ng m-3', 	'F' : 'l min-1',	
                   'SZ_BC' : 'V', 	'SB_BC' : 'V', 	        'RZ_BC' : 'V',	  'RB_BC' : 'V',        'F_BY_BC' : '-',   	
                   'ANT_BC' : '-',  'SZ_UV' : 'V',	        'SB_UV' : 'V', 	  'RZ_UV' : 'V' ,    	'RB_UV' : 'V',	    
                   'F_BY_UV' : '-', 'ANT_UV' : '-'}
    
    all_keys       = ['code','date','time','BC','BC_UV', 'F','SZ_BC','SB_BC','RZ_BC', 'RB_BC','F_BY_BC','ANT_BC','SZ_UV', 'SB_UV','RZ_UV','RB_UV','F_BY_UV','ANT_UV']
    
    
    if t1 < np.datetime64('today'):
        #url        = 'https://veenkampen.nl/data/%4d/%02d/'%(t1.astype(object).year,t1.astype(object).month)          #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
        #infilename = 'BC_%4d%02d%02d.txt'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day)    #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
        infilename    = os.path.join(datapathin,'%4d/%02d/BC_%4d%02d%02d.txt')%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).year,t1.astype(object).month,t1.astype(object).day)     #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
    else:
        #url         = 'https://veenkampen.nl/data/'                            #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
        #infilename  = 'BC_current.txt'                                         #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
        infilename    = os.path.join(datapathin,'BC_current.txt')            #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
    
#    vk_bc          = pd.read_csv(os.path.join(url,infilename),sep=',',names=all_keys,date_format='%d-%b-%y %H:%M', parse_dates=[['date','time']], index_col='date_time',na_values=[-999,-9999])   #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
    vk_bc          = pd.read_csv(infilename,sep=',',names=all_keys,date_format='%d-%b-%y %H:%M', parse_dates=[['date','time']], index_col='date_time',na_values=[-999,-9999])    #ESG_SB_20243004+ Changed data processing from veenkampen.nl to W:\ drive
    vk_bc.drop(columns=vk_bc.columns[0], axis= 1 , inplace= True)
        
    all_keys       = ['TIMESTAMP',]
    for key in vk_bc.keys(): all_keys.append(key)
    units          = [units_dict[key] for key in all_keys]
    timeIndex      = np.arange(t1,t2,np.timedelta64(5,'m')).astype(np.datetime64)
    timeIndex      = np.arange(vk_bc.index[0],vk_bc.index[0]+np.timedelta64(1,'D'),np.timedelta64(5,'m')).astype(np.datetime64)
    timeIndex      = pd.DatetimeIndex(timeIndex)
    vk_bc          = vk_bc.drop_duplicates()
    vk_bc          = vk_bc.reindex(timeIndex)
    
    outfilename    = 'VK_BC%4d%02d%02d.csv'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day)
    outfilename    = os.path.join(datapath,'VK_BC',outfilename)
    with open(outfilename, 'w') as fp:
        fp.write(','.join(all_keys) + '\n')
        fp.write(','.join(units   ) + '\n')
    vk_bc.to_csv(outfilename,sep=',',header=None,mode='a',na_rep='NaN',date_format='%Y-%m-%d %H:%M:%S')
    
    return
    
#---
def Prepare4API_Veenkampen_PM       (t1,t2,datapath):
    units_dict = {'TIMESTAMP': 'yyyy-mm-dd HH:MM:SS UTC', 'SDS_P1': 'µg m-3', 'SDS_P2': 'µg m-3'}
    
    all_keys = ['Time','durP1','ratioP1','P1','durP2','ratioP2','P2','SDS_P1','SDS_P2','PMS_P1','PMS_P2','Temp','Humidity',
                'BMP_temperature','BMP_pressure','BME280_temperature','BME280_humidity','BME280_pressure','Samples',
                'Min_cycle','Max_cycle','Signal','HPM_P1','HPM_P2']
        
    infilename = 'http://api-rrd.madavi.de/data_csv/csv-files/'+str(t1)+'/data-esp8266-13491152-'+str(t1)+'.csv'
    
    print('---->>>>', infilename)
        
    vk_pm = pd.read_csv(os.path.join(infilename),sep=';',skiprows=1,names=all_keys,date_format='%y/%m/%b %H:%M:%S', index_col='Time',na_values=[])
    vk_pm.index = pd.to_datetime(vk_pm.index, errors='coerce')
    vk_pm.drop(columns=vk_pm.columns[0:6], axis= 1 , inplace= True)
    vk_pm.drop(columns=vk_pm.columns[2:], axis= 1 , inplace= True)
    timeIndex         = np.arange(t1,t2,np.timedelta64(5,'m')).astype(np.datetime64)
    timeIndex         = pd.DatetimeIndex(timeIndex)
    vk_pm             = vk_pm.drop_duplicates()
    vk_pm             = vk_pm.reindex(timeIndex,method='bfill')
    
    all_keys = ['SDS_P1','SDS_P2']
    all_keys = ['TIMESTAMP',]

    #--- Write outfile
    for key in vk_pm.keys(): all_keys.append(key)
    units  = [units_dict[key] for key in all_keys]

    vk_pm = vk_pm.rename(columns={"SDS_P1": "PM10", "SDS_P2": "PM2.5"})
    all_keys_write = ['TIMESTAMP','PM10','PM2.5']
    
    outfilename    = 'VK_PM%4d%02d%02d.csv'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day)
    outfilename    = os.path.join(datapath,'VK_PM',outfilename)
    with open(outfilename, 'w') as fp:
        fp.write(','.join(all_keys_write) + '\n')
        fp.write(','.join(units   ) + '\n')
    vk_pm.to_csv(outfilename,sep=',',header=None,mode='a',na_rep='NaN',date_format='%Y-%m-%d %H:%M:%S')

    return
    
#---
def Prepare4API_Veenkampen_Teledyne       (t1,t2,datapath):
    datapathin = os.path.join ('W:\\','ESG','DOW_MAQ','MAQ_Archive','Veenkampen_archive','Teledyne')
        
    units_dict = {'TIMESTAMP': 'yyyy-mm-dd HH:MM:SS UTC', 'NOx': 'ppb', 'NO': 'ppb', 'NO2': 'ppb', 'O3': 'ppb'}    
        
    if t1 < date(2023,4,20):
        all_keys = ['Time Stamp (CET)', 'NOx', 'NO', 'NO2', 'Stabil', 'O3','NO (mug/m3)','NO2 (mug/m3)','O3 (mug/m3)']
        infilename = os.path.join(datapathin,'NOX_O3_log_preUTC.txt')
        teledyne_data = pd.read_csv(infilename,sep=',',names=all_keys,skiprows=1,date_format='%m/%d/%y %H:%M', index_col='Time Stamp (CET)',na_values=[-999,-9999],low_memory=False)
    else:
        all_keys = ['Time Stamp (UTC)', 'NOx', 'NO', 'NO2', 'Stabil', 'O3','NO (mug/m3)','NO2 (mug/m3)','O3 (mug/m3)']
        infilename = os.path.join(datapathin,'NOX_O3_log.txt')
        teledyne_data = pd.read_csv(infilename,sep=',',names=all_keys,skiprows=1,date_format='%m/%d/%y %H:%M', index_col='Time Stamp (UTC)',na_values=[-999,-9999],low_memory=False)
    teledyne_data.drop(columns=teledyne_data.columns[5:], axis= 1 , inplace= True)               # delete columns
    teledyne_data.drop(columns=teledyne_data.columns[3], axis= 1 , inplace= True)               # delete column "Stabil"
    all_keys.remove('NO (mug/m3)')
    all_keys.remove('NO2 (mug/m3)')
    all_keys.remove('O3 (mug/m3)')
    all_keys.remove('Stabil')
    all_keys = ['TIMESTAMP',]
    
    print('---->>>>', infilename + '    ' + str(t1))
    
    teledyne_data.index = pd.to_datetime(teledyne_data.index, errors='coerce')
    timeIndex = np.arange(t1, t2, np.timedelta64(1, 'm')).astype(np.datetime64)
    timeIndex = pd.DatetimeIndex(timeIndex)

    teledyne_data.index = pd.to_datetime(teledyne_data.index, errors='coerce')
    date_range = pd.date_range(start=teledyne_data.index.min().date(), end=teledyne_data.index.max().date(), freq='D')
    timeIndex = np.arange(t1,t2,np.timedelta64(1,'m')).astype(np.datetime64)
    timeIndex         = pd.DatetimeIndex(timeIndex)
    teledyne_data_resampled = teledyne_data.resample('T').mean()
    teledyne_data_resampled_reindexed = teledyne_data_resampled.reindex(timeIndex)
    teledyne_data = teledyne_data_resampled_reindexed
    
#    teledyne_data.index = pd.to_datetime(teledyne_data.index, errors='coerce')
#    timeIndex         = np.arange(t1,t2,np.timedelta64(1,'m')).astype(np.datetime64)
#    timeIndex         = pd.DatetimeIndex(timeIndex)
#    teledyne_data     = teledyne_data.drop_duplicates()
#    teledyne_data = teledyne_data.resample('T').mean()
#    teledyne_data     = teledyne_data.reindex(timeIndex)                #ESG_SB_20240328+ Check what goes wrong here!
#    if t1 < date(2023,4,20):
#        teledyne_data.index = teledyne_data.index.shift(-1, freq='H')
    
    all_keys = ['NOx','NO','NO2','O3']
    all_keys = ['TIMESTAMP',]
    
    #--- Write outfile
    for key in teledyne_data.keys(): all_keys.append(key)
    units  = [units_dict[key] for key in all_keys]
        
    outfilename    = 'VK_Teledyne%4d%02d%02d.csv'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day)
    outfilename    = os.path.join(datapath,'VK_Teledyne',outfilename)
    with open(outfilename, 'w') as fp:
        fp.write(','.join(all_keys) + '\n')
        fp.write(','.join(units   ) + '\n')
    teledyne_data.to_csv(outfilename,sep=',',header=None,mode='a',na_rep='NaN',date_format='%Y-%m-%d %H:%M:%S')
    if np.isnan(teledyne_data.values).all() == True:
        print(outfilename)
        os.remove(outfilename)

    return

#---
def Prepare4API_Veenkampen_BAM       (t1,t2,datapath):
    datapathin = os.path.join ('W:\\','ESG','DOW_MAQ','MAQ_Archive','Veenkampen_archive','PMsensor','BAM')
    
    units_dict = {'TIMESTAMP': 'yyyy-mm-dd HH:MM:SS UTC', 'BAM_Conc': 'mg m-3', 'BAM_Qtot': 'm3',
                  'BAM_WS': 'm s-1', 'BAM_RH': '%', 'BAM_Delta': '°C',\
                  'BAM_AT': '°C', 'BAM_E': '-', 'BAM_U': '-',
                  'BAM_M': '-', 'BAM_I': '-', 'BAM_L': '-',
                  'BAM_R': '-', 'BAM_N': '-', 'BAM_F': '-',
                  'BAM_P': '-', 'BAM_D': '-', 'BAM_C': '-', 'BAM_T': '-'}    
    
    all_keys = ['Time','Conc (mg/m3)','Qtot (m3)','no (V)',
                'WS (MPS)','no2 (V)','RH (%)','Delta (C)','AT (C)',
                'Stab (ug)','Ref (ug)','E','U','M','I','L','R',
                'N','F','P','D','C','T']
        
    if t1 < np.datetime64('today'):
        infilename    = os.path.join(datapathin,'Dayfiles/%4d/%02d/BAM_%4d%02d%02d.txt')%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).year,t1.astype(object).month,t1.astype(object).day)
    else:
        infilename    = os.path.join(datapathin,'BAM_log.txt')
    
    print('---->>>>', infilename)
        
    vk_bam = pd.read_csv(os.path.join(infilename),sep=',',skiprows=1,names=all_keys,date_format='%Y-%m-%d %H:%M', index_col='Time',na_values=[])
    vk_bam.index = pd.to_datetime(vk_bam.index, errors='coerce')
    vk_bam.drop(columns=vk_bam.columns[[2,4,8,9]], axis= 1 , inplace= True)
    vk_bam = vk_bam.rename(columns={"Conc (mg/m3)": "BAM_Conc", "Qtot (m3)": "BAM_Qtot",
                           "WS (MPS)": "BAM_WS", "RH (%)": "BAM_RH",
                           "Delta (C)": "BAM_Delta", "AT (C)": "BAM_AT",
                           "E": "BAM_E", "U": "BAM_U",
                           "M": "BAM_M", "I": "BAM_I",
                           "L": "BAM_L", "R": "BAM_R",
                           "N": "BAM_N", "F": "BAM_F",
                           "P": "BAM_P", "D": "BAM_D",
                           "C": "BAM_C", "T": "BAM_T",})
    all_keys       = ['TIMESTAMP',]
    for key in vk_bam.keys(): all_keys.append(key)
    units  = [units_dict[key] for key in all_keys]
    timeIndex         = np.arange(t1,t2,np.timedelta64(1,'h')).astype(np.datetime64)
    timeIndex         = pd.DatetimeIndex(timeIndex)
    vk_bam             = vk_bam.drop_duplicates()
    if t1 == np.datetime64('today'):
        vk_bam             = vk_bam.reindex(timeIndex,method='bfill')
            
    outfilename    = 'VK_BAM%4d%02d%02d.csv'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day)
    outfilename    = os.path.join(datapath,'VK_BAM',outfilename)
    with open(outfilename, 'w') as fp:
        fp.write(','.join(all_keys) + '\n')
        fp.write(','.join(units   ) + '\n')
    vk_bam.to_csv(outfilename,sep=',',header=None,mode='a',na_rep='NaN',date_format='%Y-%m-%d %H:%M:%S')
    
    return
    
#---
def Prepare4API_Amsterdam_Flux    (t1,t2,datapath):
    datapathin = os.path.join('W:\\','ESG','DOW_MAQ','MAQ_Archive','AAMS_archive','AAMS_data')

    # units dictionary
    units_dict_flux = {'TIMESTAMP'    : 'yyyy-mm-dd HH:MM:SS UTC',
                  'DOY' : 'ddd.ddd',                             'daytime' : '1=daytime',       'file_records' : '-',         'used_records' : '-',  'Tau' : 'kg m-1 s-2',                          'qc_Tau' : '-',                'H' : 'W m-2',                                      
                  'qc_H' : '-',                                  'LE' : 'W m-2',                'qc_LE' : '-',                                       'co2_flux' : 'µmol s-1 m-2',                   'qc_co2_flux': '-',                                                                
                  'h2o_flux' : 'mmol s-1 m-2',                   'qc_h2o_flux' : '-',                                                                'ch4_flux' : 'µmol s-1 m-2',                   'qc_ch4_flux' : '-',                                                               
                  'H_strg' : 'W m-2',                            'LE_strg' : 'W m-2',           'co2_strg' : 'µmol s-1 m-2',                         'h2o_strg' : 'mmol s-1 m-2',                   'ch4_strg' : 'µmol s-1 m-2',   'co2_v-adv' : 'µmol s-1 m-2',                       
                  'h2o_v-adv' : 'mmol s-1 m-2',                  'ch4_v-adv' : 'µmol s-1 m-2',  'co2_molar_density' : 'mmol m-3',                    'co2_mole_fraction' : 'µmol mol_a-1',                                                                                             
                  'co2_mixing_ratio' : 'µmol mol_d-1',           'co2_time_lag' : 's',              'co2_def_timelag' : '1=default',                 'h2o_molar_density' : 'mmol m-3',              'h2o_mole_fraction' : 'mmol mol_a-1',                                              
                  'h2o_mixing_ratio' : 'mmol mol_d-1',           'h2o_time_lag' : 's',              'h2o_def_timelag' : '1=default',                 'ch4_molar_density' : 'mmol m-3',              'ch4_mole_fraction' : 'µmol mol_a-1',                                              
                  'ch4_mixing_ratio'  : 'µmol mol_d-1',          'ch4_time_lag' : 's',              'ch4_def_timelag' : '1=default',                 'sonic_temperature' : 'K',                     'air_temperature' : 'K',        'air_pressure' :'Pa',                              
                  'air_density' : 'kg m-3',                      'air_heat_capacity' : 'J kg-1 K-1',                                                 'air_molar_volume' : 'm3 mol-1',               'ET'  : 'mm hour-1',           'water_vapor_density' : 'kg m-3',                   
                  'e' : 'Pa',                                    'es' : 'Pa',                    'specific_humidity' : 'kg kg-1',                    'RH' : '%',                                    'VPD' : 'Pa',                   'Tdew' : 'K',                                      
                  'u_unrot' : 'm s-1',                           'v_unrot' : 'm s-1',           'w_unrot' : 'm s-1',                                 'u_rot' : 'm s-1',                             'v_rot' : 'm s-1',             'w_rot' : 'm s-1',                                  
                  'wind_speed' : 'm s-1',                        'max_wind_speed' : 'm s-1',    'wind_dir' : 'deg_from_north',                       'yaw' : 'deg',                                 'pitch' : 'deg',                'roll' : 'deg',                                    
                  'u*' : 'm s-1',                                'TKE' : 'm2 s-2',               'L' : 'm',                                          '(z-d)/L' : '-',                               'bowen_ratio' : '-',            'T*' : 'K',                                        
                  'model' : '0=KJ/1=KM/2=HS',                    'x_peak' : 'm',                 'x_offset' : 'm',                                   'x_10%' : 'm',                                 'x_30%' : 'm',                  'x_50%' : 'm',                                     
                  'x_70%' : 'm',                                 'x_90%' : 'm',                  'un_Tau' : 'kg m-1 s-2',                            'Tau_scf' : '-',                               'un_H' : 'W m-2',              'H_scf' : '-',                                      
                  'un_LE' : 'W m-2',                             'LE_scf' : '-',                 'un_co2_flux' : 'µmol s-1 m-2',                     'co2_scf' : '-',                               'un_h2o_flux' : 'mmol s-1 m-2', 'h2o_scf' : '-',                                   
                  'un_ch4_flux' : 'µmol s-1 m-2',                'ch4_scf' : '-',                
                  'spikes_hf' : '8u/v/w/ts/co2/h2o/ch4/none','amplitude_resolution_hf' : '8u/v/w/ts/co2/h2o/ch4/none','drop_out_hf': '8u/v/w/ts/co2/h2o/ch4/none','absolute_limits_hf': '8u/v/w/ts/co2/h2o/ch4/none','skewness_kurtosis_hf': '8u/v/w/ts/co2/h2o/ch4/none','skewness_kurtosis_sf': '8u/v/w/ts/co2/h2o/ch4/none','discontinuities_hf': '8u/v/w/ts/co2/h2o/ch4/none','discontinuities_sf': '8u/v/w/ts/co2/h2o/ch4/none',
                  'timelag_hf': '8co2/h2o/ch4/none',             'timelag_sf': '8co2/h2o/ch4/none','attack_angle_hf' : '8aa',                        'non_steady_wind_hf' : '8U',                   'u_spikes' : '-',                 'v_spikes': '-',                                  
                  'w_spikes': '-',                               'ts_spikes': '-',                 'co2_spikes': '-',                                'h2o_spikes': '-',                             'ch4_spikes': '-',                                                                  
                  'chopper_LI-7500' : '#_flagged_recs', 'detector_LI-7500' : '#_flagged_recs',         'pll_LI-7500' : '#_flagged_recs',             'sync_LI-7500' : '#_flagged_recs',                                                                                                 'mean_value_AGC_LI-7500' : '-',                                                                                                    
                  'u_var' : 'm2 s-2',                            'v_var': 'm2 s-2',                                                                  'w_var': 'm2 s-2',                             'ts_var' : 'K2',   'co2_var' : '-',                               'h2o_var' : '-',                                                                    'ch4_var' : '-',                                                                                                                   
                  'w/ts_cov' : 'm s-1 K',                        'w/co2_cov'  : '-',                                                                 'w/h2o_cov'  : '-',                            'w/ch4_cov' : '-', 'diag_77_mean' : '-'}                                                                                                                  
    
    all_keys = ['filename','date','time','DOY','daytime','file_records','used_records','Tau','qc_Tau','H','qc_H','LE','qc_LE','co2_flux',                'qc_co2_flux','h2o_flux','qc_h2o_flux','ch4_flux','qc_ch4_flux','H_strg','LE_strg','co2_strg','h2o_strg','ch4_strg',                'co2_v-adv','h2o_v-adv','ch4_v-adv','co2_molar_density','co2_mole_fraction','co2_mixing_ratio','co2_time_lag',                'co2_def_timelag','h2o_molar_density','h2o_mole_fraction','h2o_mixing_ratio','h2o_time_lag','h2o_def_timelag',
                'ch4_molar_density','ch4_mole_fraction','ch4_mixing_ratio','ch4_time_lag','ch4_def_timelag','sonic_temperature',                'air_temperature','air_pressure','air_density','air_heat_capacity','air_molar_volume','ET','water_vapor_density','e','es',                'specific_humidity','RH','VPD','Tdew','u_unrot','v_unrot','w_unrot','u_rot','v_rot','w_rot','wind_speed','max_wind_speed',                'wind_dir','yaw','pitch','roll','u*','TKE','L','(z-d)/L','bowen_ratio','T*','model','x_peak','x_offset','x_10%','x_30%','x_50%',                'x_70%','x_90%','un_Tau','Tau_scf','un_H','H_scf','un_LE','LE_scf','un_co2_flux','co2_scf','un_h2o_flux','h2o_scf','un_ch4_flux',                'ch4_scf','spikes_hf','amplitude_resolution_hf','drop_out_hf','absolute_limits_hf','skewness_kurtosis_hf','skewness_kurtosis_sf',
                'discontinuities_hf','discontinuities_sf','timelag_hf','timelag_sf','attack_angle_hf','non_steady_wind_hf','u_spikes','v_spikes',                'w_spikes','ts_spikes','co2_spikes','h2o_spikes','ch4_spikes','chopper_LI-7500','detector_LI-7500','pll_LI-7500',                'sync_LI-7500','mean_value_AGC_LI-7500','u_var','v_var','w_var','ts_var','co2_var','h2o_var','ch4_var','w/ts_cov','w/co2_cov',                'w/h2o_cov','w/ch4_cov','diag_77_mean']
    
    #--- Read infile
    filename = os.path.join(datapathin,'%4d'%t1.astype(object).year,'%02d'%t1.astype(object).month,'flux_%4d%02d%02d.txt'%(t1.astype(object).year,t1.astype(object).month,t1.astype(object).day))
    adam_flux = pd.read_csv(filename,sep=',',names=all_keys,date_format='%Y-%m-%d %H:%M', parse_dates=[['date', 'time']],index_col='date_time',na_values=[-999,-9999])
    adam_flux.drop(columns=adam_flux.columns[0], axis= 1 , inplace= True)               # delete column "filename"
    all_keys.remove('filename')
    all_keys.remove('time')
    all_keys.remove('date')    
    all_keys = ['TIMESTAMP',]
    
    #--- Write outfile
    for key in adam_flux.keys(): all_keys.append(key)
    units  = [units_dict_flux[key] for key in all_keys]
    
    timeIndex         = np.arange(t1,t2,np.timedelta64(30,'m')).astype(np.datetime64)
    timeIndex         = pd.DatetimeIndex(timeIndex) + pd.Timedelta(minutes=30)
    adam_flux         = adam_flux.drop_duplicates()
    adam_flux         = adam_flux.reindex(timeIndex)
       
    filename = os.path.join(datapath,'AD_FLUX','ADAM_flux%4d%02d%02d.csv'%(t1.astype(object).year,t1.astype(object).month, t1.astype(object).day))
    with open(filename, 'w') as fp:
        fp.write(','.join(all_keys) + '\n')
        fp.write(','.join(units   ) + '\n')
        
    adam_flux.to_csv(filename,sep=',',header=None,mode='a',na_rep='NaN',date_format='%Y-%m-%d %H:%M:%S')

    return

#---
def Prepare4API_Amsterdam_Rad     (t1,t2,datapath):
    datapathin = os.path.join('W:\\','ESG','DOW_MAQ','MAQ_Archive','AAMS_archive','AAMS_data')
    
    # units dictionary
    units_dict_rad = {'TIMESTAMP'    : 'yyyy-mm-dd HH:MM:SS UTC', 'RECORD':'-','Ql_in_Avg': 'W m-2','Ql_out_Avg': 'W m-2','Qs_in_Avg': 'W m-2','Qs_out_Avg': 'W m-2','Qp_Avg': 'W m-2','T1_Avg': '°C','T2_Avg': '°C','LW_in': 'W m-2','LW_out': 'W m-2','Rnet': 'W m-2'}
    all_keys       = ['TIMESTAMP','RECORD','Ql_in_Avg','Ql_out_Avg','Qs_in_Avg','Qs_out_Avg','Qp_Avg','T1_Avg','T2_Avg','LW_in','LW_out','Rnet']
    
    # Read infile
    filename = os.path.join(datapathin,'%4d'%t1.astype(object).year,'%02d'%t1.astype(object).month,'rad_%4d%02d%02d.txt'%(t1.astype(object).year,t1.astype(object).month, t1.astype(object).day))
    adam_rad = pd.read_csv(filename,sep=',',skiprows=1,names=all_keys,index_col='TIMESTAMP',na_values=[-999,-9999])
    
    # Write outfile
    filename = os.path.join(datapath,'AD_RAD','Adam_rad%4d%02d%02d.csv'%(t1.astype(object).year,t1.astype(object).month, t1.astype(object).day))
    units  = [units_dict_rad[key] for key in all_keys]
    with open(filename, 'w') as fp:
        fp.write(','.join(all_keys) + '\n')
        fp.write(','.join(units   ) + '\n')
    adam_rad.to_csv(filename,sep=',',header=None,mode='a',na_rep='NaN',date_format='%Y-%m-%d %H:%M:%S')

    return





































'''
#---
def Prepare4API_Loobos_BM_Soil_old(t1,t2,datapath): # For the old F41 files (do not use anymore, just for archiving)
    daytime = pd.to_datetime(t1)    

    if   daytime <= pd.to_datetime('2023-01-31'):
        all_keys             = ['TIMESTAMP', 'G_IU_1_1_1','G_IU_2_1_1','G_IU_3_1_1','G_IU_4_1_1','G_SF_1_1_1','G_SF_2_1_1','G_SF_3_1_1','G_SF_4_1_1', 'TS_1_2_1',                     'TS_1_1_1','TS_2_2_1','TS_2_1_1','TS_3_1_1','TS_3_2_1','TS_4_1_1','TS_4_2_1','SWC_1_1_1','CS655_s_1_1_1','TS_1_2_2',
                                'SWC_IU_1_1_1', 'CS655_t_1_1_1','CS655_a_1_1_1','SWC_1_2_1','CS655_s_1_2_1','TS_1_3_1','SWC_IU_1_2_1','CS655_t_1_2_1','CS655_a_1_2_1','SWC_1_3_1',    'CS655_s_1_3_1','TS_1_4_1','SWC_IU_1_3_1', 'CS655_t_1_3_1','CS655_a_1_3_1','SWC_1_4_1','CS655_s_1_4_1','TS_1_5_1','SWC_IU_1_4_1','CS655_t_1_4_1',
                                'CS655_a_1_4_1','SWC_1_5_1','CS655_s_1_5_1','TS_1_6_1', 'SWC_IU_1_5_1','CS655_t_1_5_1','CS655_a_1_5_1','SWC_3_1_1','CS655_s_3_1_1','TS_3_2_2',        'SWC_IU_3_1_1','CS655_t_3_1_1','CS655_a_3_1_1','SWC_2_1_1','CS655_s_2_1_1','TS_2_2_2','SWC_IU_2_1_1','CS655_t_2_1_1','CS655_a_2_1_1','SWC_2_2_1',
                                'CS655_s_2_2_1','TS_2_3_1','SWC_IU_2_2_1','CS655_t_2_2_1','CS655_a_2_2_1','SWC_2_3_1', 'CS655_s_2_3_1','TS_2_4_1','SWC_IU_2_3_1','CS655_t_2_3_1',     'CS655_a_2_3_1','SWC_2_4_1','CS655_s_2_4_1','TS_2_5_1','SWC_IU_2_4_1','CS655_t_2_4_1','CS655_a_2_4_1','SWC_2_5_1','CS655_s_2_5_1','TS_2_6_1',
                                'SWC_IU_2_5_1','CS655_t_2_5_1','CS655_a_2_5_1','SWC_4_1_1','CS655_s_4_1_1','TS_4_2_2','SWC_IU_4_1_1','CS655_t_4_1_1','CS655_a_4_1_1','WCP_1_1_1',     'G_1_1_1','G_2_1_1','G_3_1_1','G_4_1_1', 'G_ISCAL_1_1_1',	'G_ISCAL_2_1_1',	'G_ISCAL_3_1_1',	'G_ISCAL_4_1_1']
        keys                 = all_keys
    elif daytime == pd.to_datetime('2023-02-17'):
        all_keys             = ['TIMESTAMP', 'G_IU_1_1_1','G_IU_2_1_1','G_IU_3_1_1','G_IU_4_1_1','G_SF_1_1_1','G_SF_2_1_1','G_SF_3_1_1','G_SF_4_1_1', 'TS_1_2_1','TS_1_1_1','TS_2_2_1','TS_2_1_1','TS_3_1_1','TS_3_2_1','TS_4_1_1','TS_4_2_1','SWC_1_1_1','CS655_s_1_1_1','TS_1_2_2','SWC_IU_1_1_1',                                
                                'CS655_t_1_1_1','CS655_a_1_1_1','SWC_1_2_1','CS655_s_1_2_1','TS_1_3_1','SWC_IU_1_2_1','CS655_t_1_2_1','CS655_a_1_2_1','SWC_1_3_1','CS655_s_1_3_1','TS_1_4_1','SWC_IU_1_3_1',      'CS655_t_1_3_1','CS655_a_1_3_1','SWC_1_4_1','CS655_s_1_4_1','TS_1_5_1','SWC_IU_1_4_1','CS655_t_1_4_1','CS655_a_1_4_1','SWC_1_5_1','CS655_s_1_5_1','TS_1_6_1',                 
                                'SWC_IU_1_5_1','CS655_t_1_5_1','CS655_a_1_5_1','SWC_3_1_1','CS655_s_3_1_1','TS_3_2_2','SWC_IU_3_1_1','CS655_t_3_1_1','CS655_a_3_1_1','SWC_2_1_1','CS655_s_2_1_1',                 'TS_2_2_2','SWC_IU_2_1_1','CS655_a_2_1_1','SWC_2_2_1','CS655_s_2_2_1','TS_2_3_1','SWC_IU_2_2_1','CS655_t_2_2_1','CS655_a_2_2_1','SWC_2_3_1',                                  
                                'CS655_s_2_3_1','TS_2_4_1','SWC_IU_2_3_1','CS655_t_2_3_1','CS655_a_2_3_1','SWC_2_4_1','CS655_s_2_4_1','TS_2_5_1','SWC_IU_2_4_1','CS655_t_2_4_1','CS655_a_2_4_1',                  'SWC_2_5_1','CS655_s_2_5_1','TS_2_6_1','SWC_IU_2_5_1','CS655_t_2_5_1','CS655_a_2_5_1','SWC_4_1_1','CS655_s_4_1_1','TS_4_2_2','SWC_IU_4_1_1',                                  
                                'CS655_t_4_1_1','CS655_a_4_1_1', 'WCP_1_1_1', 'TS_1_7_1', 'WCP_2_1_1', 'TS_2_7_1', 'G_1_1_1', 'G_2_1_1', 'G_3_1_1', 'G_4_1_1', 'G_ISCAL_1_1_1', 'G_ISCAL_2_1_1', 'G_ISCAL_3_1_1', 'G_ISCAL_4_1_1']                                                  
        keys                 = all_keys
    elif daytime  < pd.to_datetime('2023-02-21'):
        all_keys             = ['TIMESTAMP', 'G_IU_1_1_1','G_IU_2_1_1','G_IU_3_1_1','G_IU_4_1_1','G_SF_1_1_1','G_SF_2_1_1','G_SF_3_1_1','G_SF_4_1_1', 'TS_1_2_1','TS_1_1_1','TS_2_2_1','TS_2_1_1','TS_3_1_1','TS_3_2_1','TS_4_1_1','TS_4_2_1','SWC_1_1_1','CS655_s_1_1_1','TS_1_2_2','SWC_IU_1_1_1',                                     
                                'CS655_t_1_1_1','CS655_a_1_1_1','SWC_1_2_1','CS655_s_1_2_1','TS_1_3_1','SWC_IU_1_2_1','CS655_t_1_2_1','CS655_a_1_2_1','SWC_1_3_1','CS655_s_1_3_1','TS_1_4_1','SWC_IU_1_3_1',      'CS655_t_1_3_1','CS655_a_1_3_1','SWC_1_4_1','CS655_s_1_4_1','TS_1_5_1','SWC_IU_1_4_1','CS655_t_1_4_1','CS655_a_1_4_1','SWC_1_5_1','CS655_s_1_5_1','TS_1_6_1',                      
                                'SWC_IU_1_5_1','CS655_t_1_5_1','CS655_a_1_5_1','SWC_3_1_1','CS655_s_3_1_1','TS_3_2_2','SWC_IU_3_1_1','CS655_t_3_1_1','CS655_a_3_1_1','SWC_2_1_1','CS655_s_2_1_1',                 'TS_2_2_2','SWC_IU_2_1_1','CS655_t_2_1_1','CS655_a_2_1_1','SWC_2_2_1','CS655_s_2_2_1','TS_2_3_1','SWC_IU_2_2_1','CS655_t_2_2_1','CS655_a_2_2_1','SWC_2_3_1',                       
                                'CS655_s_2_3_1','TS_2_4_1','SWC_IU_2_3_1','CS655_t_2_3_1','CS655_a_2_3_1','SWC_2_4_1','CS655_s_2_4_1','TS_2_5_1','SWC_IU_2_4_1','CS655_t_2_4_1','CS655_a_2_4_1',                  'SWC_2_5_1','CS655_s_2_5_1','TS_2_6_1','SWC_IU_2_5_1','CS655_t_2_5_1','CS655_a_2_5_1','SWC_4_1_1','CS655_s_4_1_1','TS_4_2_2','SWC_IU_4_1_1',                                       
                                'CS655_t_4_1_1','CS655_a_4_1_1', 'WCP_1_1_1', 'TS_1_7_1', 'WCP_2_1_1', 'TS_2_7_1', 'G_1_1_1', 'G_2_1_1', 'G_3_1_1', 'G_4_1_1', 'G_ISCAL_1_1_1', 'G_ISCAL_2_1_1', 'G_ISCAL_3_1_1', 'G_ISCAL_4_1_1']                                                       
        keys                 = all_keys
    # elif daytime  < pd.to_datetime('2023-02-21'):
    #     all_keys             = ['TIMESTAMP','TS_1_1_1','TS_1_2_1','TS_1_2_2','TS_1_3_1','TS_1_4_1','TS_1_5_1','TS_1_6_1','TS_1_7_1','TS_2_1_1','TS_2_2_1','TS_2_2_2','TS_2_3_1','TS_2_4_1','TS_2_5_1','TS_2_6_1','TS_2_7_1','TS_3_1_1','TS_3_2_1','TS_3_2_2','TS_4_1_1','TS_4_2_1','TS_4_2_2','SWC_1_1_1','SWC_1_2_1','SWC_1_3_1','SWC_1_4_1','SWC_1_5_1','SWC_2_1_1','SWC_2_2_1','SWC_2_3_1','SWC_2_4_1','SWC_2_5_1','SWC_3_1_1','SWC_4_1_1','G_1_1_1','G_ISCAL_1_1_1','G_2_1_1','G_ISCAL_2_1_1','G_3_1_1','G_ISCAL_3_1_1','G_4_1_1','G_ISCAL_4_1_1','WTD_1_1_1','WTD_2_1_1','SWC_IU_1_1_1','SWC_IU_1_2_1','SWC_IU_1_3_1','SWC_IU_1_4_1','SWC_IU_1_5_1','SWC_IU_2_1_1','SWC_IU_2_2_1','SWC_IU_2_3_1','SWC_IU_2_4_1','SWC_IU_2_5_1','SWC_IU_3_1_1','SWC_IU_4_1_1','G_IU_1_1_1','G_IU_2_1_1','G_IU_3_1_1','G_IU_4_1_1','G_SF_1_1_1','G_SF_2_1_1','G_SF_3_1_1','G_SF_4_1_1','WCP_1_1_1','WCP_2_1_1']
    
    else: #if day >= pd.to_datetime('2023-02-21'):
        all_keys             = ['TIMESTAMP','TS_1_1_1','TS_1_2_1','TS_1_2_2','TS_1_3_1','TS_1_4_1','TS_1_5_1','TS_1_6_1','TS_1_7_1','TS_2_1_1','TS_2_2_1','TS_2_2_2','TS_2_3_1','TS_2_4_1','TS_2_5_1','TS_2_6_1','TS_2_7_1','TS_3_1_1','TS_3_2_1','TS_3_2_2','TS_4_1_1','TS_4_2_1','TS_4_2_2','SWC_1_1_1','SWC_1_2_1','SWC_1_3_1','SWC_1_4_1','SWC_1_5_1','SWC_2_1_1','SWC_2_2_1','SWC_2_3_1','SWC_2_4_1','SWC_2_5_1','SWC_3_1_1','SWC_4_1_1','G_1_1_1','G_ISCAL_1_1_1','G_2_1_1','G_ISCAL_2_1_1','G_3_1_1','G_ISCAL_3_1_1','G_4_1_1','G_ISCAL_4_1_1','WTD_1_1_1','WTD_2_1_1','SWC_IU_1_1_1','SWC_IU_1_2_1','SWC_IU_1_3_1','SWC_IU_1_4_1','SWC_IU_1_5_1','SWC_IU_2_1_1','SWC_IU_2_2_1','SWC_IU_2_3_1','SWC_IU_2_4_1','SWC_IU_2_5_1','SWC_IU_3_1_1','SWC_IU_4_1_1','G_IU_1_1_1','G_IU_2_1_1','G_IU_3_1_1','G_IU_4_1_1','G_SF_1_1_1','G_SF_2_1_1','G_SF_3_1_1','G_SF_4_1_1','WCP_1_1_1','WCP_2_1_1']
        keys                 = all_keys
    #--- Read infile
    bm_soil = Loobos_Read_NL_Loo_BM_Soil(  t1,t2,keys=None)
    
    #--- UTC
    CET           = pytz.timezone('Etc/GMT-1')
    bm_soil.index = bm_soil.index.tz_localize(CET).tz_convert(pytz.utc)
    
    if   daytime <= pd.to_datetime('2023-01-31'):
        units_dict_soil   = {'TIMESTAMP'    : 'yyyy-mm-dd HH:MM:SS UTC',
                             'G_IU_1_1_1':'-',       'G_IU_2_1_1':'-',         'G_IU_3_1_1':'-',         'G_IU_4_1_1':'-',       'G_SF_1_1_1':'-',       'G_SF_2_1_1':'-',         'G_SF_3_1_1':'-',         'G_SF_4_1_1':'-',      
                             'TS_1_2_1': '°C',       'TS_1_1_1': '°C',         'TS_2_2_1': '°C',         'TS_2_1_1': '°C',       'TS_3_1_1': '°C',       'TS_3_2_1': '°C',         'TS_4_1_1': '°C',         'TS_4_2_1': '°C',      
                             'SWC_1_1_1':'-',        'CS655_s_1_1_1':'-',      'TS_1_2_2': '°C',         'SWC_IU_1_1_1':'-',     'CS655_t_1_1_1':'-',    'CS655_a_1_1_1':'-',      'SWC_1_2_1':'-',          'CS655_s_1_2_1':'-',   
                             'TS_1_3_1': '°C',       'SWC_IU_1_2_1':'-',       'CS655_t_1_2_1':'-',      'CS655_a_1_2_1':'-',    'SWC_1_3_1':'-',        'CS655_s_1_3_1':'-',      'TS_1_4_1': '°C',         'SWC_IU_1_3_1':'-',    
                             'CS655_t_1_3_1':'-',    'CS655_a_1_3_1':'-',      'SWC_1_4_1':'-',          'CS655_s_1_4_1':'-',    'TS_1_5_1': '°C',       'SWC_IU_1_4_1':'-',       'CS655_t_1_4_1':'-',      'CS655_a_1_4_1':'-',   
                             'SWC_1_5_1':'-',        'CS655_s_1_5_1':'-',      'TS_1_6_1': '°C',         'SWC_IU_1_5_1':'-',     'CS655_t_1_5_1':'-',    'CS655_a_1_5_1':'-',      'SWC_3_1_1':'-',          'CS655_s_3_1_1':'-',   
                             'TS_3_2_2': '°C',       'SWC_IU_3_1_1':'-',       'CS655_t_3_1_1':'-',      'CS655_a_3_1_1':'-',    'SWC_2_1_1':'-',        'CS655_s_2_1_1':'-',      'TS_2_2_2': '°C',         'SWC_IU_2_1_1':'-',    
                             'CS655_t_2_1_1':'-',    'CS655_a_2_1_1':'-',      'SWC_2_2_1':'-',          'CS655_s_2_2_1':'-',    'TS_2_3_1': '°C',       'SWC_IU_2_2_1':'-',       'CS655_t_2_2_1':'-',      'CS655_a_2_2_1':'-',   
                             'SWC_2_3_1':'-',        'CS655_s_2_3_1':'-',      'TS_2_4_1': '°C',         'SWC_IU_2_3_1':'-',     'CS655_t_2_3_1':'-',    'CS655_a_2_3_1':'-',      'SWC_2_4_1':'-',          'CS655_s_2_4_1':'-',   
                             'TS_2_5_1': '°C',       'SWC_IU_2_4_1':'-',       'CS655_t_2_4_1':'-',      'CS655_a_2_4_1':'-',    'SWC_2_5_1':'-',        'CS655_s_2_5_1':'-',      'TS_2_6_1': '°C',         'SWC_IU_2_5_1':'-',    
                             'CS655_t_2_5_1':'-',    'CS655_a_2_5_1':'-',      'SWC_4_1_1':'-',          'CS655_s_4_1_1':'-',    'TS_4_2_2': '°C',       'SWC_IU_4_1_1':'-',       'CS655_t_4_1_1':'-',      'CS655_a_4_1_1':'-',   
                             'WCP_1_1_1':'Pa',       'G_1_1_1':'W -m2',        'G_2_1_1':'W m-2',        'G_3_1_1':'W m-2',      'G_4_1_1':'W m-2',      'G_ISCAL_1_1_1' : '-',    'G_ISCAL_2_1_1' : '-',    'G_ISCAL_3_1_1' : '-',       	'G_ISCAL_4_1_1' : '-'}
    elif   daytime < pd.to_datetime('2023-02-21'):
        units_dict_soil   = {'TIMESTAMP'    : 'yyyy-mm-dd HH:MM:SS UTC',
                             'G_IU_1_1_1':'-',        'G_IU_2_1_1':'-',         'G_IU_3_1_1':'-',       'G_IU_4_1_1':'-',        'G_SF_1_1_1':'-',        'G_SF_2_1_1':'-',         'G_SF_3_1_1':'-',       'G_SF_4_1_1':'-',      
                             'TS_1_2_1': '°C',        'TS_1_1_1': '°C',         'TS_2_2_1': '°C',       'TS_2_1_1': '°C',        'TS_3_1_1': '°C',        'TS_3_2_1': '°C',         'TS_4_1_1': '°C',       'TS_4_2_1': '°C',      
                             'SWC_1_1_1':'-',         'CS655_s_1_1_1':'-',      'TS_1_2_2': '°C',       'SWC_IU_1_1_1':'-',      'CS655_t_1_1_1':'-',     'CS655_a_1_1_1':'-',      'SWC_1_2_1':'-',        'CS655_s_1_2_1':'-',   
                             'TS_1_3_1': '°C',        'SWC_IU_1_2_1':'-',       'CS655_t_1_2_1':'-',    'CS655_a_1_2_1':'-',     'SWC_1_3_1':'-',         'CS655_s_1_3_1':'-',      'TS_1_4_1': '°C',       'SWC_IU_1_3_1':'-',    
                             'CS655_t_1_3_1':'-',     'CS655_a_1_3_1':'-',      'SWC_1_4_1':'-',        'CS655_s_1_4_1':'-',     'TS_1_5_1': '°C',        'SWC_IU_1_4_1':'-',       'CS655_t_1_4_1':'-',    'CS655_a_1_4_1':'-',   
                             'SWC_1_5_1':'-',         'CS655_s_1_5_1':'-',      'TS_1_6_1': '°C',       'SWC_IU_1_5_1':'-',      'CS655_t_1_5_1':'-',     'CS655_a_1_5_1':'-',      'SWC_3_1_1':'-',        'CS655_s_3_1_1':'-',   
                             'TS_3_2_2': '°C',        'SWC_IU_3_1_1':'-',       'CS655_t_3_1_1':'-',    'CS655_a_3_1_1':'-',     'SWC_2_1_1':'-',         'CS655_s_2_1_1':'-',      'TS_2_2_2': '°C',       'SWC_IU_2_1_1':'-',    
                             'CS655_t_2_1_1':'-',     'CS655_a_2_1_1':'-',      'SWC_2_2_1':'-',        'CS655_s_2_2_1':'-',     'TS_2_3_1': '°C',        'SWC_IU_2_2_1':'-',       'CS655_t_2_2_1':'-',    'CS655_a_2_2_1':'-',   
                             'SWC_2_3_1':'-',         'CS655_s_2_3_1':'-',      'TS_2_4_1': '°C',       'SWC_IU_2_3_1':'-',      'CS655_t_2_3_1':'-',     'CS655_a_2_3_1':'-',      'SWC_2_4_1':'-',        'CS655_s_2_4_1':'-',   
                             'TS_2_5_1': '°C',        'SWC_IU_2_4_1':'-',       'CS655_t_2_4_1':'-',    'CS655_a_2_4_1':'-',     'SWC_2_5_1':'-',         'CS655_s_2_5_1':'-',      'TS_2_6_1': '°C',       'SWC_IU_2_5_1':'-',    
                             'CS655_t_2_5_1':'-',     'CS655_a_2_5_1':'-',      'SWC_4_1_1':'-',        'CS655_s_4_1_1':'-',     'TS_4_2_2': '°C',        'SWC_IU_4_1_1':'-',       'CS655_t_4_1_1':'-',    'CS655_a_4_1_1':'-',   
                             'WCP_1_1_1':'Pa',        'TS_1_7_1': '°C',         'WCP_2_1_1':'Pa',       'TS_2_7_1': '°C',        'G_1_1_1':'W m-2',       'G_2_1_1':'W m-2',        'G_3_1_1':'W m-2',      'G_4_1_1':'W m-2',     
                             'G_ISCAL_1_1_1' : '-',   'G_ISCAL_2_1_1' : '-',    'G_ISCAL_3_1_1' : '-',  'G_ISCAL_4_1_1' : '-'}
    
    
    elif   daytime == pd.to_datetime('2023-02-17'):
        units_dict_soil   = {'TIMESTAMP'    : 'yyyy-mm-dd HH:MM:SS UTC',
                            'G_IU_1_1_1':'-',       'G_IU_2_1_1':'-',           'G_IU_3_1_1':'-',         'G_IU_4_1_1':'-',      'G_SF_1_1_1':'-',       'G_SF_2_1_1':'-',           'G_SF_3_1_1':'-',         'G_SF_4_1_1':'-',      
                            'TS_1_2_1': '°C',       'TS_1_1_1': '°C',           'TS_2_2_1': '°C',         'TS_2_1_1': '°C',      'TS_3_1_1': '°C',       'TS_3_2_1': '°C',           'TS_4_1_1': '°C',         'TS_4_2_1': '°C',      
                            'SWC_1_1_1':'-',        'CS655_s_1_1_1':'-',        'TS_1_2_2': '°C',         'SWC_IU_1_1_1':'-',    'CS655_t_1_1_1':'-',    'CS655_a_1_1_1':'-',        'SWC_1_2_1':'-',          'CS655_s_1_2_1':'-',   
                            'TS_1_3_1': '°C',       'SWC_IU_1_2_1':'-',         'CS655_t_1_2_1':'-',      'CS655_a_1_2_1':'-',   'SWC_1_3_1':'-',        'CS655_s_1_3_1':'-',        'TS_1_4_1': '°C',         'SWC_IU_1_3_1':'-',    
                            'CS655_t_1_3_1':'-',    'CS655_a_1_3_1':'-',        'SWC_1_4_1':'-',          'CS655_s_1_4_1':'-',   'TS_1_5_1': '°C',       'SWC_IU_1_4_1':'-',         'CS655_t_1_4_1':'-',      'CS655_a_1_4_1':'-',   
                            'SWC_1_5_1':'-',        'CS655_s_1_5_1':'-',        'TS_1_6_1': '°C',         'SWC_IU_1_5_1':'-',    'CS655_t_1_5_1':'-',    'CS655_a_1_5_1':'-',        'SWC_3_1_1':'-',          'CS655_s_3_1_1':'-',   
                            'TS_3_2_2': '°C',       'SWC_IU_3_1_1':'-',         'CS655_t_3_1_1':'-',      'CS655_a_3_1_1':'-',   'SWC_2_1_1':'-',        'CS655_s_2_1_1':'-',        'TS_2_2_2': '°C',         'SWC_IU_2_1_1':'-',    
                            'CS655_a_2_1_1':'-',    'SWC_2_2_1':'-',            'CS655_s_2_2_1':'-',                             'TS_2_3_1': '°C',       'SWC_IU_2_2_1':'-',         'CS655_t_2_2_1':'-',      'CS655_a_2_2_1':'-',   
                            'SWC_2_3_1':'-',        'CS655_s_2_3_1':'-',        'TS_2_4_1': '°C',         'SWC_IU_2_3_1':'-',    'CS655_t_2_3_1':'-',    'CS655_a_2_3_1':'-',        'SWC_2_4_1':'-',          'CS655_s_2_4_1':'-',   
                            'TS_2_5_1': '°C',       'SWC_IU_2_4_1':'-',         'CS655_t_2_4_1':'-',      'CS655_a_2_4_1':'-',   'SWC_2_5_1':'-',        'CS655_s_2_5_1':'-',        'TS_2_6_1': '°C',         'SWC_IU_2_5_1':'-',    
                            'CS655_t_2_5_1':'-',    'CS655_a_2_5_1':'-',        'SWC_4_1_1':'-',          'CS655_s_4_1_1':'-',   'TS_4_2_2': '°C',       'SWC_IU_4_1_1':'-',         'CS655_t_4_1_1':'-',      'CS655_a_4_1_1':'-',    
                            'WCP_1_1_1':'Pa',       'TS_1_7_1': '°C',           'WCP_2_1_1':'Pa',         'TS_2_7_1': '°C',      'G_1_1_1':'W m-2',      'G_2_1_1':'W m-2',          'G_3_1_1':'W m-2',        'G_4_1_1':'W m-2',     
                            'G_ISCAL_1_1_1' : '-',  'G_ISCAL_2_1_1' : '-',      'G_ISCAL_3_1_1' : '-',    'G_ISCAL_4_1_1' : '-'}
    
    # elif daytime  < pd.to_datetime('2023-02-21'):
    #     units_dict_soil   = {'TIMESTAMP'    : 'yyyy-mm-dd HH:MM:SS UTC',
    #                      'TS_1_2_1': '°C',            'TS_1_1_1': '°C',          'TS_2_2_1': '°C',         'TS_2_1_1': '°C',          #                      'TS_3_1_1': '°C',            'TS_3_2_1': '°C',          'TS_4_1_1': '°C',         'TS_4_2_1': '°C',      
    #                      'CS655_s_1_1_1':'-',         'TS_1_2_2': '°C',          'SWC_IU_1_1_1':'-',                                  #                      'CS655_t_1_1_1':'-',         'CS655_a_1_1_1':'-',       'CS655_s_1_2_1':'-',                             
    #                      'TS_1_3_1': '°C',            'SWC_IU_1_2_1':'-',        'CS655_t_1_2_1':'-',      'CS655_a_1_2_1':'-',       #                      'CS655_s_1_3_1':'-',         'TS_1_4_1': '°C',          'SWC_IU_1_3_1':'-',                              
    #                      'CS655_t_1_3_1':'-',         'CS655_a_1_3_1':'-',       'CS655_s_1_4_1':'-',                                 #                      'TS_1_5_1': '°C',            'SWC_IU_1_4_1':'-',        'CS655_t_1_4_1':'-',      'CS655_a_1_4_1':'-',   
    #                      'CS655_s_1_5_1':'-',         'TS_1_6_1': '°C',          'SWC_IU_1_5_1':'-',                                  #                      'CS655_t_1_5_1':'-',         'CS655_a_1_5_1':'-',       'CS655_s_3_1_1':'-',                             
    #                      'TS_3_2_2': '°C',            'SWC_IU_3_1_1':'-',        'CS655_t_3_1_1':'-',      'CS655_a_3_1_1':'-',       #                      'CS655_s_2_1_1':'-',         'TS_2_2_2': '°C',          'SWC_IU_2_1_1':'-',                              
    #                      'CS655_t_2_1_1':'-',         'CS655_a_2_1_1':'-',       'CS655_s_2_2_1':'-',                                 #                      'TS_2_3_1': '°C',            'SWC_IU_2_2_1':'-',        'CS655_t_2_2_1':'-',      'CS655_a_2_2_1':'-',   
    #                      'CS655_s_2_3_1':'-',         'TS_2_4_1': '°C',          'SWC_IU_2_3_1':'-',                                  #                      'CS655_t_2_3_1':'-',         'CS655_a_2_3_1':'-',       'CS655_s_2_4_1':'-',                             
    #                      'TS_2_5_1': '°C',            'SWC_IU_2_4_1':'-',        'CS655_t_2_4_1':'-',      'CS655_a_2_4_1':'-',       #                      'CS655_s_2_5_1':'-',         'TS_2_6_1': '°C',          'SWC_IU_2_5_1':'-',                              
    #                      'CS655_t_2_5_1':'-',         'CS655_a_2_5_1':'-',       'CS655_s_4_1_1':'-',                                 #                      'TS_4_2_2': '°C',            'SWC_IU_4_1_1':'-',        'CS655_t_4_1_1':'-',      'CS655_a_4_1_1':'-',   
    #                      'WCP_1_1_1':'Pa',            'TS_1_7_1': '°C',          'WCP_2_1_1':'-'}                                     
    else: 
        units_dict_soil = {'TIMESTAMP'    : 'yyyy-mm-dd HH:MM:SS UTC',
                           'TS_1_1_1': '°C',              'TS_1_2_1': '°C',          'TS_1_2_2': '°C',          'TS_1_3_1': '°C',       'TS_1_4_1': '°C',              'TS_1_5_1': '°C',          'TS_1_6_1': '°C',          'TS_1_7_1': '°C',          
                           'TS_2_1_1': '°C',              'TS_2_2_1': '°C',          'TS_2_2_2': '°C',          'TS_2_3_1': '°C',       'TS_2_4_1': '°C',              'TS_2_5_1': '°C',          'TS_2_6_1': '°C',          'TS_2_7_1': '°C',          
                           'TS_3_1_1': '°C',              'TS_3_2_1': '°C',          'TS_3_2_2': '°C',          'TS_4_1_1': '°C',       'TS_4_2_1': '°C',              'TS_4_2_2': '°C',          'SWC_1_1_1' : '%',          'SWC_1_2_1': '%',         
                           'SWC_1_3_1': '%',              'SWC_1_4_1': '%',          'SWC_1_5_1': '%',          'SWC_2_1_1': '%',       'SWC_2_2_1': '%',              'SWC_2_3_1': '%',          'SWC_2_4_1': '%',          'SWC_2_5_1': '%',          
                           'SWC_3_1_1': '%',              'SWC_4_1_1': '%',          'G_1_1_1' : 'W m-2',       'G_ISCAL_1_1_1' : '-',  'G_2_1_1' : 'W m-2',           'G_ISCAL_2_1_1' : '-',     'G_3_1_1' : 'W m-2',                                  
                           'G_ISCAL_3_1_1' : '-',         'G_4_1_1' : 'W m-2',       'G_ISCAL_4_1_1' : '-',     'WTD_1_1_1' : 'm',      'WTD_2_1_1' : 'm',             'SWC_IU_1_1_1' : '-',      'SWC_IU_1_2_1' : '-',      'SWC_IU_1_3_1' : '-',      
                           'SWC_IU_1_4_1' : '-',          'SWC_IU_1_5_1' : '-',      'SWC_IU_2_1_1' : '-',      'SWC_IU_2_2_1' : '-',   'SWC_IU_2_3_1' : '-',          'SWC_IU_2_4_1' : '-',      'SWC_IU_2_5_1' : '-',      'SWC_IU_3_1_1' : '-',      
                           'SWC_IU_4_1_1' : '-',          'G_IU_1_1_1' : 'mV',       'G_IU_2_1_1': 'mV',        'G_IU_3_1_1': 'mV',     'G_IU_4_1_1': 'mV',            'G_SF_1_1_1' : 'uV W-1 m2','G_SF_2_1_1': 'uV W-1 m2', 'G_SF_3_1_1': 'uV W-1 m2', 
                           'G_SF_4_1_1': 'uV W-1 m2',     'WCP_1_1_1' : 'Pa',        'WCP_2_1_1' : 'Pa'}
    
    # Write outfile
    filename = os.path.join(datapath,'LB_BM-Soil','LB_SOIL%4d%02d%02d.csv'%(t1.astype(object).year, t1.astype(object).month, t1.astype(object).day))#(daytime.year,daytime.month,daytime.day)
    units  = [units_dict_soil[key] for key in all_keys]
    with open(filename, 'w') as fp:
        fp.write(','.join(all_keys) + '\n')
        fp.write(','.join(units   ) + '\n')
    bm_soil.to_csv(filename,sep=',',header=None,mode='a',na_rep='NaN',date_format='%Y-%m-%d %H:%M:%S')

    return
#---
'''

