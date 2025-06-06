import sys
import os
import shutil
from sync_data import sync_files
from datetime import datetime
import numpy as np
import pandas as pd
sys.path.append(os.path.join('W:\\','ESG','DOW_MAQ','MAQ_Archive','MAQ-Observations.nl','Python'))
from   Post2API_functions   import *

#Define time parameters. By default runs for yesterday and today otherwise outcomment and define start_date and end_date.
today = np.datetime64('today', 'D')
start_date = today - np.timedelta64(1, 'D')
end_date = today + np.timedelta64(1, 'D')
#start_date = np.datetime64('2025-04-21').astype('datetime64[D]')
#end_date = np.datetime64('2025-05-30').astype('datetime64[D]') + np.timedelta64(1,'D')

post_to_api = True

#Define the renaming dataframe used later
rename_dict_distnetw = {'th1-s31_temp':'DN_TA_1_1_1', 'th1-s31_humidity':'DN_RH_1_1_1',                                         #Gustav Mahlerplein
                        'th2-s31_temp':'DN_TA_2_1_1', 'th2-s31_humidity':'DN_RH_2_1_1',                                         #Nieuwe Uilenburgstraat
                        'u1-sdi_u':'DN_WS_2_1_1', 'u1-sdi_dir':'DN_WD_2_1_1', 'u1-sdi_gust':'DN_WX_2_1_1',                      #Nieuwe Uilenburgstraat
                        'th5-s31_temp':'DN_TA_3_1_1', 'th5-s31_humidity':'DN_RH_3_1_1', 'temperature2-d20_temp':'DN_TG_3_1_1',  #IJburglaan
                        'u3-sdi_u':'DN_WS_3_1_1', 'u3-sdi_dir':'DN_WD_3_1_1', 'u3-sdi_gust':'DN_WX_3_1_1',                      #IJburglaan
                       }

#Define the renaming for KPN data used later
rename_dict_distnetw_kpn = {'temp3_temp':'DN_TG_4_1_1',                                                                         #Lutmastraat
                            'th6_temp':'DN_TA_4_1_1','th6_humidity':'DN_RH_4_1_1',                                              #Lutmastraat
                            'u6_u':'DN_WS_4_1_1', 'u6_dir':'DN_WD_4_1_1', 'u6_gust':'DN_WX_4_1_1',                              #Lutmastraat
                            'th4_temp':'DN_TA_5_1_1','th4_humidity':'DN_RH_5_1_1',                                              #Benkoelenstraat
                            'u4_u':'DN_WS_5_1_1', 'u4_dir':'DN_WD_5_1_1', 'u4_gust':'DN_WX_5_1_1',                              #Benkoelenstraat
                            }


#Define the unit dataframe used later. Needs to be consistent with renaming dataframe
units_dict_distnetw = {'TIMESTAMP':'yyyy-mm-dd HH:MM:SS UTC',
                       'DN_TA_1_1_1':'°C','DN_RH_1_1_1':'%',                                    #Gustav Mahlerplein
                       'DN_TA_2_1_1':'°C','DN_RH_2_1_1':'%',                                    #Nieuwe Uilenburgstraat
                       'DN_WS_2_1_1':'m s-1','DN_WD_2_1_1':'°','DN_WX_2_1_1':'m s-1',           #Nieuwe Uilenburgstraat
                       'DN_TA_3_1_1':'°C','DN_RH_3_1_1':'%','DN_TG_3_1_1':'°C',                 #IJburglaan
                       'DN_WS_3_1_1':'m s-1','DN_WD_3_1_1':'°','DN_WX_3_1_1':'m s-1',           #IJburglaan
                       }

#Define the unit dataframe used later. Needs to be consistent with renaming dataframe (KPN)
units_dict_distnetw_kpn = {'TIMESTAMP':'yyyy-mm-dd HH:MM:SS UTC',
                           'DN_TG_4_1_1':'°C',                                                  #Lutmastraat
                           'DN_TA_4_1_1':'°C','DN_RH_4_1_1':'%',                                #Lutmastraat
                           'DN_WS_4_1_1':'m s-1', 'DN_WD_4_1_1':'°', 'DN_WX_4_1_1':'m s-1',     #Lutmastraat
                           'DN_TA_5_1_1':'°C','DN_RH_5_1_1':'%',                                #Benkoelenstraat
                           'DN_WS_5_1_1':'m s-1', 'DN_WD_5_1_1':'°', 'DN_WX_5_1_1':'m s-1',     #Benkoelenstraat
                           }

######CODE SHOULD NOT HAVE TO BE CHANGED BELOW THIS LINE.
###ADD NEW STREAMS = ADAPT 2 DICTS
###RUN FOR HISTORICAL DATES = ADAPT STARTIME ENDTIME ABOVE
######

#First sync the directories
source = r'C:\AAMS_data\DistributedNetwork'
dest = r'W:\ESG\DOW_MAQ\MAQ_Archive\AAMS_archive\DistributedNetwork'
dest2 = r'D:\AAMS_archive\AAMS_data\DistributedNetwork'

sync_files(source, dest)
sync_files(source, dest2)

#Now prepare the data for MAQ-Obs database
datapath_data = r'C:\AAMS_data\DistributedNetwork\\'
datapath_maqobs = os.path.join('W:\\','ESG','DOW_MAQ','MAQ_Archive','MAQ-Observations.nl','data')

for t1 in np.arange(start_date,end_date,np.timedelta64(1,'D')):
    #Load and prepare file (original)
    t2      = t1+np.timedelta64(1,'D')
    dayfile = pd.read_csv(datapath_data+'IOT_data_'+str(t1).replace('-', '')+'.csv',header=0)
    dayfile.rename(columns={'timestamp': 'TIMESTAMP'}, inplace=True)
    dayfile['TIMESTAMP'] = pd.to_datetime(dayfile['TIMESTAMP']).dt.tz_localize(None)
    dayfile.set_index('TIMESTAMP', inplace=True)
    
    #Load and prepare file (KPN)
    dayfile_kpn = pd.read_csv(datapath_data+'IOT_data_kpn_'+str(t1).replace('-', '')+'.csv',header=0)
    dayfile_kpn.rename(columns={'timestamp': 'TIMESTAMP'}, inplace=True)
    dayfile_kpn['TIMESTAMP'] = pd.to_datetime(dayfile_kpn['TIMESTAMP']).dt.tz_localize(None)
    dayfile_kpn.set_index('TIMESTAMP', inplace=True)
    
    #Only take the variables listed in rename_dict and give them new name (original)
    columns_to_keep = [col for col in rename_dict_distnetw if col in dayfile.columns]
    dayfile = dayfile[columns_to_keep]
    dayfile = dayfile.rename(columns={col: rename_dict_distnetw[col] for col in columns_to_keep})
    
    #Only take the variables listed in rename_dict and give them new name (KPN)
    columns_to_keep_kpn = [col for col in rename_dict_distnetw_kpn if col in dayfile_kpn.columns]
    dayfile_kpn = dayfile_kpn[columns_to_keep_kpn]
    dayfile_kpn = dayfile_kpn.rename(columns={col: rename_dict_distnetw_kpn[col] for col in columns_to_keep_kpn})

    #Merge dataframes and reorganize time
    merged_dayfile = pd.merge(dayfile, dayfile_kpn, left_index=True, right_index=True, how='outer')
    merged_dayfile = merged_dayfile.sort_index()

    # Resample to the closest minute
    merged_dayfile.index = merged_dayfile.index.round('min')
    start_time = pd.Timestamp(t1.astype('datetime64[D]'))  # Start at midnight
    end_time = pd.Timestamp(t1.astype('datetime64[D]') + np.timedelta64(1, 'D') - np.timedelta64(1, 's'))  # End at 23:59:59
    resampled_index = pd.date_range(start=start_time, end=end_time, freq='min')
    dayfile_resampled = pd.DataFrame(index=resampled_index)
    dayfile_resampled = dayfile_resampled.join(merged_dayfile, how='left')
    dayfile_resampled.replace(-999.0, np.nan, inplace=True)

    #Save MAQ-Obs data to W:/ as MAQ-Obs structure
    renamed_vars = [col for col in dayfile_resampled.columns if col in rename_dict_distnetw.values()]
    renamed_vars_kpn = [col for col in dayfile_resampled.columns if col in rename_dict_distnetw_kpn.values()]
    all_keys = ['TIMESTAMP'] + renamed_vars + renamed_vars_kpn
    units  = ['yyyy-mm-dd HH:MM:SS UTC'] + [units_dict_distnetw.get(col, '') for col in renamed_vars] + [units_dict_distnetw_kpn.get(col, '') for col in renamed_vars_kpn]

    filename = os.path.join(datapath_maqobs,'AD_DN','ADAM_DN%4d%02d%02d.csv'%(t1.astype(object).year,t1.astype(object).month, t1.astype(object).day))
    
    with open(filename, 'w') as fp:
        fp.write(','.join(all_keys) + '\n')
        fp.write(','.join(units   ) + '\n')
    dayfile_resampled.to_csv(filename,sep=',',header=None,mode='a',na_rep='NaN',date_format='%Y-%m-%d %H:%M:%S')

    #Send to MAQ-Obs database
    if post_to_api == True:
        datapath = os.path.join('W:\\','ESG','DOW_MAQ','MAQ_Archive','MAQ-Observations.nl','data','AD_DN')
        END_POINT_AD = '/wp-json/maq/v1/sites/3/stations/3/import'
        print('Post2API Amsterdam Distributed Network started')
        prepare_files(t1,t2,datapath+'\\',END_POINT_AD,False)
        print('Post2API Amsterdam Distributed Network successful')
