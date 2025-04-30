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
#end_date = np.datetime64('2025-04-29').astype('datetime64[D]') + np.timedelta64(1,'D')

post_to_api = True

#Define the renaming dataframe used later
rename_dict_distnetw = {'th1-s31_temp':'DN_TA_1_1_1', 'th1-s31_humidity':'DN_RH_1_1_1',
                        'th2-s31_temp':'DN_TA_2_1_1', 'th2-s31_humidity':'DN_RH_2_1_1',
                        'u1-sdi_u':'DN_WS_2_1_1', 'u1-sdi_dir':'DN_WD_2_1_1', 'u1-sdi_gust':'DN_WX_2_1_1'
                       }

#Define the unit dataframe used later. Needs to be consistent with renaming dataframe
units_dict_distnetw = {'TIMESTAMP':'yyyy-mm-dd HH:MM:SS UTC',
                       'DN_TA_1_1_1':'°C','DN_RH_1_1_1':'%',
                       'DN_TA_2_1_1':'°C','DN_RH_2_1_1':'%',
                       'DN_WS_2_1_1':'m s-1','DN_WD_2_1_1':'°','DN_WX_2_1_1':'m s-1'
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
    t2      = t1+np.timedelta64(1,'D')
    dayfile = pd.read_csv(datapath_data+'IOT_data_'+str(t1).replace('-', '')+'.csv',header=0)
    dayfile.rename(columns={'timestamp': 'TIMESTAMP'}, inplace=True)
    dayfile['TIMESTAMP'] = pd.to_datetime(dayfile['TIMESTAMP']).dt.tz_localize(None)
    dayfile.set_index('TIMESTAMP', inplace=True)
    
    #Only take the variables listed in rename_dict and give them new name
    columns_to_keep = [col for col in rename_dict_distnetw if col in dayfile.columns]
    dayfile = dayfile[columns_to_keep]
    dayfile = dayfile.rename(columns={col: rename_dict_distnetw[col] for col in columns_to_keep})

    # Resample to the closest minute
    dayfile.index = dayfile.index.round('min')
    start_time = pd.Timestamp(t1.astype('datetime64[D]'))  # Start at midnight
    end_time = pd.Timestamp(t1.astype('datetime64[D]') + np.timedelta64(1, 'D') - np.timedelta64(1, 's'))  # End at 23:59:59
    resampled_index = pd.date_range(start=start_time, end=end_time, freq='min')
    dayfile_resampled = pd.DataFrame(index=resampled_index)
    dayfile_resampled = dayfile_resampled.join(dayfile, how='left')

    #Save MAQ-Obs data to W:/ as MAQ-Obs structure
    renamed_vars = [col for col in dayfile_resampled.columns if col in rename_dict_distnetw.values()]
    all_keys = ['TIMESTAMP'] + renamed_vars
    units  = ['yyyy-mm-dd HH:MM:SS UTC'] + [units_dict_distnetw.get(col, '') for col in renamed_vars]

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
