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
#start_date = np.datetime64('2025-05-26').astype('datetime64[D]')
#end_date = np.datetime64('2025-06-05').astype('datetime64[D]') + np.timedelta64(1,'D')

post_to_api = True

#Define the renaming dataframe used later
rename_dict_distnetw = {'th1-s31_temp':'DN_TA_1_1_1', 'th1-s31_humidity':'DN_RH_1_1_1',                                         #Gustav Mahlerplein DN_1
                        'th2-s31_temp':'DN_TA_2_1_1', 'th2-s31_humidity':'DN_RH_2_1_1',                                         #Nieuwe Uilenburgstraat DN_2
                        'u1-sdi_u':'DN_WS_2_1_1', 'u1-sdi_dir':'DN_WD_2_1_1', 'u1-sdi_gust':'DN_WX_2_1_1',                      #Nieuwe Uilenburgstraat DN_2
                        'th23_temp':'DN_TA_23_1_1', 'th23_humidity':'DN_RH_23_1_1',                                     #Natuur eiland IJburg DN_23
                        'u23_u':'DN_WS_23_1_1', 'u23_dir':'DN_WD_23_1_1', 'u23_gust':'DN_WX_23_1_1',                #Natuur eiland IJburg DN_23                    
                       }

#Define the renaming for KPN data used later
rename_dict_distnetw_kpn = {'th5_temp':'DN_TA_3_1_1', 'th5_humidity':'DN_RH_3_1_1', 'temp2_temp':'DN_TG_3_1_1',                     #Diemerparklaan DN_3
                            'u3_u':'DN_WS_3_1_1', 'u3_dir':'DN_WD_3_1_1', 'u3_gust':'DN_WX_3_1_1',                                  #Diemerparklaan DN_3
                            'th6_temp':'DN_TA_4_1_1', 'th6_humidity':'DN_RH_4_1_1', 'temp3_temp':'DN_TG_4_1_1',                     #Lutmastraat DN_4
                            'u6_u':'DN_WS_4_1_1', 'u6_dir':'DN_WD_4_1_1', 'u6_gust':'DN_WX_4_1_1',                                  #Lutmastraat DN_4
                            'th4_temp':'DN_TA_5_1_1','th4_humidity':'DN_RH_5_1_1',                                                  #Benkoelenstraat DN_5
                            'u4_u':'DN_WS_5_1_1', 'u4_dir':'DN_WD_5_1_1', 'u4_gust':'DN_WX_5_1_1',                                  #Benkoelenstraat DN_5
                            'th3_temp':'DN_TA_6_1_1', 'th3_humidity':'DN_RH_6_1_1', 'temp1_temp':'DN_TG_6_1_1',                     #Majangracht DN_6
                            'u5_u':'DN_WS_6_1_1', 'u5_dir':'DN_WD_6_1_1', 'u5_gust':'DN_WX_6_1_1',                                  #Majangracht DN_6
                            'th7-s31_temp':'DN_TA_7_1_1', 'th7_humidity':'DN_RH_7_1_1',                                             #Oudezijdsvoorburgwal_N DN_7
                            'u7_u':'DN_WS_7_1_1', 'u7_dir':'DN_WD_7_1_1', 'u7_gust':'DN_WX_7_1_1',                                  #Oudezijdsvoorburgwal_N DN_7
                            'th8_temp':'DN_TA_8_1_1', 'th8_humidity':'DN_RH_8_1_1',                                                 #Oudezijdsvoorburgwal_Z DN_8
                            'u8_u':'DN_WS_8_1_1', 'u8_dir':'DN_WD_8_1_1', 'u8_gust':'DN_WX_8_1_1',                                  #Oudezijdsvoorburgwal_Z DN_8
                            'lws2_lws':'DN_LW_LWS_8_1_1', 'lws2_temp_leaf':'DN_LW_TLEAF_8_1_1',                                     #Oudezijdsvoorburgwal_Z DN_8
                            'th9_temp':'DN_TA_9_1_1','th9_humidity':'DN_RH_9_1_1',                                                  #Saskia van Uylenburgweg DN_9
                            'u9_u':'DN_WS_9_1_1', 'u9_dir':'DN_WD_9_1_1', 'u9_gust':'DN_WX_9_1_1',                                  #Saskia van Uylenburgweg DN_9
                            'th10_temp':'DN_TA_10_1_1','th10_humidity':'DN_RH_10_1_1',                                              #Tweede Oosterparkstraat DN_10
                            'u10_u':'DN_WS_10_1_1', 'u10_dir':'DN_WD_10_1_1', 'u10_gust':'DN_WX_10_1_1',                            #Tweede Oosterparkstraat DN_10
                            'th11_temp':'DN_TA_11_1_1','th11_humidity':'DN_RH_11_1_1',                                              #Galileiplantsoen DN_11
                            'u11_u':'DN_WS_11_1_1', 'u11_dir':'DN_WD_11_1_1', 'u11_gust':'DN_WX_11_1_1',                            #Galileiplantsoen DN_11
                            'th12_temp':'DN_TA_12_1_1', 'th12_humidity':'DN_RH_12_1_1', 'temp4_temp':'DN_TG_12_1_1',                #Saxenburgerdwarsstraat DN_12
                            'u12_u':'DN_WS_12_1_1', 'u12_dir':'DN_WD_12_1_1', 'u12_gust':'DN_WX_12_1_1',                            #Saxenburgerdwarsstraat DN_12
                            'th13_temp':'DN_TA_13_1_1','th13_humidity':'DN_RH_13_1_1',                                              #Raephaelstraat DN_13
                            'u13_u':'DN_WS_13_1_1', 'u13_dir':'DN_WD_13_1_1', 'u13_gust':'DN_WX_13_1_1',                            #Raephaelstraat DN_13
                            'th14_temp':'DN_TA_14_1_1','th14_humidity':'DN_RH_14_1_1',                                              #Kinkerstraat DN_14
                            'u14_u':'DN_WS_14_1_1', 'u14_dir':'DN_WD_14_1_1', 'u14_gust':'DN_WX_14_1_1',                            #Kinkerstraat DN_14
                            'th15_temp':'DN_TA_15_1_1', 'th15_humidity':'DN_RH_15_1_1', 'temp5_temp':'DN_TG_15_1_1',                #Anjeliersstraat DN_15
                            'u15_u':'DN_WS_15_1_1', 'u15_dir':'DN_WD_15_1_1', 'u15_gust':'DN_WX_15_1_1',                            #Anjeliersstraat DN_15
                            'th16_temp':'DN_TA_16_1_1','th16_humidity':'DN_RH_16_1_1',                                              #Kattengat DN_16
                            'u16_u':'DN_WS_16_1_1', 'u16_dir':'DN_WD_16_1_1', 'u16_gust':'DN_WX_16_1_1',                            #Kattengat DN_16
                            'th17_temp':'DN_TA_17_1_1', 'th17_humidity':'DN_RH_17_1_1', 'temp6_temp':'DN_TG_17_1_1',                #Markengauw DN_17
                            'u17_u':'DN_WS_17_1_1', 'u17_dir':'DN_WD_17_1_1', 'u17_gust':'DN_WX_17_1_1',                            #Markengauw DN_17
                            'th18_temp':'DN_TA_18_1_1','th18_humidity':'DN_RH_18_1_1',                                              #Purmerweg DN_18
                            'u18_u':'DN_WS_18_1_1', 'u18_dir':'DN_WD_18_1_1', 'u18_gust':'DN_WX_18_1_1',                            #Purmerweg DN_18
                            'th19_temp':'DN_TA_19_1_1','th19_humidity':'DN_RH_19_1_1',                                              #Zamenhofstraat DN_19
                            'u19_u':'DN_WS_19_1_1', 'u19_dir':'DN_WD_19_1_1', 'u19_gust':'DN_WX_19_1_1',                            #Zamenhofstraat DN_19
                            'th20_temp':'DN_TA_20_1_1','th20_humidity':'DN_RH_20_1_1',                                              #Scherpenzeelstraat DN_20
                            'u20_u':'DN_WS_20_1_1', 'u20_dir':'DN_WD_20_1_1', 'u20_gust':'DN_WX_20_1_1',                            #Scherpenzeelstraat DN_20
                            'th21_temp':'DN_TA_21_1_1','th21_humidity':'DN_RH_21_1_1',                                              #Comeniusstraat DN_21
                            'u21_u':'DN_WS_21_1_1', 'u21_dir':'DN_WD_21_1_1', 'u21_gust':'DN_WX_21_1_1',                            #Comeniusstraat DN_21
                            'th22_temp':'DN_TA_22_1_1','th22_humidity':'DN_RH_22_1_1',                                              #Kwelderweg DN_22
                            'u22_u':'DN_WS_22_1_1', 'u22_dir':'DN_WD_22_1_1', 'u22_gust':'DN_WX_22_1_1',                            #Kwelderweg DN_22
                            }


#Define the unit dataframe used later. Needs to be consistent with renaming dataframe
units_dict_distnetw = {'TIMESTAMP':'yyyy-mm-dd HH:MM:SS UTC',
                       'DN_TA_1_1_1':'°C','DN_RH_1_1_1':'%',                                    #Gustav Mahlerplein DN_1
                       'DN_TA_2_1_1':'°C','DN_RH_2_1_1':'%',                                    #Nieuwe Uilenburgstraat DN_2
                       'DN_WS_2_1_1':'m s-1','DN_WD_2_1_1':'°','DN_WX_2_1_1':'m s-1',           #Nieuwe Uilenburgstraat DN_2
                       'DN_TA_23_1_1':'°C','DN_RH_23_1_1':'%',                                  #Nieuwe Uilenburgstraat DN_23
                       'DN_WS_23_1_1':'m s-1','DN_WD_23_1_1':'°','DN_WX_23_1_1':'m s-1',        #Nieuwe Uilenburgstraat DN_23
                       }

#Define the unit dataframe used later. Needs to be consistent with renaming dataframe (KPN)
units_dict_distnetw_kpn = {'TIMESTAMP':'yyyy-mm-dd HH:MM:SS UTC',
                           'DN_TA_3_1_1':'°C','DN_RH_3_1_1':'%','DN_TG_3_1_1':'°C',                 #Diemerparklaan DN_3
                           'DN_WS_3_1_1':'m s-1','DN_WD_3_1_1':'°','DN_WX_3_1_1':'m s-1',           #Diemerparklaan DN_3
                           'DN_TA_4_1_1':'°C','DN_RH_4_1_1':'%','DN_TG_4_1_1':'°C',                 #Lutmastraat DN_4
                           'DN_WS_4_1_1':'m s-1', 'DN_WD_4_1_1':'°', 'DN_WX_4_1_1':'m s-1',         #Lutmastraat DN_4
                           'DN_TA_5_1_1':'°C','DN_RH_5_1_1':'%',                                    #Benkoelenstraat DN_5
                           'DN_WS_5_1_1':'m s-1', 'DN_WD_5_1_1':'°', 'DN_WX_5_1_1':'m s-1',         #Benkoelenstraat DN_5
                           'DN_TA_6_1_1':'°C','DN_RH_6_1_1':'%','DN_TG_6_1_1':'°C',                 #Majangracht DN_6
                           'DN_WS_6_1_1':'m s-1','DN_WD_6_1_1':'°','DN_WX_6_1_1':'m s-1',           #Majangracht DN_6
                           'DN_TA_7_1_1':'°C','DN_RH_7_1_1':'%',                                    #Oudezijdsvoorburgwal_N DN_7
                           'DN_WS_7_1_1':'m s-1','DN_WD_7_1_1':'°','DN_WX_7_1_1':'m s-1',           #Oudezijdsvoorburgwal_N DN_7
                           'DN_TA_8_1_1':'°C','DN_RH_8_1_1':'%',                                    #Oudezijdsvoorburgwal_Z DN_8
                           'DN_WS_8_1_1':'m s-1','DN_WD_8_1_1':'°','DN_WX_8_1_1':'m s-1',           #Oudezijdsvoorburgwal_Z DN_8
                           'DN_LW_LWS_8_1_1':'%','DN_LW_TLEAF_8_1_1':'°C',                          #Oudezijdsvoorburgwal_Z DN_8
                           'DN_TA_9_1_1':'°C','DN_RH_9_1_1':'%',                                    #Saskia van Uylenburgweg DN_9
                           'DN_WS_9_1_1':'m s-1', 'DN_WD_9_1_1':'°', 'DN_WX_9_1_1':'m s-1',         #Saskia van Uylenburgweg DN_9
                           'DN_TA_10_1_1':'°C','DN_RH_10_1_1':'%',                                  #Tweede Oosterparkstraat DN_10
                           'DN_WS_10_1_1':'m s-1', 'DN_WD_10_1_1':'°', 'DN_WX_10_1_1':'m s-1',      #Tweede Oosterparkstraat DN_10
                           'DN_TA_11_1_1':'°C','DN_RH_11_1_1':'%',                                  #Galileiplantsoen DN_11
                           'DN_WS_11_1_1':'m s-1', 'DN_WD_11_1_1':'°', 'DN_WX_11_1_1':'m s-1',      #Galileiplantsoen DN_11
                           'DN_TA_12_1_1':'°C','DN_RH_12_1_1':'%','DN_TG_12_1_1':'°C',              #Saxenburgerdwarsstraat DN_12
                           'DN_WS_12_1_1':'m s-1', 'DN_WD_12_1_1':'°', 'DN_WX_12_1_1':'m s-1',      #Saxenburgerdwarsstraat DN_12
                           'DN_TA_13_1_1':'°C','DN_RH_13_1_1':'%',                                  #Raephaelstraat DN_13
                           'DN_WS_13_1_1':'m s-1', 'DN_WD_13_1_1':'°', 'DN_WX_13_1_1':'m s-1',      #Raephaelstraat DN_13
                           'DN_TA_14_1_1':'°C','DN_RH_14_1_1':'%',                                  #Kinkerstraat DN_14
                           'DN_WS_14_1_1':'m s-1', 'DN_WD_14_1_1':'°', 'DN_WX_14_1_1':'m s-1',      #Kinkerstraat DN_14
                           'DN_TA_15_1_1':'°C','DN_RH_15_1_1':'%','DN_TG_15_1_1':'°C',              #Anjeliersstraat DN_15
                           'DN_WS_15_1_1':'m s-1', 'DN_WD_15_1_1':'°', 'DN_WX_15_1_1':'m s-1',      #Anjeliersstraat DN_15
                           'DN_TA_16_1_1':'°C','DN_RH_16_1_1':'%',                                  #Kattengat DN_16
                           'DN_WS_16_1_1':'m s-1', 'DN_WD_16_1_1':'°', 'DN_WX_16_1_1':'m s-1',      #Kattengat DN_16
                           'DN_TA_17_1_1':'°C','DN_RH_17_1_1':'%','DN_TG_17_1_1':'°C',              #Markengauw DN_17
                           'DN_WS_17_1_1':'m s-1', 'DN_WD_17_1_1':'°', 'DN_WX_17_1_1':'m s-1',      #Markengauw DN_17
                           'DN_TA_18_1_1':'°C','DN_RH_18_1_1':'%',                                  #Purmerweg DN_18
                           'DN_WS_18_1_1':'m s-1', 'DN_WD_18_1_1':'°', 'DN_WX_18_1_1':'m s-1',      #Purmerweg DN_18
                           'DN_TA_19_1_1':'°C','DN_RH_19_1_1':'%',                                  #Zamenhofstraat DN_19
                           'DN_WS_19_1_1':'m s-1', 'DN_WD_19_1_1':'°', 'DN_WX_19_1_1':'m s-1',      #Zamenhofstraat DN_19
                           'DN_TA_20_1_1':'°C','DN_RH_20_1_1':'%',                                  #Scherpenzeelstraat DN_20
                           'DN_WS_20_1_1':'m s-1', 'DN_WD_20_1_1':'°', 'DN_WX_20_1_1':'m s-1',      #Scherpenzeelstraat DN_20
                           'DN_TA_21_1_1':'°C','DN_RH_21_1_1':'%',                                  #Comeniusstraat DN_21
                           'DN_WS_21_1_1':'m s-1', 'DN_WD_21_1_1':'°', 'DN_WX_21_1_1':'m s-1',      #Comeniusstraat DN_21
                           'DN_TA_22_1_1':'°C','DN_RH_22_1_1':'%',                                  #Kwelderweg DN_22
                           'DN_WS_22_1_1':'m s-1', 'DN_WD_22_1_1':'°', 'DN_WX_22_1_1':'m s-1',      #Kwelderweg DN_22
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

    #Only take the variables listed in rename_dict and give them new name (original)
    columns_to_keep = [col for col in rename_dict_distnetw if col in dayfile.columns]
    dayfile = dayfile[columns_to_keep]
    dayfile = dayfile.rename(columns={col: rename_dict_distnetw[col] for col in columns_to_keep})

    try:
        #Load and prepare file (KPN)
        dayfile_kpn = pd.read_csv(datapath_data+'IOT_data_kpn_'+str(t1).replace('-', '')+'.csv',header=0)
        dayfile_kpn.rename(columns={'timestamp': 'TIMESTAMP'}, inplace=True)
        dayfile_kpn['TIMESTAMP'] = pd.to_datetime(dayfile_kpn['TIMESTAMP']).dt.tz_localize(None)
        dayfile_kpn.set_index('TIMESTAMP', inplace=True)
            
        #Only take the variables listed in rename_dict and give them new name (KPN)
        columns_to_keep_kpn = [col for col in rename_dict_distnetw_kpn if col in dayfile_kpn.columns]
        dayfile_kpn = dayfile_kpn[columns_to_keep_kpn]
        dayfile_kpn = dayfile_kpn.rename(columns={col: rename_dict_distnetw_kpn[col] for col in columns_to_keep_kpn})
    except:
        print('No KPN data available')

    #Merge dataframes and reorganize time
    try:
        merged_dayfile = pd.merge(dayfile, dayfile_kpn, left_index=True, right_index=True, how='outer')
    except:
        merged_dayfile = dayfile
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
