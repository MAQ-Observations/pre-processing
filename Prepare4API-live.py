import numpy as np
import pandas as pd
import os
import sys
datapath_toolbox   = os.path.join(<Masked>)
sys.path.insert(1, datapath_toolbox)
from   Loobos_Toolbox_NewTower import *
from   Prepare4API_functions   import *
import warnings
warnings.filterwarnings("ignore")

# Set datapath for output files (subfolder will be added in function)
datapath    = os.path.join(<Masked>)

#--- Define the day to process; by default today and for 1 day, but you can also run it for different days and longer periods
RUN_TODAY       = True
#if RUN_TODAY:   #Run_Today always runs yesterday and today to avoid data loss during day transition (UTC vs local time)
day1        = np.datetime64('today').astype('datetime64[D]') - 1
day2        = day1 + np.timedelta64(2,'D') # exclusive
#else:
#    day1        = np.datetime64('2023-11-17').astype('datetime64[D]')
#    day2        = np.datetime64('2023-11-20').astype('datetime64[D]') + np.timedelta64(1,'D') # exclusive

#--- Select which streams to process
all_streams = ['Veenkampen_Meteo','Veenkampen_Flux','Veenkampen_BC','Veenkampen_PM','Veenkampen_Teledyne','Loobos_BM','Loobos_BM-Backup','Loobos_BM_Soil','Loobos_BM_Precip','Loobos_EC','Loobos_ST','Loobos_LFW','Amsterdam_Rad','Amsterdam_Flux']
do_streams  = all_streams

#--- Loop over all selected days 
days        = np.arange(day1,day2,np.timedelta64(1,'D'))
for t1 in days:
    t2      = t1+np.timedelta64(1,'D')    
    
    #--- Prepare4API_Veenkampen_Meteo
    if 'Veenkampen_Meteo' in do_streams: 
        try: 
            Prepare4API_Veenkampen_Meteo(t1,t2, datapath)
            print('Prepare4API_Veenkampen_Meteo successful')
        except:
            print('Prepare4API_Veenkampen_Meteo failed')

    #--- Prepare4API_Veenkampen_Flux
    if 'Veenkampen_Flux' in do_streams: 
        try: 
            Prepare4API_Veenkampen_Flux(t1,t2, datapath)
            print('Prepare4API_Veenkampen_Flux successful')
        except:
            print('Prepare4API_Veenkampen_Flux failed')
    
    #--- Prepare4API_Veenkampen_BC
    if 'Veenkampen_BC' in do_streams: 
       try: 
           Prepare4API_Veenkampen_BC(t1,t2, datapath)
           print('Prepare4API_Veenkampen_BC successful')
       except:
           print('Prepare4API_Veenkampen_BC failed')
           
    #--- Prepare4API_Veenkampen_PM
    if 'Veenkampen_PM' in do_streams: 
       try: 
           Prepare4API_Veenkampen_PM(t1,t2, datapath)
           print('Prepare4API_Veenkampen_PM successful')
       except:
           print('Prepare4API_Veenkampen_PM failed')

    #--- Prepare4API_Veenkampen_Teledyne
    if 'Veenkampen_Teledyne' in do_streams: 
       try: 
           Prepare4API_Veenkampen_Teledyne(t1,t2, datapath)
           print('Prepare4API_Veenkampen_Teledyne successful')
       except:
           print('Prepare4API_Veenkampen_Teledyne failed')

    #--- Loobos BM
    if 'Loobos_BM' in do_streams: 
        try: 
            Prepare4API_Loobos_BM(t1,t2, datapath)
            print('Prepare4API_Loobos_BM successful')
        except:
            print('Prepare4API_Loobos_BM failed')
    
    #--- Loobos BM-Backup
    if 'Loobos_BM-Backup' in do_streams: 
        try: 
            Prepare4API_Loobos_BM_Backup(t1,t2, datapath)
            print('Prepare4API_Loobos_BM-Backup successful')
        except:
            print('Prepare4API_Loobos_BM-Backup failed')
        
    #--- Loobos BM_Soil
    if 'Loobos_BM_Soil' in do_streams: 
        try: 
            Prepare4API_Loobos_BM_Soil(t1,t2, datapath)
            print('Loobos_BM_Soil successful')
        except:
            print('Loobos_BM_Soil failed')
               
    #--- Loobos EC
    if 'Loobos_EC' in do_streams: 
        try: 
            Prepare4API_Loobos_EC(t1,t2, datapath)
            print('Prepare4API_Loobos_EC successful')
        except:
            print('Prepare4API_Loobos_EC failed')

    #--- Loobos BM Precipitation
    if 'Loobos_BM_Precip' in do_streams: 
        try: 
            Prepare4API_Loobos_BM_Precip(t1,t2, datapath)
            print('Prepare4API_Loobos_BM_Precip successful')
        except:
            print('Prepare4API_Loobos_BM_Precip failed')
    
    #--- Loobos ST
    if 'Loobos_ST' in do_streams: 
        try: 
            Prepare4API_Loobos_ST_Cal(t1,t2, datapath)
            print('Prepare4API_Loobos_ST successful')
        except:
            print('Prepare4API_Loobos_ST failed')

    #--- Loobos LFW
    if 'Loobos_LFW' in do_streams: 
        try: 
            Prepare4API_Loobos_LFW(t1,t2, datapath)
            print('Prepare4API_Loobos_LFW successful')
        except:
            print('Prepare4API_Loobos_LFW failed')
    
    #--- Prepare4API_Amsterdam_Rad
    if 'Amsterdam_Rad' in do_streams: 
        try: 
            Prepare4API_Amsterdam_Rad(t1,t2, datapath)
            print('Prepare4API_Amsterdam_Rad successful')
        except:
            print('Prepare4API_Amsterdam_Rad failed')
            
    #--- Prepare4API_Amsterdam_Flux
    if 'Amsterdam_Flux' in do_streams: 
        try: 
            Prepare4API_Amsterdam_Flux(t1,t2, datapath)
            print('Prepare4API_Amsterdam_Flux successful')
        except:
            print('Prepare4API_Amsterdam_Flux failed')
