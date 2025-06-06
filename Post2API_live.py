import numpy as np
import pandas as pd
import os
from   Post2API_functions   import *

# Set datapath for output files (subfolder will be added in function)
datapath    = os.path.join('W:\\','ESG','DOW_MAQ','MAQ_Archive','MAQ-Observations.nl','data')

#--- Define the day to process; by default yesterday, today. To run it for different days and longer periods use the Post2API_historical.py script.
t           = np.datetime64('today') - np.timedelta64(1,'D')
t1          = t.astype('datetime64[D]')
t2          = t1 + np.timedelta64(1,'D')

#--- Select which streams to process
all_streams = ['VK_Meteo','VK_Flux','VK_BC','VK_PM','VK_Teledyne','VK_BAM','LB_BM','LB_BM_Backup','LB_BM_Precipitation','LB_BM_soil','LB_EC','LB_ST','LB_AQ','LB_LFW','AD_Rad','AD_Flux']
do_streams  = all_streams
do_streams  = ['VK_Meteo','VK_Flux','VK_BC','VK_PM','VK_Teledyne','VK_BAM','LB_BM','LB_BM_Backup','LB_BM_Precipitation','LB_BM_soil','LB_EC','LB_ST','LB_LFW','AD_Rad','AD_Flux']

#Define datapaths
datapath_VK_Meteo     = os.path.join(datapath,'VK_METEO')
datapath_VK_Flux      = os.path.join(datapath,'VK_FLUX')
datapath_VK_BC        = os.path.join(datapath,'VK_BC')
datapath_VK_PM        = os.path.join(datapath,'VK_PM')
datapath_VK_Teledyne  = os.path.join(datapath,'VK_Teledyne')
datapath_VK_BAM       = os.path.join(datapath,'VK_BAM')
datapath_LB_BM        = os.path.join(datapath,'LB_BM')
datapath_LB_BM_Backup = os.path.join(datapath,'LB_BM-Backup')
datapath_LB_BM_Precipitation = os.path.join(datapath,'LB_BM-Precipitation')
datapath_LB_BM_soil   = os.path.join(datapath,'LB_BM-Soil')
datapath_LB_EC        = os.path.join(datapath,'LB_EC')
datapath_LB_ST        = os.path.join(datapath,'LB_ST')
datapath_LB_AQ        = os.path.join(datapath,'LB_AQ')
datapath_LB_LFW       = os.path.join(datapath,'LB_LFW')
datapath_AD_Rad       = os.path.join(datapath,'AD_RAD')
datapath_AD_Flux      = os.path.join(datapath,'AD_FLUX')

#Define end points
END_POINT_VK = '/wp-json/maq/v1/sites/1/stations/1/import'  #End point Veenkampen (site 1, station 1)
END_POINT_LB = '/wp-json/maq/v1/sites/2/stations/2/import'  #End point Loobos  (site 2, station 2)
END_POINT_AD = '/wp-json/maq/v1/sites/3/stations/3/import'  #End point Amsterdam  (site 3, station 3)

if 'VK_Meteo' in do_streams: 
    try: #--- Post2API Veenkampen Meteo
        print('Post2API Veenkampen Meteo started')
        prepare_files(t1,t2,datapath_VK_Meteo,END_POINT_VK,True)
        print('Post2API Veenkampen Meteo successful')
    except:
        print('Post2API Veenkampen Meteo failed')
    
if 'VK_Flux' in do_streams: 
    try: #--- Post2API Veenkampen Flux
        print('Post2API Veenkampen Flux started')
        prepare_files(t1,t2,datapath_VK_Flux,END_POINT_VK,True)
        print('Post2API Veenkampen Flux successful')
    except:
        print('Post2API Veenkampen Flux failed')

if 'VK_BC' in do_streams: 
    try: #--- Post2API Veenkampen BC
        print('Post2API Veenkampen BC started')
        prepare_files(t1,t2,datapath_VK_BC,END_POINT_VK,True)
        print('Post2API Veenkampen BC successful')
    except:
        print('Post2API Veenkampen BC failed')
        
if 'VK_PM' in do_streams: 
    try: #--- Post2API Veenkampen PM
        print('Post2API Veenkampen PM started')
        prepare_files(t1,t2,datapath_VK_PM,END_POINT_VK,True)
        print('Post2API Veenkampen PM successful')
    except:
        print('Post2API Veenkampen PM failed')

if 'VK_Teledyne' in do_streams: 
    try: #--- Post2API Veenkampen Teledyne
        print('Post2API Veenkampen Teledyne started')
        prepare_files(t1,t2,datapath_VK_Teledyne,END_POINT_VK,True)
        print('Post2API Veenkampen Teledyne successful')
    except:
        print('Post2API Veenkampen Teledyne failed')
        
if 'VK_BAM' in do_streams: 
    try: #--- Post2API Veenkampen BAM
        prepare_files(t1,t2,datapath_VK_BAM,END_POINT_VK,False)
        print('Post2API Veenkampen BAM successful')
    except:
        print('Post2API Veenkampen BAM failed')
        
if 'LB_BM' in do_streams: 
    try: #--- Post2API Loobos BM
        print('Post2API Loobos BM started')
        prepare_files(t1,t2,datapath_LB_BM,END_POINT_LB,True)
        print('Post2API Loobos BM successful')
    except:
        print('Post2API Loobos BM failed')

if 'LB_BM_Backup' in do_streams: 
    try: #--- Post2API Loobos BM Backup
        print('Post2API Loobos BM Backup started')
        prepare_files(t1,t2,datapath_LB_BM_Backup,END_POINT_LB,True)
        print('Post2API Loobos BM Backup successful')
    except:
        print('Post2API Loobos BM Backup failed')

if 'LB_BM_Precipitation' in do_streams: 
    try: #--- Post2API Loobos BM Precipitation
        print('Post2API Loobos BM Precipitation started')
        prepare_files(t1,t2,datapath_LB_BM_Precipitation,END_POINT_LB,True)
        print('Post2API Loobos BM Precipitation successful')
    except:
        print('Post2API Loobos BM Precipitation failed')

if 'LB_BM_soil' in do_streams: 
    try: #--- Post2API Loobos BM-soil
        print('Post2API Loobos BM-soil started')
        prepare_files(t1,t2,datapath_LB_BM_soil,END_POINT_LB,True)
        print('Post2API Loobos BM-soil successful')
    except:
        print('Post2API Loobos BM-soil failed')

if 'LB_EC' in do_streams: 
    try: #--- Post2API Loobos EC
        print('Post2API Loobos EC started')
        prepare_files(t1,t2,datapath_LB_EC,END_POINT_LB,True)
        print('Post2API Loobos EC successful')
    except:
        print('Post2API Loobos EC failed')

if 'LB_ST' in do_streams: 
    try: #--- Post2API Loobos ST
        print('Post2API Loobos ST started')
        prepare_files(t1,t2,datapath_LB_ST,END_POINT_LB,True)
        print('Post2API Loobos ST successful')
    except:
        print('Post2API Loobos ST failed')

if 'LB_AQ' in do_streams: 
    try: #--- Post2API Loobos AQ
        print('Post2API Loobos AQ started')
        prepare_files(t1,t2,datapath_LB_AQ,END_POINT_LB,True)
        print('Post2API Loobos AQ successful')
    except:
        print('Post2API Loobos AQ failed')

if 'LB_LFW' in do_streams: 
    try: #--- Post2API Loobos LFW
        print('Post2API Loobos LFW started')
        prepare_files(t1,t2,datapath_LB_LFW,END_POINT_LB,True)
        print('Post2API Loobos LFW successful')
    except:
        print('Post2API Loobos LFW failed')

if 'AD_Rad' in do_streams: 
    try: #--- Post2API Amsterdam Rad
        print('Post2API Amsterdam Rad started')
        prepare_files(t1,t2,datapath_AD_Rad,END_POINT_AD,True)
        print('Post2API Amsterdam Rad successful')
    except:
        print('Post2API Amsterdam Rad failed')

if 'AD_Flux' in do_streams: 
    try: #--- Post2API Amsterdam Flux
        print('Post2API Amsterdam Flux started')
        prepare_files(t1,t2,datapath_AD_Flux,END_POINT_AD,True)
        print('Post2API Amsterdam Flux successful')
    except:
        print('Post2API Amsterdam Flux failed')

