####################################################################################
# ANUClimate automation
# v15.0
# author: Ian Marang
#
# Description:
# script to be run daily, downloads BoM data, compiles a df and subsets it based on
# day and datastream into alpha (daily only), beta (month of days and monthly) and stable
# (month of days and monthly) files.
####################################################################################

# full tested code below:

# import libraries
import ftplib
import decimal
import os, sys
import pandas as pd
import numpy as np
import datetime as dt
from dateutil import relativedelta
import zipfile
import shutil
import fnmatch as fn
import calendar
import time
from tabulate import tabulate
from glob import glob

class ANUClimateAuto_rerun(object):
    def __init__(self):

        self.hostName = 'ftp.bom.gov.au'
        self.hostPath = '/anon2/home/ncc/srds/Scheduled_Jobs/DS082_ANU/'
        self.destPath = '/srv/ANUClimate_auto/download/'
        self.zipPath = '/srv/ANUClimate_auto/unzip/'
        self.logPath = '/srv/ANUClimate_auto/log/'
        self.backupPath = '/srv/ANUClimate_auto/backup/'
        self.nodata = '-99.9'
        self.state = 0
        self.baseDir = '/srv/ANUClimate_auto/processed/fenner'
        # varDict = df col name:[var name for file[0],alpha daily files location[1],beta month of days files location[2], stable month of days files location[3],beta monthly files location[4], stable monthly files location[5]]
        self.varDict = {'Prec_mm':['rain',self.baseDir+'/rain_day_v2_0/alpha/dat/bomdat/',self.baseDir+'/rain_day_v2_0/beta/dat/bomdat/',self.baseDir+'/rain_day_v2_0/stable/dat/bomdat/',self.baseDir+'/rain_mth_v2_0/beta/dat/bomdat/',self.baseDir+'/rain_mth_v2_0/stable/dat/bomdat'],
                        'Evap_mm':['evap',self.baseDir+'/evap_day_v2_0/alpha/dat/bomdat/',self.baseDir+'/evap_day_v2_0/beta/dat/bomdat/',self.baseDir+'/evap_day_v2_0/stable/dat/bomdat/',self.baseDir+'/evap_mth_v2_0/beta/dat/bomdat/',self.baseDir+'/evap_mth_v2_0/stable/dat/bomdat/'],
                        'Tmax_C':['tmax',self.baseDir+'/tmax_day_v2_0/alpha/dat/bomdat/',self.baseDir+'/tmax_day_v2_0/beta/dat/bomdat/',self.baseDir+'/tmax_day_v2_0/stable/dat/bomdat/',self.baseDir+'/tmax_mth_v2_0/beta/dat/bomdat/',self.baseDir+'/tmax_mth_v2_0/stable/dat/bomdat/'],
                        'Tmin_C':['tmin',self.baseDir+'/tmin_day_v2_0/alpha/dat/bomdat/',self.baseDir+'/tmin_day_v2_0/beta/dat/bomdat/',self.baseDir+'/tmin_day_v2_0/stable/dat/bomdat/',self.baseDir+'/tmin_mth_v2_0/beta/dat/bomdat/',self.baseDir+'/tmin_mth_v2_0/stable/dat/bomdat/'],
                        'Vapp_avg_hPa':['vp',self.baseDir+'/vp_day_v2_0/alpha/dat/bomdat/',self.baseDir+'/vp_day_v2_0/beta/dat/bomdat/',self.baseDir+'/vp_day_v2_0/stable/dat/bomdat/',self.baseDir+'/vp_mth_v2_0/beta/dat/bomdat/',self.baseDir+'/vp_mth_v2_0/stable/dat/bomdat/'],
                        'Temp_avg_C':['tavg',self.baseDir+'/tavg_day_v2_0/alpha/dat/bomdat/',self.baseDir+'/tavg_day_v2_0/beta/dat/bomdat/',self.baseDir+'/tavg_day_v2_0/stable/dat/bomdat/',self.baseDir+'/tavg_mth_v2_0/beta/dat/bomdat/',self.baseDir+'/tavg_mth_v2_0/stable/dat/bomdat/'],
                        'Vpd_avg_hPa':['vpd',self.baseDir+'/vpd_day_v2_0/alpha/dat/bomdat/',self.baseDir+'/vpd_day_v2_0/beta/dat/bomdat/',self.baseDir+'/vpd_day_v2_0/stable/dat/bomdat/',self.baseDir+'/vpd_mth_v2_0/beta/dat/bomdat/',self.baseDir+'/vpd_mth_v2_0/stable/dat/bomdat/']}
    


     ####################################################################################
    # UTILITY (prep)
    ####################################################################################

    # code for fixed width files - GENERAL
    def to_fwf(self,df, fname, type):
        '''
        Function to call from within reformat functions to output fixed width file format .dat files for ANUClimate.
        args:
        * df - pandas dataframe
        * fname - output filename
        * type - str of output file type (one of 'normal','pw' or 'fd')
        '''
        if type == 'normal':
            content = tabulate(df, tablefmt="plain",floatfmt=".1f")
        elif type == 'pw':
            content = tabulate(df, tablefmt="plain",floatfmt=".3f")
        open(fname, "w").write(content)
        

    # date string finder
    def getDateStringMissing(self,call=None,fHandle=None):
        '''
        Takes a requested call and returns:
        * a string in correct format to match BoM zip files
        * a lit of date strings for use in outputting required folders

        i.e., DS082_ANUdaily9am3pm<YYYYMMDDDayOfWeek> e.g., DS082_ANUdaily9am3pm20170328Tue

        NB: prefix "DS082_" not included to streamline folder structure

        arg:
        * call: str representing when date should be in relation to today, 3 options:
          - 'alpha' - for alpha product, only produces list with one element (date string to use when reformatting df)
          - 'all' - for all products, produces list with three elements (date strings to use when reformatting df)
        * mod: (optional) int of number of days to process back from current day (for rerun)
        '''
        start = time.time()
        baseStr = 'ANUdaily9am3pm'
        if fHandle:
            dtToday = dt.datetime(int(fHandle[14:18]),int(fHandle[18:20]),int(fHandle[20:22]))
        else:
            dtToday = dt.datetime.today()
        # create fHandle (looking for file from today)
        dateYr = dtToday.isoformat().split('T')[0][:4]
        dateMt = dtToday.isoformat().split('T')[0][5:7]
        dateDy = dtToday.isoformat().split('T')[0][8:10]
        dayOfWeek = calendar.day_name[dtToday.weekday()]
        # create alpha dayDtStr - looking for data from 2 days ago
        alToday = dtToday - relativedelta.relativedelta(days=2)
        dtDateYr = alToday.isoformat().split('T')[0][:4]
        dtDateMt = alToday.isoformat().split('T')[0][5:7]
        dtDateDy = alToday.isoformat().split('T')[0][8:10]
        dayDtStr = dtDateYr+'_'+dtDateMt+'_'+dtDateDy

        if call == 'alpha':
            dateList=[dayDtStr]
            self.state = 0
            comment = 'alpha call'
        elif call == 'all':
            betaDate = dtToday-relativedelta.relativedelta(months=2)
            betaDtStr = betaDate.isoformat().split('T')[0][:4]+'_'+betaDate.isoformat().split('T')[0][5:7]
            stableDate = dtToday-relativedelta.relativedelta(months=6)
            stableDtStr = stableDate.isoformat().split('T')[0][:4]+'_'+stableDate.isoformat().split('T')[0][5:7]
            dateList= [dayDtStr,betaDtStr,stableDtStr]
            self.state = 0
            comment = 'all call'
        else:
            comment = 'Incorrect call arg supplied, must be str "alpha", or "all".'
            self.state = 1

        timing = time.time()-start
        return dateList,timing,self.state,comment;
        
        
    def findMissingDates(self,dateL):
        '''
        Takes list of dates and finds dates with no entry from the start of the log till now
        
        args:
        * dateL - list of dates in log
        
        returns:
        * missing - sorted list of missing dates
        '''
        # set cut off at 6mth mark (BoM files deleted after 6mths)
        threshDate = dt.datetime.today() - relativedelta.relativedelta(months=6)
        if dateL[0]<threshDate:
            startD = threshDate
        else:
            startD = dateL[0]
        date_set = set(startD + dt.timedelta(x) for x in range((dt.datetime.today() - startD).days))
        missing = sorted(date_set - set(dateL))
        return missing;
    

    def downloadFTP(self,fHandle):
        '''
        Connects to BoM ftp site, makes dirs, downloads file and unzips it, writing a list of unzipped files in the backup dir

        args:
        * fHandle - str used from ANUClimateGetDateString func
        
        returns:
        timing, state and comment
        '''
        start = time.time()
        # establish ftp connection and move to correct folder
        ftp = ftplib.FTP(self.hostName)
        ftp.login()
        ftp.cwd(self.hostPath)
        # find correct file on ftp server and create vars for folder names
        fName = 'DS082_'+fHandle+'.zip'
        dirName = self.destPath+fHandle
        zipName = self.zipPath+fHandle
        listName = self.backupPath+fHandle
        # make folders
        try:
            os.makedirs(dirName)
        except:
            pass
        try:
            os.makedirs(zipName)
        except:
            pass
        try:
            os.makedirs(listName)
        except:
            pass
        # create zip file and download
        zFile = open(dirName+'/'+fName,'wb')
        remoteSize = ftp.size(fName)
        ftp.retrbinary('RETR '+fName,zFile.write)
        zFile.close()
        localInfo = os.stat(dirName+'/'+fName)
        if str(localInfo.st_size)!=str(remoteSize):
            self.state = 1
            comment = 'Size check failed'
        else:
            pass
        # move to download folder and extract to unzip folder
        os.chdir(dirName)
        try:
            zipFile = zipfile.ZipFile(dirName+'/'+fName,'r')
            zipFile.extractall(zipName)
        except Exception as e:
            self.state = 1
            comment = 'Unzip failed '+str(e)
        # make a list of all downloaded files
        f = open(listName+'/'+fHandle+'.txt','w')
        for fname in zipFile.namelist():
            f.write(fname+',\n')
        f.close()
        # set state
        self.state = 0
        comment = 'FTP success for '+str(fHandle)
        timing = time.time() - start
        return timing,self.state,comment;


    def daysInMth(self,yrIn,mthIn):
        '''
        Function to check for number of days in month
        
        args:
        * yrIn - int of current year
        * mthIn - int of current month
        * dyIn - int of current day
        
        returns:
        daysInMth - int of number of days in month
        '''
        mthDays = {31:['01','03','05','07','08','10','12'],30:['04','06','09','11'],28:['02']}
        mthDaysLeap = {31:['01','03','05','07','08','10','12'],30:['04','06','09','11'],29:['02']}
    
        date = dt.datetime(yrIn,mthIn,1)
        mth = date.isoformat().split('T')[0][5:7]
        yr = date.isoformat().split('T')[0][:4]
    
    
        if calendar.isleap(int(yr)):
            for var,varDetails in mthDaysLeap.items():
                if mth in set(varDetails):
                    daysInMth = var
        else:
            for var,varDetails in mthDays.items():
                if mth in set(varDetails):
                    daysInMth = var
    
        return daysInMth;

    ####################################################################################
    # FILE OPEN
    ####################################################################################

    # df compiler
    # testing dropping comparison to see why some rows are dropped from final df
    def fileOpen(self,fname,dtStr,dStream):
        '''
        Daily operations function for use with cron job to store daily bomdat input files

        - drops rows based on quality flags and accumulated days into a dfRemove 
        (currently not stored) and dfKeep for use in subsequent reformatting into the daily files
        --- rows with quality flags 'Y' or 'N' kept, anything else dropped
        --- rows with accumulated days > 1 is dropped

        Args:
        fname - a string representing the path/filename to open and process
        '''

        # set col parameters and import unzipped test file
        usecols = ['Station Number',
               'Year',
               'Month',
               'Day',
               'Precipitation in the 24 hours before 9am (local time) in mm',
               'Quality of precipitation value',
               'Number of days of rain within the days of accumulation',
               'Accumulated number of days over which the precipitation was measured',
               'Evaporation in 24 hours before 9am (local time) in mm',
               'Quality of evaporation in 24 hours before 9am (local time)',
               'Days of accumulation for evaporation',
               'Maximum temperature in 24 hours after 9am (local time) in Degrees C',
               'Quality of maximum temperature in 24 hours after 9am (local time)',
               'Days of accumulation of maximum temperature',
               'Minimum temperature in 24 hours before 9am (local time) in Degrees C',
               'Quality of minimum temperature in 24 hours before 9am (local time)',
               'Days of accumulation of minimum temperature',
               'Air temperature observation at 09 hours Local Time in Degrees C',
               'Quality of air temperature observation at 09 hours Local Time',
               'Air temperature observation at 15 hours Local Time in Degrees C',
               'Quality of air temperature observation at 15 hours Local Time',
               'Dew point temperature observation at 09 hours Local Time in Degrees C',
               'Quality of dew point temperature observation at 09 hours Local Time',
               'Dew point temperature observation at 15 hours Local Time in Degrees C',
               'Quality of dew point temperature observation at 15 hours Local Time',
               'Wet bulb temperature observation at 09 hours Local Time in Degrees C',
               'Quality of wet bulb temperature observation at 09 hours Local Time',
               'Wet bulb temperature observation at 15 hours Local Time in Degrees C',
               'Quality of wet bulb temperature observation at 15 hours Local Time',
               'Relative humidity for observation at 09 hours Local Time in percentage %',
               'Quality of relative humidity for observation at 09 hours Local Time',
               'Relative humidity for observation at 15 hours Local Time in percentage %',
               'Quality of relative humidity for observation at 15 hours Local Time',
               'Vapour pressure at 09 hours Local Time in hPa',
               'Quality of vapour pressure at 09 hours Local Time',
               'Vapour pressure at 15 hours Local Time in hPa',
               'Quality of vapour pressure at 15 hours Local Time',
               'Saturated vapour pressure at 09 hours in hPa',
               'Quality of saturated vapour pressure at 09 hours Local Time',
               'Saturated vapour pressure at 15 hours in hPa',
               'Quality of saturated vapour pressure at 15 hours Local Time']


        headers = ['Station_ID',
                   'Year',
                   'Month',
                   'Day',
                   'Prec_mm',# Prec = Precipitation
                   'Prec_Quality',
                   'Prec_Days_of_Rain_within_Accumulation',
                   'Prec_Accumulated_Days',
                   'Evap_mm', # Evap = Evaporation
                   'Evap_Quality',
                   'Evap_Accumulated_Days',
                   'Tmax_C', # Tmax = Max Temperature
                   'Tmax_Quality',
                   'Tmax_Accumulated_Days',
                   'Tmin_C', # Tmin = Min Temperature
                   'Tmin_Quality',
                   'Tmin_Accumulated_Days',
                   'Temp_9am_C', # Temp = Air Temperature
                   'Temp_9am_Quality',
                   'Temp_3pm_C',
                   'Temp_3pm_Quality',
                   'DewP_9am_C', # DewP = Dew Point temperature
                   'DewP_9am_Quality',
                   'DewP_3pm_C',
                   'DewP_3pm_Quality',
                   'WetB_9am_C', # WetB = Wet Bulb temperature
                   'WetB_9am_Quality',
                   'WetB_3pm_C',
                   'WetB_3pm_Quality',
                   'RelH_9am_%', # RelH = Relative Humidity
                   'RelH_9am_Quality',
                   'RelH_3pm_%',
                   'RelH_3pm_Quality',
                   'Vapp_9am_hPa', # Vapp = Vapour Pressure
                   'Vapp_9am_Quality',
                   'Vapp_3pm_hPa',
                   'Vapp_3pm_Quality',
                   'SVap_9am_hPa', # SVap = Saturated Vapour pressure
                   'SVap_9am_Quality',
                   'SVap_3pm_hPa',
                   'SVap_3pm_Quality']

        nodata = '-99.9'
        dfIn = pd.read_table(fname,sep=',',usecols=usecols)
        dfIn.columns = headers
        dfIn.Station_ID = [str(x).zfill(6) for x in dfIn.Station_ID]

        # from http://stackoverflow.com/questions/13445241/replacing-blank-values-white-space-with-nan-in-pandas
        dfIn = dfIn.applymap(lambda x: np.nan if isinstance(x, basestring) and x.isspace() else x)

        # strip extra whitespace
        dfIn = dfIn.applymap(lambda x: x.strip() if isinstance(x, basestring) else x)
        
        # fix type on year & month
        dfIn.Day = [str(x).zfill(2) for x in dfIn.Day]
        dfIn.Month = [str(x).zfill(2) for x in dfIn.Month]
        dfIn.Year = [str(x) for x in dfIn.Year]
        
        # only propagate df for dStream and date
        if dStream == 'alpha':
            df = dfIn.loc[(dfIn.Year==dtStr.split('_')[0])&(dfIn.Month==dtStr.split('_')[1])&(dfIn.Day==dtStr.split('_')[-1])]
        else:
            df = dfIn.loc[(dfIn.Year==dtStr.split('_')[0])&(dfIn.Month==dtStr.split('_')[1])]
            if df.Day.min()!='01' and df.index.size!=0:
                station = df.Station_ID.iloc[0]
                dayList = range(1,int(df.Day.min()))
                dayList = [str(x).zfill(2) for x in dayList]
                a = {'Station_ID':[station]*len(dayList),
                      'Year':[dtStr.split('_')[0]]*len(dayList),
                      'Month':[dtStr.split('_')[1]]*len(dayList),
                      'Day':dayList,
                     'Prec_mm':[np.nan]*len(dayList),
                     'Prec_Days_of_Rain_within_Accumulation':[np.nan]*len(dayList),
                     'Prec_Quality':[np.nan]*len(dayList),
                     'Prec_Accumulated_Days':[np.nan]*len(dayList),
                     'Evap_mm':[np.nan]*len(dayList),
                     'Evap_Quality':[np.nan]*len(dayList),
                     'Evap_Accumulated_Days':[np.nan]*len(dayList),
                     'Tmax_C':[np.nan]*len(dayList),
                     'Tmax_Quality':[np.nan]*len(dayList),
                     'Tmax_Accumulated_Days':[np.nan]*len(dayList),
                     'Tmin_C':[np.nan]*len(dayList),
                     'Tmin_Quality':[np.nan]*len(dayList),
                     'Tmin_Accumulated_Days':[np.nan]*len(dayList),
                     'Temp_9am_C':[np.nan]*len(dayList),
                     'Temp_9am_Quality':[np.nan]*len(dayList),
                     'Temp_3pm_C':[np.nan]*len(dayList),
                     'Temp_3pm_Quality':[np.nan]*len(dayList),
                     'DewP_9am_C':[np.nan]*len(dayList),
                     'DewP_9am_Quality':[np.nan]*len(dayList),
                     'DewP_3pm_C':[np.nan]*len(dayList),
                     'DewP_3pm_Quality':[np.nan]*len(dayList),
                     'WetB_9am_C':[np.nan]*len(dayList),
                     'WetB_9am_Quality':[np.nan]*len(dayList),
                     'WetB_3pm_C':[np.nan]*len(dayList),
                     'WetB_3pm_Quality':[np.nan]*len(dayList),
                     'RelH_9am_%':[np.nan]*len(dayList),
                     'RelH_9am_Quality':[np.nan]*len(dayList),
                     'RelH_3pm_%':[np.nan]*len(dayList),
                     'RelH_3pm_Quality':[np.nan]*len(dayList),
                     'Vapp_9am_hPa':[np.nan]*len(dayList),
                     'Vapp_9am_Quality':[np.nan]*len(dayList),
                     'Vapp_3pm_hPa':[np.nan]*len(dayList),
                     'Vapp_3pm_Quality':[np.nan]*len(dayList),
                     'SVap_9am_hPa':[np.nan]*len(dayList),
                     'SVap_9am_Quality':[np.nan]*len(dayList),
                     'SVap_3pm_hPa':[np.nan]*len(dayList),
                     'SVap_3pm_Quality':[np.nan]*len(dayList)}

                newdf = pd.DataFrame.from_dict(a)
                df = df.append(newdf)
        
        del dfIn
        #print 'second index size: '+str(df.index.size)
        # fix Quality
        # convert fields to str for removal comparison operation
        df.Prec_Quality = [str(x) for x in df.Prec_Quality]
        df.Evap_Quality = [str(x) for x in df.Evap_Quality]
        df.Tmax_Quality = [str(x) for x in df.Tmax_Quality]
        df.Tmin_Quality = [str(x) for x in df.Tmin_Quality]
        df.Temp_9am_Quality = [str(x) for x in df.Temp_9am_Quality]
        df.Temp_3pm_Quality = [str(x) for x in df.Temp_3pm_Quality]
        df.DewP_9am_Quality = [str(x) for x in df.DewP_9am_Quality]
        df.DewP_3pm_Quality = [str(x) for x in df.DewP_3pm_Quality]
        df.WetB_9am_Quality = [str(x) for x in df.WetB_9am_Quality]
        df.WetB_3pm_Quality = [str(x) for x in df.WetB_3pm_Quality]
        df.RelH_9am_Quality = [str(x) for x in df.RelH_9am_Quality]
        df.RelH_3pm_Quality = [str(x) for x in df.RelH_3pm_Quality]
        df.Vapp_9am_Quality = [str(x) for x in df.Vapp_9am_Quality]
        df.Vapp_3pm_Quality = [str(x) for x in df.Vapp_3pm_Quality]
        df.SVap_9am_Quality = [str(x) for x in df.SVap_9am_Quality]
        df.SVap_3pm_Quality = [str(x) for x in df.SVap_3pm_Quality]

        # fix data fields (testing Assign rather than loop)
        df.Prec_mm = [str(x) for x in df.Prec_mm]
        df.Evap_mm = [str(x) for x in df.Evap_mm]
        df.Tmax_C = [str(x) for x in df.Tmax_C]
        df.Tmin_C = [str(x) for x in df.Tmin_C]
        df.Vapp_9am_hPa = [str(x) for x in df.Vapp_9am_hPa]
        df.Vapp_3pm_hPa = [str(x) for x in df.Vapp_3pm_hPa]
        df.SVap_9am_hPa = [str(x) for x in df.SVap_9am_hPa]
        df.SVap_3pm_hPa = [str(x) for x in df.SVap_3pm_hPa]
        
        
        # fix data fields (testing Assign rather than loop)
        df.Prec_mm = [np.nan if len(x.strip())==0 or x!=x or x == 'nan' else float(x) for x in df.Prec_mm]
        df.Evap_mm = [np.nan if len(x.strip())==0 or x!=x or x == 'nan' else float(x) for x in df.Evap_mm]
        df.Tmax_C = [np.nan if len(x.strip())==0 or x!=x or x == 'nan' else float(x) for x in df.Tmax_C]
        df.Tmin_C = [np.nan if len(x.strip())==0 or x!=x or x == 'nan' else float(x) for x in df.Tmin_C]
        df.Vapp_9am_hPa = [np.nan if len(x.strip())==0 or x!=x or x == 'nan' else float(x) for x in df.Vapp_9am_hPa]
        df.Vapp_3pm_hPa = [np.nan if len(x.strip())==0 or x!=x or x == 'nan' else float(x) for x in df.Vapp_3pm_hPa]
        df.SVap_9am_hPa = [np.nan if len(x.strip())==0 or x!=x or x == 'nan' else float(x) for x in df.SVap_9am_hPa]
        df.SVap_3pm_hPa = [np.nan if len(x.strip())==0 or x!=x or x == 'nan' else float(x) for x in df.SVap_3pm_hPa]
        
        
        # convert string nan to N for empty Quality flag cells
        df.loc[(df.Prec_mm.notnull()) & (df.Prec_Quality.isnull()), 'Prec_Quality'] = 'N'
        df.loc[(df.Evap_mm.notnull()) & (df.Evap_Quality.isnull()), 'Evap_Quality'] = 'N'
        df.loc[(df.Tmax_C.notnull()) & (df.Tmax_Quality.isnull()), 'Tmax_Quality'] = 'N'
        df.loc[(df.Tmin_C.notnull()) & (df.Tmin_Quality.isnull()), 'Tmin_Quality'] = 'N'
        df.loc[(df.Temp_9am_C.notnull()) & (df.Temp_9am_Quality.isnull()), 'Temp_9am_Quality'] = 'N'
        df.loc[(df.Temp_3pm_C.notnull()) & (df.Temp_3pm_Quality.isnull()), 'Temp_3pm_Quality'] = 'N'
        df.loc[(df.DewP_9am_C.notnull()) & (df.DewP_9am_Quality.isnull()), 'DewP_9am_Quality'] = 'N'
        df.loc[(df.DewP_3pm_C.notnull()) & (df.DewP_3pm_Quality.isnull()), 'DewP_3pm_Quality'] = 'N'
        df.loc[(df.WetB_9am_C.notnull()) & (df.WetB_9am_Quality.isnull()), 'WetB_9am_Quality'] = 'N'
        df.loc[(df.WetB_3pm_C.notnull()) & (df.WetB_3pm_Quality.isnull()), 'WetB_3pm_Quality'] = 'N'
        df.loc[(df['RelH_9am_%'].notnull()) & (df.RelH_9am_Quality.isnull()), 'RelH_9am_Quality'] = 'N'
        df.loc[(df['RelH_3pm_%'].notnull()) & (df.RelH_3pm_Quality.isnull()), 'RelH_3pm_Quality'] = 'N'
        df.loc[(df.Vapp_3pm_hPa.notnull()) & (df.Vapp_9am_Quality.isnull()), 'Vapp_9am_Quality'] = 'N'
        df.loc[(df.Vapp_3pm_hPa.notnull()) & (df.Vapp_3pm_Quality.isnull()), 'Vapp_3pm_Quality'] = 'N'
        df.loc[(df.SVap_3pm_hPa.notnull()) & (df.SVap_9am_Quality.isnull()), 'SVap_9am_Quality'] = 'N'
        df.loc[(df.SVap_3pm_hPa.notnull()) & (df.SVap_3pm_Quality.isnull()), 'SVap_3pm_Quality'] = 'N'
        
        
        # discrepancy with station 14955 Prec_accumm_day = 1 tho prec_mm empty
        df.Prec_Accumulated_Days = df.Prec_Accumulated_Days.apply(lambda x: np.nan if isinstance(x, basestring) and x.isspace() else x)
        
        # fix Accum days
        # convert field to str for removal comparison operation
        df.Prec_Accumulated_Days = [str(x) for x in df.Prec_Accumulated_Days]
        df.Evap_Accumulated_Days = [str(x) for x in df.Evap_Accumulated_Days]
        df.Tmax_Accumulated_Days = [str(x) for x in df.Tmax_Accumulated_Days]
        df.Tmin_Accumulated_Days = [str(x) for x in df.Tmin_Accumulated_Days]
        
        # make all accum flags numeric
        df.Prec_Accumulated_Days = [0 if x!=x or x == 'nan' else int(float(x)) for x in df.Prec_Accumulated_Days]
        df.Evap_Accumulated_Days = [0 if x!=x or x == 'nan' else int(float(x)) for x in df.Evap_Accumulated_Days]
        df.Tmax_Accumulated_Days = [0 if x!=x or x == 'nan' else int(float(x)) for x in df.Tmax_Accumulated_Days]
        df.Tmin_Accumulated_Days = [0 if x!=x or x == 'nan' else int(float(x)) for x in df.Tmin_Accumulated_Days]

        
        # convert nan to 1 for zero Accumulated days if var is not null
        df.loc[(df.Prec_mm.notnull()) & (df.Prec_Accumulated_Days==0),'Prec_Accumulated_Days'] = 1
        df.loc[(df.Evap_mm.notnull()) & (df.Evap_Accumulated_Days==0),'Evap_Accumulated_Days'] = 1
        
        # mark rows with empty prec_mm but accum flag = 1 as accum flag = 0
        df.loc[(df.Prec_mm.isnull()) & (df.Prec_Accumulated_Days==1),'Prec_Accumulated_Days'] = 0
        
        # mark temp accum days flag > 1 as np.nan
        #df.loc[df.Tmax_Accumulated_Days>1,'Tmax_C']=np.nan
        #df.loc[df.Tmin_Accumulated_Days>1,'Tmin_C']=np.nan
        
        # find rows with quality NOT == ('Y','N') or Accumulated_days>1 replace
        okQual = ['Y','N']

        # quality rows
        df.loc[~df.Prec_Quality.isin(okQual),'Prec_mm']=np.nan
        df.loc[~df.Evap_Quality.isin(okQual),'Evap_mm']=np.nan
        df.loc[~df.Tmax_Quality.isin(okQual),'Tmax_C']=np.nan
        df.loc[~df.Tmin_Quality.isin(okQual),'Tmin_C']=np.nan
        df.loc[~df.Temp_9am_Quality.isin(okQual),'Temp_9am_C']=np.nan
        df.loc[~df.Temp_3pm_Quality.isin(okQual),'Temp_3pm_C']=np.nan
        df.loc[~df.DewP_9am_Quality.isin(okQual),'DewP_9am_C']=np.nan
        df.loc[~df.DewP_3pm_Quality.isin(okQual),'DewP_3pm_C']=np.nan
        df.loc[~df.WetB_9am_Quality.isin(okQual),'WetB_9am_C']=np.nan
        df.loc[~df.WetB_3pm_Quality.isin(okQual),'WetB_3pm_C']=np.nan
        df.loc[~df.RelH_9am_Quality.isin(okQual),'RelH_9am_%']=np.nan
        df.loc[~df.RelH_3pm_Quality.isin(okQual),'RelH_3pm_%']=np.nan
        df.loc[~df.Vapp_9am_Quality.isin(okQual),'Vapp_9am_hPa']=np.nan
        df.loc[~df.Vapp_3pm_Quality.isin(okQual),'Vapp_3pm_hPa']=np.nan
        df.loc[~df.SVap_9am_Quality.isin(okQual),'SVap_9am_hPa']=np.nan
        df.loc[~df.SVap_3pm_Quality.isin(okQual),'SVap_3pm_hPa']=np.nan


        # add cols for Tavg, Vavg, Year_Month (for output files) and index (for cross ref)
        df = df.assign(Temp_avg_C=(df.Tmax_C+df.Tmin_C)/2.,Vapp_avg_hPa=(df.Vapp_9am_hPa+df.Vapp_3pm_hPa)/2.,SVap_avg_hPa=(df.SVap_9am_hPa+df.SVap_3pm_hPa)/2.)
        df = df.assign(Vpd_avg_hPa=df.SVap_avg_hPa-df.Vapp_avg_hPa)
        df['Year_Month'] = df[['Year','Month']].apply(lambda x: '_'.join(x),axis=1)
        df['primary_key'] = df[['Station_ID','Year','Month','Day']].apply(lambda x: '_'.join(x),axis=1)
        
        
        # adjust cols for month of days file output
        df.drop(['Prec_Quality',
                 'Evap_Quality',
                 'Tmax_Quality',
                 'Tmax_Accumulated_Days',
                 'Tmin_Quality',
                 'Tmin_Accumulated_Days',
                 'Temp_9am_C',
                 'Temp_9am_Quality',
                 'Temp_3pm_C',
                 'Temp_3pm_Quality',
                 'DewP_9am_C',
                 'DewP_9am_Quality',
                 'DewP_3pm_C',
                 'DewP_3pm_Quality',
                 'WetB_9am_C',
                 'WetB_9am_Quality',
                 'WetB_3pm_C',
                 'WetB_3pm_Quality',
                 'RelH_9am_%',
                 'RelH_9am_Quality',
                 'RelH_3pm_%',
                 'RelH_3pm_Quality',
                 'Vapp_9am_hPa',
                 'Vapp_9am_Quality',
                 'Vapp_3pm_hPa',
                 'Vapp_3pm_Quality',
                 'SVap_9am_hPa',
                 'SVap_9am_Quality',
                 'SVap_3pm_hPa',
                 'SVap_3pm_Quality'],inplace=True,axis=1)
        
        df.set_index(['primary_key'],inplace=True)
        df.sort_index(inplace=True)
        return df;


    ####################################################################################
    # REFORMAT to output
    ####################################################################################

    
    def getData(self,df,var,station):
        '''
        getData function - used within reformatDaily and reformatMonthOfDays functions to subset df and create dfOut
        
        args:
        * df - input dataframe from BoM data
        * var - str of one of desired BoM variables
        * station - str of BoM station
        
        returns:
        * dfOut - a reformatted df for use in .dat files
        '''
        data = df[var].loc[df.Station_ID == station].values
        station = str(station).zfill(6)
        dfOut = pd.DataFrame({station:[x for x in data]},dtype=str)
        return dfOut.T;
    
    
    def getMthData(self,df,var,station,numDays):
        '''
        getMthData function - used within reformatMonthly function to subset df and create dfOut
        
        args:
        * df - input dataframe from BoM data
        * var - str of one of desired BoM variables
        * station - str of BoM station
        
        returns:
        * dfOut - a reformatted df for use in .dat files
        '''
        data = df[var].loc[df.Station_ID == station].values
        data1 = np.asarray(data)
        data1 = [np.nan if x!=x else float(x) for x in data1]
        # count number of NaN
        numNonNan = df[var].loc[df.Station_ID == station].notnull().sum()
        # split processing based on MH email into prec/evap and rest
        # prec/evap already have checks in place, evap needs min 20 days, prec has no day requirement
        if var == 'Prec_mm':
            accumTotP = df.Prec_Accumulated_Days.loc[df.Station_ID==station].sum()
            lastDayFlag = df.Prec_Accumulated_Days.loc[df.Station_ID==station].tail(1)
            if accumTotP!=accumTotP:
                pass
            else:
                if float(accumTotP)!=float(numDays):
                    dataOut = np.nan
                else:
                    if float(lastDayFlag) >= 1.:
                        dataOut=np.nansum(data1)
                    else:
                        dataOut = np.nan
        elif var in ('Tmax_C','Tmin_C','Vapp_avg_hPa','Temp_avg_C','Vpd_avg_hPa','Evap_mm'):# proceed if number of nonNaNs >=25
            if numNonNan >= 25:
                if var == 'Evap_mm':
                    accumTotE = df.Evap_Accumulated_Days.loc[df.Station_ID==station].sum()
                    if accumTotE!=accumTotE:
                        pass
                    else:
                        if float(accumTotE)<25. or float(accumTotE)>35.:
                            dataOut=np.nan
                        else:
                            dataOut=(np.nansum(data1)/float(accumTotE))*float(numDays)
                else:
                    dataOut=np.nanmean(data1)
            else:
                dataOut = np.nan
        # force station to string
        station = str(station).zfill(6)
        dfOut = pd.DataFrame({station:[dataOut]},dtype=str)
        return dfOut.T;
    
      
    def getPWdata(self,df,station,numDays):
        '''
        getPWData function - used within reformatMonthly functions to subset df and create dfOut of PW
        
        args:
        * df - input dataframe from BoM data
        * var - str of one of desired BoM variables
        * station - str of BoM station
        
        returns:
        * dfOut - a reformatted df for use in .dat files
        '''
        # count number of NaN
        numNoNNan = df.Prec_mm.loc[df.Station_ID==station].notnull().sum()
        # proceed if number of nonNaNs >=25
        if numNoNNan>=25:
            # count number of rain days
            numRain = df.Prec_mm.loc[(df.Station_ID==station)&(df.Prec_mm>0.2)].index.size
            try:
                # calc proportion of wet days - scaled by valid days in month
                pw = np.round((float(numRain)/float(numNoNNan))*numDays,3)
            except:
                pass
        else:
            pw = np.nan
        # create new df for PW
        station = str(station).zfill(6)
        dfOut = pd.DataFrame({station:[pw]},dtype=np.float64)
        return dfOut.T;
        
    def getFDdata(self,df,station,numDays):
        '''
        getFDData function - used within reformatMonthly functions to subset df and create dfOut of frost days
        
        args:
        * df - input dataframe from BoM data
        * var - str of one of desired BoM variables
        * station - str of BoM station
        
        returns:
        * dfOut - a reformatted df for use in .dat files
        '''
        # count number of NaN
        numNoNNan = df.Tmin_C.loc[df.Station_ID==station].notnull().sum()
        # proceed if number of nonNaNs >=25
        fdays = np.nan
        if numNoNNan>=25:
            # count number of frost days
            frostDays = df.Tmin_C.loc[(df.Station_ID==station)&(df.Tmin_C<=2.)].index.size
            # calc frost days with ratio of frost to valid days
            fdays = float(numDays)*(float(frostDays)/float(numNoNNan))
        else:
            fdays = np.nan
        # create new df for FD
        station = str(station).zfill(6)
        dfOut = pd.DataFrame({station:[fdays]},dtype=np.float64)
        return dfOut.T;
    
    
    # reformat Daily
    def reFormatDaily(self,df,dtStr):
        start = time.time()
        # adjust cols for daily file output
        df = df.assign(Year_Month_Day=str(df.Year)+'_'+str(df.Month)+'_'+str(df.Day)) 
        
        # find unique Station_IDs and convert to list for iterating over
        setStation_ID = set(df.Station_ID)
        trueSetSI = {x for x in setStation_ID if x==x}
        listSI = sorted(list(trueSetSI))

        dfGroupD = df.set_index('Year_Month_Day',drop=True)
        # iterate over list to create year/month files for each variable
        for var,varDetails in self.varDict.iteritems():
            frames = [self.getData(dfGroupD,var,station) for station in listSI]
            if len(frames)!=0:
                dfGroup1a = pd.concat(frames)
            
                # drop empty rows before adding year and month cols
                dfGroup1a.dropna(inplace=True,axis=0,how='all')
                # make year and month lists
                yList = [dtStr.split('_')[0]]*dfGroup1a.index.size
                mList = [dtStr.split('_')[1]]*dfGroup1a.index.size
                dList = [dtStr.split('_')[-1]]*dfGroup1a.index.size
                # insert cols
                dfGroup1a.insert(loc=0,column='year',value=yList)
                dfGroup1a.insert(loc=1,column='month',value=mList)
                dfGroup1a.insert(loc=2,column='day',value=dList)
                # change nans to -99.9
                dfGroup1a = dfGroup1a.applymap(lambda x: '-99.9' if x!=x else x)
                if len(dfGroup1a.index) !=0 and isinstance(dfGroup1a, pd.DataFrame):
                    self.to_fwf(dfGroup1a,varDetails[1]+varDetails[0]+'_'+dtStr+'.dat','normal')
            
        timing = time.time() - start
        self.state = 0
        comment = 'reformatDaily complete'
        return timing,self.state,comment;


    # reformat month of days
    def reFormatMonthOfDays(self,df,dtStr,dataStream):
        start = time.time()

        # find unique Station_IDs and convert to list for iterating over
        setStation_ID = set(df.Station_ID)
        trueSetSI = {x for x in setStation_ID if x==x}
        listSI = sorted(list(trueSetSI))
        
        # iterate over dict to create year/month files for each variable
        for var,varDetails in self.varDict.iteritems():
            frames = [self.getData(df,var,station) for station in listSI]
            if len(frames)!=0:
                dfGroup1a = pd.concat(frames)
                # drop empty rows before adding year and month cols
                dfGroup1a.dropna(inplace=True,axis=0,how='all')
                #print dfGroup1a
                # make year and month lists
                yList = [dtStr.split('_')[0]]*dfGroup1a.index.size
                mList = [dtStr.split('_')[-1]]*dfGroup1a.index.size
                # insert cols
                dfGroup1a.insert(loc=0,column='year',value=yList)
                dfGroup1a.insert(loc=1,column='month',value=mList)
                # change nans to -99.9
                dfGroup1a = dfGroup1a.applymap(lambda x: '-99.9' if x!=x else x)
                if dataStream=='beta':
                    self.to_fwf(dfGroup1a,varDetails[2]+varDetails[0]+'_'+dtStr+'.dat','normal')
                if dataStream=='stable':
                    self.to_fwf(dfGroup1a,varDetails[3]+varDetails[0]+'_'+dtStr+'.dat','normal')
        timing = time.time() - start
        self.state = 0
        comment = 'reformatMonthOfDays complete'
        return timing,self.state,comment;


    # reformat monthly file
    def reFormatMonthly(self,df,dtStr,dataStream):
        start = time.time()
        
        # find unique Station_IDs and convert to list for iterating over
        setStation_ID = set(df.Station_ID)
        trueSetSI = {x for x in setStation_ID if x==x}
        listSI = sorted(list(trueSetSI))
        
        # reformat Prec_mm col
        df.Prec_mm = [np.nan if x!=x else float(x) for x in df.Prec_mm]
        
        # find numDays var for total accum days check and pw calc
        numDays = self.daysInMth(int(dtStr.split('_')[0]),int(dtStr.split('_')[-1]))
        

        ##### Proportion of days that are wet
        #cycle through stations
        frames = [self.getPWdata(df,station,numDays) for station in listSI]
        if len(frames)!=0:
            dfGroupPW = pd.concat(frames)
            # drop empty rows before adding year and month cols
            dfGroupPW.dropna(inplace=True,axis=0,how='all')
            # make year and month lists
            yList = [dtStr.split('_')[0]]*dfGroupPW.index.size
            mList = [dtStr.split('_')[-1]]*dfGroupPW.index.size
            # insert cols
            dfGroupPW.insert(loc=0,column='year',value=yList)
            dfGroupPW.insert(loc=1,column='month',value=mList)
    
            # change nans to -99.9
            dfGroupPW = dfGroupPW.applymap(lambda x: '-99.9' if x!=x else x)
            if dataStream=='beta':
                self.to_fwf(dfGroupPW,self.baseDir+'/pw_mth_v2_0/beta/dat/bomdat/pw_'+dtStr+'.dat','pw')
            if dataStream=='stable':
                self.to_fwf(dfGroupPW,self.baseDir+'/pw_mth_v2_0/stable/dat/bomdat/pw_'+dtStr+'.dat','pw') 
        
        del dfGroupPW
        
        ##### Number of Frost Days
        #cycle through stations
        frames = [self.getFDdata(df,station,numDays) for station in listSI]
        if len(frames)!=0:
            dfGroupFD = pd.concat(frames)
            # drop empty rows before adding year and month cols
            dfGroupFD.dropna(inplace=True,axis=0,how='all')
            # make year and month lists
            yList = [dtStr.split('_')[0]]*dfGroupFD.index.size
            mList = [dtStr.split('_')[-1]]*dfGroupFD.index.size
            # insert cols
            dfGroupFD.insert(loc=0,column='year',value=yList)
            dfGroupFD.insert(loc=1,column='month',value=mList)
    
            # change nans to -99.9
            dfGroupFD = dfGroupFD.applymap(lambda x: '-99.9' if x!=x else x)
            if dataStream=='beta':
                self.to_fwf(dfGroupFD,self.baseDir+'/frst_mth_v2_0/beta/dat/bomdat/fd_'+dtStr+'.dat','normal')
            if dataStream=='stable':
                self.to_fwf(dfGroupFD,self.baseDir+'/frst_mth_v2_0/stable/dat/bomdat/fd_'+dtStr+'.dat','normal')
    
        del dfGroupFD
        
    
        # iterate over list to create year/month files for each variable
        for var,varDetails in self.varDict.iteritems():
            frames = [self.getMthData(df,var,station,numDays) for station in listSI]
            if len(frames)!=0:
                dfGroup1a = pd.concat(frames)

                # convert '0.0' to nan for dropping for Evap_mm
                if var == 'Evap_mm':
                    dfGroup1a = dfGroup1a.applymap(lambda x: np.nan if x in ('0.0',' 0.0 ',' 0.0','0.0 ','0',0.,0) else x)
                # drop empty rows before adding year and month cols
                dfGroup1a.dropna(inplace=True,axis=0,how='all')
                # make year and month lists
                yList = [dtStr.split('_')[0]]*dfGroup1a.index.size
                mList = [dtStr.split('_')[-1]]*dfGroup1a.index.size
                # insert cols
                dfGroup1a.insert(loc=0,column='year',value=yList)
                dfGroup1a.insert(loc=1,column='month',value=mList)
                # change nans to -99.9
                dfGroup1a = dfGroup1a.applymap(lambda x: '-99.9' if x!=x else x)
                if dataStream=='beta':
                    self.to_fwf(dfGroup1a,varDetails[4]+varDetails[0]+'_'+dtStr+'.dat','normal')
                if dataStream=='stable':
                    self.to_fwf(dfGroup1a,varDetails[5]+varDetails[0]+'_'+dtStr+'.dat','normal')
    
        # remove redundant df
        del dfGroup1a
         
                    
        timing = time.time() - start
        self.state = 0
        comment = 'reformatMonthly complete'
        return timing,self.state,comment;

    ####################################################################################
    # COMPILER loop and LOGGER
    ####################################################################################

    # loop for processing all bomdat daily files using ANUClimateFileOpen function
    def compileLoop(self,fHandle,dtStr,dStream):
        start = time.time()
        itra = 0
        os.chdir(self.zipPath+fHandle)
        fileList = glob('DC02D_Data_*')
        frames = [self.fileOpen(fname,dtStr,dStream) for fname in fileList]
        df = pd.concat(frames)
        dfOut = df.drop_duplicates(['Station_ID','Year','Month','Day'])
        self.state = 0
        comment = 'compile complete (numFiles, numRows): '+str(len(fileList))+' '+str(dfOut.index.size)
        dfOut.to_csv(self.backupPath+fHandle+'_df'+dStream+'.csv')
        timing = time.time() - start
        return dfOut,timing,self.state,comment;
        #return df,timing,self.state,comment;
    
        # logger func
    def logger(self,fHandle,process,timing,status,comment=None):
        '''
        Logger function - creates/maintains a csv log file for all steps in ANUClimate class
    
        args:
        * fHandle - str of BoM file identifier
        * process - str of ANUClimate_auto step
        * timing - str (rounded to 4 places) of time in secs for processing
        * status - end state of process (0 = success, 1 = fail)
        
        returns:
        nothing
        '''
        # check if file exists, if not create it
        if not os.path.isfile(self.logPath+'ANUClimate_log.csv'):
            # create seq for date and processing increment (eg: '2017_05_27_1' being 1st step on 27/5/2017 file)
            date = dt.datetime.today()
            seq = date.isoformat().split('T')[0][:4]+'_'+date.isoformat().split('T')[0][5:7]+'_'+date.isoformat().split('T')[0][8:10]+'_1'
            # create df
            df = pd.DataFrame({'date':[dt.datetime.today().isoformat()],'seq':[seq],'file_handle':[fHandle],'process':[process],'timing_secs':[timing],'state':[status],'comment':[comment]})
            # save df to csv
            df.to_csv(self.logPath+'ANUClimate_log.csv')
        # if file exists, open it
        elif os.path.isfile(self.logPath+'ANUClimate_log.csv'):
            df = pd.read_csv(self.logPath+'ANUClimate_log.csv',index_col=[0])
            # find next increment by index size
            prev = int(df.index.size)
            date = dt.datetime.today()
            seq = date.isoformat().split('T')[0][:4]+'_'+date.isoformat().split('T')[0][5:7]+'_'+date.isoformat().split('T')[0][8:10]+'_'+str(prev+1)
            # create new df and append to existing
            df1 = pd.DataFrame({'date':[dt.datetime.today().isoformat()],'seq':[seq],'file_handle':[fHandle],'process':[process],'timing_secs':[timing],'state':[status],'comment':[comment]})
            df = df.append(df1)
            # save df back to csv
            df.to_csv(self.logPath+'ANUClimate_log.csv')
        else:
            pass
            
            
##################################################
# LOOP
##################################################
# find unique dates of processing
## two parts:
## 1) find dates that didn't run at all
## 2) find dates that didn't complete (ie have entries, but no 'run_complete' in dfLog.process) DONE

ancr = ANUClimateAuto_rerun()

# part 1

# open log file
dfLog = pd.read_csv(ancr.logPath+'ANUClimate_log.csv',index_col=[0])
dfLogDates = sorted(list(set(dfLog.file_handle.sort_values())))
dfLogDates = [dt.datetime(int(x[14:18]),int(x[18:20]),int(x[20:22])) if type(x) == str and len(x) == 25 else '' for x in dfLogDates]
dfLogDates = list(filter(None, dfLogDates))
fullMissingDates = ancr.findMissingDates(dfLogDates)

fullMissingDatesFH = ['ANUdaily9am3pm'+str(x.isoformat().split('T')[0][:4])+str(x.isoformat().split('T')[0][5:7]).zfill(2)+str(x.isoformat().split('T')[0][8:10]).zfill(2)+calendar.day_name[x.weekday()][:3] for x in fullMissingDates]



# Part 2
#### N.B. run this process weekly and only look for dates in preceding week, therefore avoiding repeat re-runs
dateNow = dt.datetime.today()
dateMin = dateNow - relativedelta.relativedelta(days=1000)

# split out int time components from file_handle (eg: 'ANUdaily9am3pm20170812Sat' is 2017,08,12 for datetime comparison)
dfLog['fhDate'] = [dt.datetime(int(x[14:18]),int(x[18:20]),int(x[20:22])) if type(x) == str and len(x) == 25 else '' for x in dfLog.file_handle]
dateListFH = list(set(dfLog.fhDate.loc[pd.to_datetime(dfLog.fhDate)>=dateMin]))

# loop over dates to find processing dates where run_complete flag not found
for date in dateListFH:
    #print date
    dfLogDate = dfLog.loc[dfLog.fhDate==date]
    # check for run_complete flag (if found, pass; if missing process)
    if 'run_complete' in list(set(dfLogDate.process)):
        pass
    else:
        # create datevars to include in missingDict
        fH = list(set(dfLogDate.file_handle))
        #print fH
        fullMissingDatesFH.append(fH[0])
        # initial dict creation
        
# create master list of dates to rerun
itr = 0
for i in fullMissingDatesFH:
    numDays = ancr.daysInMth(int(i[14:18]),int(i[18:20]))
    if int(i[20:22])==int(numDays):
        call = 'all'
    else:
        call = 'alpha'
    dateList,timing,state,comment = ancr.getDateStringMissing(call=call,fHandle=i)
    if itr == 0:
            missingDict = {i:[call,dateList]}
    # append for new dates
    else:
        missingDict1 = {i:[call,dateList]}
        missingDict.update(missingDict1)
    itr+=1
    
f = open(ancr.logPath+'missingDates_'+str(dt.datetime.today().isoformat().split('T')[0])+'.txt','w')


try:
    print missingDict

    for mFHandle,mDateVars in missingDict.iteritems():
        f.write(mFHandle+','+str(mDateVars)+'\n')
        print mFHandle,str(mDateVars)
        startFull = time.time()
    
        ancr.logger(mFHandle,'findDataStream_'+str(mDateVars[0]),'0.001','0','findDataStream complete RERUN')
        ancr.logger(mFHandle,'getDateString_'+str(mDateVars[1]),'0.001','0',mDateVars[0]+' call RERUN')

        ftpTiming,ftpState,ftpComment = ancr.downloadFTP(mFHandle)

        ancr.logger(mFHandle,'downloadFTP',str(round(ftpTiming,4)),str(ftpState),str(ftpComment)+' RERUN')

        if mDateVars[0] == 'alpha':
            dfDay,clTiming,clState,clComment = ancr.compileLoop(mFHandle,mDateVars[1][0],'alpha')
            ancr.logger(mFHandle,'compileLoop_alpha',str(round(clTiming,4)),str(clState),str(clComment)+' RERUN')
            aTiming,aState,aComment = ancr.reFormatDaily(dfDay,mDateVars[1][0])
            ancr.logger(mFHandle,'reFormatDaily',str(round(aTiming,4)),str(aState),str(aComment)+' RERUN')
        else:
            dfDay,clTiming,clState,clComment = ancr.compileLoop(mFHandle,mDateVars[1][0],'alpha')
            ancr.logger(mFHandle,'compileLoop_alpha',str(round(clTiming,4)),str(clState),str(clComment)+' RERUN')
            aTiming,aState,aComment = ancr.reFormatDaily(dfDay,mDateVars[1][0])
            ancr.logger(mFHandle,'reFormatDaily',str(round(aTiming,4)),str(aState),str(aComment)+' RERUN')
            del dfDay
            dfBMth,clTiming,clState,clComment = ancr.compileLoop(mFHandle,mDateVars[1][1],'beta')
            ancr.logger(mFHandle,'compileLoop_beta',str(round(clTiming,4)),str(clState),str(clComment)+' RERUN')
            bTiming,bState,bComment = ancr.reFormatMonthOfDays(dfBMth,mDateVars[1][1],'beta')
            ancr.logger(mFHandle,'reFormatMonthOfDays_beta',str(round(bTiming,4)),str(bState),str(bComment))
            bMTiming,bMState,bMComment = ancr.reFormatMonthly(dfBMth,mDateVars[1][1],'beta')
            ancr.logger(mFHandle,'reFormatMonthly_beta',str(round(bMTiming,4)),str(bMState),str(bMComment)+' RERUN')
            del dfBMth
            dfSMth,clTiming,clState,clComment = ancr.compileLoop(mFHandle,mDateVars[1][2],'stable')
            ancr.logger(mFHandle,'compileLoop_stable',str(round(clTiming,4)),str(clState),str(clComment)+' RERUN')
            sTiming,sState,sComment = ancr.reFormatMonthOfDays(dfSMth,mDateVars[1][2],'stable')
            ancr.logger(mFHandle,'reFormatMonthOfDays_stable',str(round(sTiming,4)),str(sState),str(sComment)+' RERUN')
            sMTiming,sMState,sMComment = ancr.reFormatMonthly(dfSMth,mDateVars[1][2],'stable')
            ancr.logger(mFHandle,'reFormatMonthly_stable',str(round(sMTiming,4)),str(sMState),str(sMComment)+' RERUN')
            del dfSMth

        timeFull = time.time()-startFull
        ancr.logger(mFHandle,'run_complete',str(round(timeFull,4)),'0','RERUN')

    f.close()

except:
    print 'No missing dates'
