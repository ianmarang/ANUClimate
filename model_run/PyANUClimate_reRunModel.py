# final rerun script
# import libraries
import pandas as pd
import numpy as np
import os,sys
import time
import datetime as dt
import subprocess
from dateutil.relativedelta import relativedelta
import fnmatch as fn

class ANUClimate_reRunModel(object):
    def __init__(self):
        self.baseDir = '/g/data/rr9/fenner'
        self.scriptPath = self.baseDir+'/prerelease/ANUClimate_auto/script'
        self.logPath = self.baseDir+'/prerelease/ANUClimate_auto/log'
        # varDict definition = {variable:[.dat source file location, batch file location, output array location]}
        self.varDict = {'tmax':[self.baseDir+'/prerelease/fenner/tmax_day_v2_0/alpha/dat/bomdat/',self.baseDir+'/prerelease/fenner/tmax_day_v2_0/alpha/batch/',self.baseDir+'/aus_tmax_day_v2_0/alpha/']}#,
                        #'tmin':[self.baseDir+'/prerelease/fenner/tmin_day_v2_0/alpha/dat/bomdat/',self.baseDir+'/prerelease/fenner/tmin_day_v2_0/alpha/batch/',self.baseDir+'aus_tmin_day_v2_0/alpha/'],
                        #'tavg':[self.baseDir+'/prerelease/fenner/tavg_day_v2_0/alpha/dat/bomdat/',self.baseDir+'/prerelease/fenner/tavg_day_v2_0/alpha/batch/',self.baseDir+'aus_tavg_day_v2_0/alpha/'],
                        #'rain':[self.baseDir+'/prerelease/fenner/rain_day_v2_0/alpha/dat/bomdat/',self.baseDir+'/prerelease/fenner/rain_day_v2_0/alpha/batch/',self.baseDir+'aus_rain_day_v2_0/alpha/'],
                        #'vp':[self.baseDir+'/prerelease/fenner/vp_day_v2_0/alpha/dat/bomdat/',self.baseDir+'/prerelease/fenner/vp_day_v2_0/alpha/batch/',self.baseDir+'aus_tmax_day_v2_0/alpha/'],
                        #'vpd':[self.baseDir+'/prerelease/fenner/vpd_day_v2_0/alpha/dat/bomdat/',self.baseDir+'/prerelease/fenner/vpd_day_v2_0/alpha/batch/',self.baseDir+'aus_vpd_day_v2_0/alpha/'],
                        #'evap':[self.baseDir+'/prerelease/fenner/evap_day_v2_0/alpha/dat/bomdat/',self.baseDir+'/prerelease/fenner/evap_day_v2_0/alpha/batch/',self.baseDir+'aus_evap_day_v2_0/alpha/'],
                        #'pw':[self.baseDir+'/prerelease/fenner/pw_day_v2_0/alpha/dat/bomdat/',self.baseDir+'/prerelease/fenner/pw_day_v2_0/alpha/batch/',self.baseDir+'aus_vpd_day_v2_0/alpha/']}

    def logger(self,currDt,action,tarDt,jobID):
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
        if not os.path.isfile(self.logPath+'/ANUClimate_model_run_log.csv'):
            # create df
            df = pd.DataFrame({'datetime':[currDt],'process':[action],'target_date':[tarDt],'jobID':[jobID]})
            # save df to csv
            df.to_csv(self.logPath+'/ANUClimate_model_run_log.csv',columns=['datetime','process','target_date','jobID'],index=False)
        # if file exists, open it
        else:
            df = pd.read_csv(self.logPath+'/ANUClimate_model_run_log.csv')
            # create new df and append to existing
            df1 = pd.DataFrame({'datetime':[currDt],'process':[action],'target_date':[tarDt],'jobID':[jobID]})
            df = df.append(df1)
            # save df back to csv
            df.to_csv(self.logPath+'/ANUClimate_model_run_log.csv',columns=['datetime','process','target_date','jobID'],index=False)


    # function
    def findMissingDates(self,dateL):
        '''
        Takes list of dates and finds dates with no entry from the start of the output files till now

        args:
        * dateL - list of dates in log

        returns:
        * missing - sorted list of missing dates
        '''
        dtWeekAgo = dt.datetime.today() - relativedelta(days=7)
        date_set = set(dtWeekAgo + dt.timedelta(x) for x in range(6))
        missing = sorted(date_set - set(dateL))
        return missing;

    def reRun(self,var):
        '''
        Function to check if any output arrays are missing, then checking if these missing dates have source files,
        before submitting the batch jobs and logging the steps
        
        args:
        * self - class object of ANUClimate_reRunModel
        * var - str of ANUClimate variable
        '''
        # date var for submitting today's model run
        dtNohyp=(dt.datetime.today()-relativedelta(days=2)).strftime('%Y%m%d')
        # date var for log and comparing against next hour so script breaks if after midnight (to prevent infinite loop)
        dtToday = dt.datetime.today().strftime('%Y%m%d%H%M')
        
        # create empty container to hold dates of model output files and move to correct folder
        dateList = []
        varDetails = self.varDict[var]

        # loop over files in output dir
        for fname in os.listdir(varDetails[-1]):
            # find files with .flt extension
            if fname.endswith('.flt'):
                # create date object from each file name
                dtObj = dt.datetime(int(fname.split('_')[-1][:4]),int(fname.split('_')[-1][4:6]),int(fname.split('_')[-1][6:8]))
                # add to dateList container
                dateList.append(dtObj)

        # call function to find missing dates
        missingList = self.findMissingDates(sorted(dateList))       
        
        # change to batch dir so the resulting batch files are deposited here
        os.chdir(varDetails[1])

        # check source file dates to confirm if we have source files for missing model run days
        for srcFile in os.listdir(varDetails[0]):
            if srcFile.endswith('.dat'):
                # create srcFileDate obj to compare against missingList
                srcFileDate = dt.datetime(int(srcFile.split('_')[1]),int(srcFile.split('_')[2]),int(srcFile.split('_')[-1][:-4]))
                # check if in missingList
                if srcFileDate in missingList:
                    # create datevars from identified srcFiles
                    srcYr,srcMt,srcDy = srcFileDate.isoformat().split('T')[0].split('-')[0],srcFileDate.isoformat().split('T')[0].split('-')[1],srcFileDate.isoformat().split('T')[0].split('-')[-1]
                    # call subprocess to run '/g/data/rr9/fenner/prerelease/fenner/tmax_day_v2_0/alpha/runtmax YYYY MM DD'
                    p = subprocess.Popen(['../runtmax '+srcYr+' '+srcMt+' '+srcDy],shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
                    b = p.communicate()
                    self.logger(dtToday,'called run'+var+' '+srcYr+' '+srcMt+' '+srcDy,dtNohyp,'na')
                    # run with small lag to prevent overlap
                    time.sleep(10)

        
        # collect all date elements of batch filenames for comparison
        fnameDates = [x[4:12] for x in os.listdir('.')]

        # loop over date elements
        for dtBits in fnameDates:
            # create new fnameList for each date element
            fnameList = []
            # loop over files to compare against date elements
            for fname in os.listdir('.'):
                # if match date element, append to list
                if fn.fnmatch(fname,'tmax'+dtBits+'*'):
                    fnameList.append(fname)
            # check list length, if only 1 the batch job hasn't been run
            if len(fnameList)==1:
                # submit batch job through PBS queue
                p = subprocess.Popen(['qsub '+fnameList[0]],shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
                b = p.communicate()
                # log bash command for batch model run
                self.logger(dtToday,'rerun: qsub '+fnameList[0],dtNohyp,b[0].decode('ascii'))
            else:
                pass

# check if script running as module or called program (ie 'python ANUClimate_rerunModel.py')
if __name__ == '__main__':
    # create object
    anm = ANUClimate_reRunModel()
    # iterate over varDict to process each var
    for var in anm.varDict:
        # call reRun function for each var
        anm.reRun(var)
