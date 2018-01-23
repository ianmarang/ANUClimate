#################################################################################################################################################
# ANUClimate_model_run.py v 1.0
#################################################################################################################################################
# Called by ANUClimate_model_run.sh each day at 4pm, will check for src .dat files then process background files ('../alpha/run<var> YYYY MM DD')
# and batch command (../batch/<var><YYYYMMDD>.  If src files are not available at 4pm, it will resubmit for each hour and then try again next day
#
# Python 2.7
# PEP 8 compliant
# author: Ian Marang

# import libraries
import pandas as pd
import numpy as np
import os,sys
import time
import datetime as dt
import subprocess
from dateutil.relativedelta import relativedelta


# create class object
class ANUClimate_model_run(object):
    def __init__(self):
        self.baseDir = '/g/data/rr9/fenner'
        self.scriptPath = '/g/data/rr9/fenner/prerelease/ANUClimate_auto/script/model_runtime'
        self.logPath = '/g/data/rr9/fenner/prerelease/ANUClimate_auto/log'
        # varDict definition = {variable:[.dat source file location, batch file location, output array location]}
        self.varDict = {'tmax':[self.baseDir+'/prerelease/fenner/tmax_day_v2_0/alpha/dat/bomdat/',self.baseDir+'/prerelease/fenner/tmax_day_v2_0/alpha/batch/',self.baseDir+'aus_tmax_day_v2_0/alpha/']}#,
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
    
    
    def runModel(self,var):
        '''
        Function to see if source files are present, to launch initial run<var> compiled fortran code and to run model batch.  If src files
        aren't found, will resubmit for the next hour to check again.  If still not found before end of day, resubmit for next day
        
        args:
        * self - class instance of ANUClimate_model_run
        * var - string of ANUClimate model run variable name
        '''
        # collect details for the var
        varDetails = self.varDict[var]
        # create date vars
        # date var for source data
        dthyp = (dt.datetime.today()-relativedelta(days=2)).strftime('%Y_%m_%d')
        # component date vars for run fortran code for background fields
        dtYr,dtMt,dtDy = dthyp.split('_')[0],dthyp.split('_')[1],dthyp.split('_')[2]
        # date var for resubmitting the model for tomorrow
        dtNextday = (dt.datetime.combine(dt.datetime.today()+relativedelta(days=1),dt.time(16,0,0))).strftime('%Y%m%d%H%M')
        # date var for submitting today's model run
        dtNohyp=(dt.datetime.today()-relativedelta(days=2)).strftime('%Y%m%d')
        # date var for resubmitting this script in an hour (if source data files unavailable)
        dtNexthour = (dt.datetime.today()+relativedelta(hours=1)).strftime('%Y%m%d%H%M')
        # date var for comparing against next hour so script breaks if after midnight (to prevent infinite loop)
        dtToday = dt.datetime.today().strftime('%Y%m%d%H%M')
        
        # check if source files available
        if os.path.isfile(varDetails[0]+var+'_'+dthyp+'.dat'):
            # move to batch folder for processing run<var> and batch job
            os.chdir(varDetails[1])
            # call subprocess to run '/g/data/rr9/fenner/prerelease/fenner/tmax_day_v2_0/alpha/runtmax YYYY MM DD'
            p = subprocess.Popen(['../runtmax '+dtYr+' '+dtMt+' '+dtDy],shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
            b = p.communicate()
            # log bash command for run<var>
            self.logger(dtToday,'called run'+var+' '+dtYr+' '+dtMt+' '+dtDy,dtNohyp,'na')
            # wait 10mins for run<var> file to complete
            time.sleep(600)
            # call subprocess to submit batch model run
            p = subprocess.Popen(['qsub '+var+dtNohyp],shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
            b = p.communicate()
            # log bash command for batch model run
            self.logger(dtToday,'qsub '+var+dtNohyp,dtNohyp,b[0].decode('ascii'))
            # run the bash script calling this python script tomorrow, call from script dir
            os.chdir(self.scriptPath)
            p = subprocess.Popen(['qsub -a '+dtNextday+' ANUClimate_model_run.sh'],shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
            b = p.communicate()
            # log bash command for next day resubmission
            self.logger(dtToday,'qsub -a '+dtNextday+' ANUClimate_model_run.sh',dtNohyp,b[0].decode('ascii'))
        else:
            if int(dtNexthour[6:8]) != int(dtToday[6:8]):
                # change to script dir
                os.chdir(self.scriptPath)
                # call subprocess for next day resubmission
                p = subprocess.Popen(['qsub -a '+dtNextday+' ANUClimate_model_run.sh'],shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
                b = p.communicate()
                # log bash command for next day resubmission
                self.logger(dtToday,'abort run no src files: qsub -a '+dtNextday+' ANUClimate_model_run.sh',dtNohyp,b[0].decode('ascii'))
            else:
                # call subprocess for next day resubmission
                p = subprocess.Popen(['qsub -a '+dtNexthour+' ANUClimate_model_run.sh'],shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
                b = p.communicate()
                # log bash command for next hour resubmission
                self.logger(dtToday,'no src files: qsub -a '+dtNexthour+' ANUClimate_model_run.sh',dtNohyp,b[0].decode('ascii'))
            
if __name__ == '__main__':           
    # create object            
    anm = ANUClimate_model_run()

    # iterate over list of vars
    for var,varList in anm.varDict.iteritems():
        # process each var
        anm.runModel(var)

