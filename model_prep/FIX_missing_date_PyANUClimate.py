# code to fix Aug 2016 failed stable run

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
import calendar
from glob import glob

# import ANUClimate class
import ANUClimateAuto

anc = ANUClimateAuto.ANUClimateAuto()

startHalf = time.time()
# import stable
dateList = ['2017_10_29', '2017_08', '2017_04']
fHandle = 'ANUdaily9am3pm20171031Tue'

#dfBMth = pd.read_csv(anc.backupPath+fHandle+'_dfbeta.csv',index_col=[0])
#dfBMth.Station_ID = [str(x).zfill(6) for x in dfBMth.Station_ID]
#dfBMth.Day = [str(x).zfill(2) for x in dfBMth.Day]
#dfBMth.Month = [str(x).zfill(2) for x in dfBMth.Month]
#dfBMth.Year = [str(x) for x in dfBMth.Year]
#bMTiming,bMState,bMComment = anc.reFormatMonthly(dfBMth,dateList[1],'beta')
#anc.logger(fHandle,'reFormatMonthly_beta',str(round(bMTiming,4)),str(bMState),str(bMComment))
#del dfBMth

dfSMth = pd.read_csv(anc.backupPath+fHandle+'_dfstable.csv',index_col=[0])
dfSMth.Station_ID = [str(x).zfill(6) for x in dfSMth.Station_ID]
dfSMth.Day = [str(x).zfill(2) for x in dfSMth.Day]
dfSMth.Month = [str(x).zfill(2) for x in dfSMth.Month]
dfSMth.Year = [str(x) for x in dfSMth.Year]
#dfSMth,clTiming,clState,clComment = anc.compileLoop(fHandle,dateList[2],'stable')
#print str(clTiming),clComment
sTiming,sState,sComment = anc.reFormatMonthOfDays(dfSMth,dateList[2],'stable')
print str(sTiming),sComment
sMTiming,sMState,sMComment = anc.reFormatMonthly(dfSMth,dateList[2],'stable')
print str(sMTiming),sMComment
print 'complete: '+str(time.time()-startHalf)
