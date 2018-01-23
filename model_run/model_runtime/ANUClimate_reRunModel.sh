#!/bin/bash
#PBS -P xa5
#PBS -q normal
#PBS -l walltime=2:00:00,mem=1024MB

# move to correct base dir
cd /g/data/rr9/fenner/prerelease/ANUClimate_auto/script

# set up date for run tomorrow
dtNextWeek=$(date --date="7pm next Thursday" +%Y%m%d%H%M)

# launch python rerun code
python ./PyANUClimate_reRunModel.py

# resubmit for following week
qsub -a $dtNextWeek ./model_run/ANUClimate_rerunModel.sh
