#!/bin/bash
#PBS -P xa5
#PBS -q normal
#PBS -l walltime=0:40:00,mem=1024MB

python /g/data/rr9/fenner/prerelease/ANUClimate_auto/script/PyANUClimate_model_run.py
