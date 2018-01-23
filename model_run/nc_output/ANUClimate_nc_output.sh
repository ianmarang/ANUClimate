#!/bin/bash
#PBS -P xa5
#PBS -q copyq
#PBS -l mem=32gb
#PBS -l walltime=0:50:00

python /g/data/rr9/fenner/prerelease/ANUClimate_auto/script/PyANUClimate_nc.py
cd /g/data/rr9/fenner/prerelease/ANUClimate_auto/script/nc_output
chmod -R 755 ./*
