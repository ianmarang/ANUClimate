# ANUClimate
Automated model run from Bureau of Meteorology point data files for use in DPI drought indices and other projects

Description:
================
"The daily and monthly climate variables presented here have been generated using ANUClimate 1.0. This is a spatial model, developed by Michael Hutchinson, that integrates a new approach to the interpolation of Australia’s national point climate data to produce climate variables on a 0.01° longitude/latitude grid. Most climate values have been modelled by expressing each value as a normalised anomaly with respect to the gridded 1976-2005 mean. These means and anomalies were all interpolated by trivariate thin plate smoothing spline functions of longitude, latitude and vertically exaggerated elevation using ANUSPLIN Version 4.5, with additional dependences on proximity to the coast for the temperature and vapour pressure variables. Station elevations for the gridded temperature and vapour pressure variables were obtained from 0.01° local averages of grid values from the GEODATA 9 second DEM version 3. Station elevations for the gridded rainfall and pan evaporation variables were obtained from 0.05° local averages of grid values from the GEODATA 9 second DEM version 3. 

The main aim of this project is to support the modelling of the spatial distributions of plants and animals, to make long-term estimates of land surface processes for assessment of agriculture and biodiversity, and to provide a baseline for the assessment of the impacts of projected climate change."

System overview:
================
ANUClimate ingests reformatted point data from BoM daily txt files and interpolates the variable across a continental scale surface grid.  These .txt files have various QA steps taken internally by the BoM and the automation of the model run is split across three different products to match this.  These are:

Alpha - near real time product (minimal BoM QA) from data 2 days prior
Beta - medium level certainty product (some BoM QA) from data 2 months prior
Stable - high level certainty product (full BoM QA) from data 6 months prior

In addition to the QA undertaken by BoM, the automation schema handles some basic checks on data quality at the reformatting stage.  There are 3 file types produced depending on when the process is run.  If the last day of the month, daily, month of days and monthly aggregated files are produced.  At any other time only daily files are returned.

Python scripts to achieve this are as follows:
- PyANUClimate.py
- PyANUClimate_model_run (and a bash shell script to call it)
- PyANUClimate_reRun_model (and a bash shell script to call it)
- PyANUClimate_nc (and a bash shell script to call it) TBD

In order to comply with the operating environment on the main HPC system in use (NCI), PyANUClimate.py is run on an externally hosted VM (within USYD network), while the remaining 3 key processes are run on NCI using submitted PBS batch jobs (and hence have a .sh script to call the Python scripts).

Process flow:
================

Daily (time, name of script, desc, location)
2.35pm, PyANUClimate.py, BoM .zip archive of .txt files downloaded via ftp and reformatted for daily (alpha only) month of days (beta, stable) and monthly (beta, stable) files, USYD VM
3.05pm, ANUClimate_auto_transfer.sh, transfers files to appropriate folder locations on NCI, USYD VM
4.00pm, PyANUClimate_model_run.py, checks for source files (from USYD VM) then runs initial fortran code to generate spline coefficients for model run and waits 10mins before submitting model run batch job and finally resubmits itself (through ANUClimate_model_run.sh) for the next day (if files are not found it resubmits itself for the next hour until midnight), NCI 
7.30pm, PyANUClimate_rerun.py, checks if any dates are missing or incomplete in log file and reruns for that date, USYD VM
9.00pm, ANUClimate_auto_transfer.sh, transfers files to appropriate folder locations on NCI, USYD VM

Weekly (time, name of script, desc, location)
Thu 7.00pm, checks output arrays (.flt) to find any missing dates and reruns the model for those dates


