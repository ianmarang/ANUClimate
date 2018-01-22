ANUClimate model runs daily, as .dat files are available.  

The model run is a two part process:

1. A pre-compiled fortran executable accesses the source .dat files and generates a grid of coefficients
2. Using pre-generated background files and the grid of coefficients, variables of interest are output as ESRI .flt arrays

The model_run script uses the NCI Raijin's batch queue system (PBSPro) with timed submission to launch the python scripts
