The ANUClimate model runs on meteorological point data from Australia's Bureau of Meteorology (BoM).  

These data files are accessed through ftp as a .zip file containing data for each station with observations available in .txt format.

Accompanying the .txt files are a station listing (with lat, lon, name, elevation, etc) and an explanatory notes file.

Each .txt file contains ~ 6 months of data (with significant variation in length for some files) with each day on a new line.

The quality for each observation depends on how old it is:
- up to 2 months, none to sparse data quality checks => alpha
- 2 months to 6 months, limited data quality checks => beta
- 6 months old, full data quality checks applied => stable

The model_prep module covers the preprocessing required by the ANUClimate model and follows these steps:
1. find the correct datastream (alpha only or alpha, beta and stable processing)
--- if end of month: all datastreams are processed
--- if any other day: only alpha

2. determine file name for download and list of dates to process (for alpha, beta and/or stable)

3. logs into ftp site, makes dirs and downloads and unzips file

4. compiles pandas dataframe of days data for each station, running preliminary data quality checks as required

5. outputs fixed width .dat files for model run

6. rsyncs files over to ANUClimate model processing location on NCI's raijin (/g/data/rr9/fenner/...)
