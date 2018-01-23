# class ANUClimateAuto netcdf creator
# import libraries
import numpy as np
import netCDF4 as nc
import os
import requests
import xml.etree.ElementTree as ET
import gdal
import datetime as dt
from dateutil.relativedelta import relativedelta
import pandas as pd
import decimal
import subprocess



class ANUClimateAutoNetcdf(object):
    def __init__(self):
        self.nameSpace = {'gmd':'http://www.isotc211.org/2005/gmd',
                          'gco':'http://www.isotc211.org/2005/gco',
                          'gml':'http://www.opengis.net/gml'}
        self.baseDir = '/g/data/rr9/fenner/'
        self.logPath = self.baseDir+'prerelease/ANUClimate_auto/log'
        self.varDict = {'tmax':['aus_tmax_day_v2_0','ANUClimate_v2-0_tmax-alpha_daily_0-01deg','7f12ef37-6464-4ab8-a217-37a070ec2d96']}#,
                        #'rain':['aus_rain_day_v2_0','ANUClimate_v1-0_rainfall_daily_0-01deg_1970-2014','d128bb4f-586f-4264-bd31-a774a7ac440f'],
                        #'srad':['aus_srad_day_v2_0','ANUClimate_v1-1_solar-radiation_daily_0-01deg_1970-2014','842a2599-6443-48e2-af84-39e9448600d4'],
                        #'tmin':['aus_tmin_day_v2_0','ANUClimate_v1-1_temperature-min_daily_0-01deg_1970-2014','e1a3ef9b-b339-4dcc-aec7-d49ac9b6317f'],
                        #'vp':['aus_vp_day_v2_0','ANUClimate_v1-1_vapour-pressure_daily_0-01deg_1970-2014','040c599f-88b2-4bad-b244-8cf3190c9b16']}
        self.xmlURL_base = 'http://geonetworkrr9.nci.org.au/geonetwork/srv/eng/xml.metadata.get?uuid='
        # auth is a tupe of (username,password)
        self.auth = ('admin','admin')
        self.xmlStore = '/g/data/rr9/IM_PhD/data/Metadata/metadata_dump/'
        
        for var, varDetails in self.varDict.iteritems():
            try:
                os.makedirs(self.baseDir+'prerelease/ANUClimate_auto/script/nc_output/'+varDetails[1])
            except:
                pass
    
    # function for downloading and reading in a dict of metadata
    
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


    def getMeta(self,dataset,uuid):
        '''
        Looks up metadata on geonetwork, downloads the .xml, then reads through xml finding the variable elements required for netcdf publication

        args:
        dataset - str of dataset name (eg ANUClimate_v1-0_rainfall_daily_0-01deg_1970-2014)
        uuid - str of geonetwork uuid (eg 834849ce-61b3-4bdd-8a79-edab103b583a)

        returns:
        xmlDict of metadata
        '''
        os.chdir(self.xmlStore)

        xmlName = dataset+'_'+uuid+'.xml'

        f = open(xmlName,'wb')
        r = requests.get(self.xmlURL_base+uuid, auth=self.auth).content
        f.write(r)
        f.close()

        tree = ET.parse(xmlName)
        root = tree.getroot()
        Title =  root.findall('.//gmd:title',self.nameSpace) # var: long_name, global: title
        AltTitle =  root.findall('.//gmd:alternateTitle',self.nameSpace) # var: standard_name
        Code =  root.findall('.//gmd:code',self.nameSpace) # global: id
        Abstract =  root.findall('.//gmd:abstract',self.nameSpace) # global: summary
        Credit =  root.findall('.//gmd:credit',self.nameSpace) # global: citation, global: references
        Unit =  root.findall('.//gmd:LI_Lineage',self.nameSpace) # var: units
        FileIdentifier =  root.findall('.//gmd:fileIdentifier',self.nameSpace) # metadata_uuid [6] source [7]
        xmlList = [Title[0][0].text, # 0 - var: long_name, global: title
		           AltTitle[0][0].text, # 1 - var: standard_name
                   Code[0][0].text, # 2 - global: id
                   Abstract[0][0].text, # 3 - global: summary
                   'Michael Hutchinson'+Credit[0][0].text.split('Michael Hutchinson')[-1], # 4 - global: citation
                   Credit[0][0].text.split('Michael Hutchinson')[0][:-11], # 5 global: references
                   Unit[0][3][0].text, # 6 - var: units
                   FileIdentifier[0][0].text, # 7 - metadata_uuid
                   str.split(Code[0][0].text,'_')[0]+' '+str.split(Code[0][0].text,'_')[1]]#, # 8 - global: source 

        return xmlList;
    
    def make_nc(self,
                outfile=None,
                data=None,
                lati=None,
                loni=None,
                timei=None,
                header=None,
                nodata=-999.,
                metadata=None):
        ncds = nc.Dataset(outfile, 'w', zlib=True,complevel=9,format='NETCDF4')
        time  = ncds.createDimension('time', len(timei))
        lat   = ncds.createDimension('lat', lati.shape[0])
        lon   = ncds.createDimension('lon', loni.shape[0])
        times = ncds.createVariable('time','f8',('time'))
        latitudes = ncds.createVariable('lat','f8',('lat'))
        longitudes = ncds.createVariable('lon','f8',('lon'))
        variable = ncds.createVariable(metadata[1],'f4',('time','lat','lon',),chunksizes=(1,100,100),zlib=True,complevel=9,fill_value=-999.)
        latitudes[:] = lati[:]
        longitudes[:] = loni[:]
        times[:] = timei[:]
        #print np.shape(data)
        variable[:,:,:] = data
        variable.standard_name = metadata[1]
        variable.grid_mapping = 'crs'
        variable.coordinates = 'time lat lon'
        variable.long_name = metadata[0]
        variable.units = metadata[6]
        variable.coverage_content_type = 'modelResult'
        latitudes.long_name = 'latitude'
        latitudes.standard_name = 'latitude'
        latitudes.units = 'degrees_north'
        latitudes.axis = 'Y'
        longitudes.long_name = 'longitude'
        longitudes.standard_name = 'longitude'
        longitudes.units = 'degrees_east'
        longitudes.axis = 'X'
        times.long_name = 'time'
        times.standard_name = 'time'
        times.units = 'seconds since 1970-01-01 00:00:00'
        times.calendar = 'gregorian'
        times.axis = 'T'

        ncds.geospatial_lat_min = header['latsmin']
        ncds.geospatial_lat_max = header['latsmax']
        ncds.geospatial_lat_units = 'degrees_north'
        ncds.geospatial_lat_resolution = header['resolution']
        ncds.geospatial_lon_min = header['lonsmin']
        ncds.geospatial_lon_max = header['lonsmax']
        ncds.geospatial_lon_units = 'degrees_east'
        ncds.geospatial_lon_resolution = header['resolution']
        
        # Set crs as variable
        # use setattr as name is protected system value
        crs=ncds.createVariable('crs','f4')
        crs.setncattr('grid_mapping_name', "GDA94")
        crs.spatial_ref='GEOGCS["GDA94",DATUM["Geocentric_Datum_of_Australia_1994",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6283"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329252,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4283"]]'
        setattr(crs, 'datum', "Geocentric_Datum_of_Australia_1994")
        setattr(crs, 'inverse_flattening', 298.257222101)
        setattr(crs, 'semi_major_axis', 6378137.0)
        setattr(crs, 'semi_minor_axis', 6356752.314140356)
        setattr(crs, '_CoordinateTransformType', "Projection")
        setattr(crs, '_CoordinateAxisTypes', "GeoX GeoY")
        setattr(crs, 'proj4text', "+proj=longlat +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +no_defs")

        # Set crs globally [alternative method!]
        setattr(ncds, 'time_coverage_start', str(dt.date(1970,1,1)+relativedelta(seconds=timei[0])))
        setattr(ncds, 'time_coverage_end', str(dt.date(1970,1,1)+relativedelta(seconds=timei[-1])))
        setattr(ncds, 'DOI', "To be added")
        setattr(ncds, 'id', metadata[2])
        setattr(ncds, 'title', metadata[0])
        setattr(ncds, 'citation', metadata[4])
        setattr(ncds, 'references',metadata[5])
        setattr(ncds, 'keywords', "EARTH SCIENCE > ATMOSPHERE")
        setattr(ncds, 'summary', metadata[3])
        setattr(ncds, 'publisher_name', "The University of Sydney")
        setattr(ncds, 'acknowledgment', "The creation of this data was funded by The University of Sydney.")
        setattr(ncds, 'source', 'ANUClimate_v2-0')
        setattr(ncds, 'publisher_url', "http://anuclimate.wordpress.com")
        setattr(ncds, 'publisher_email', "anuclimate@gmail.com")
        setattr(ncds, 'keywords_vocabulary', "ANZSCR-FOR: http://www.abs.gov.au/ausstats/abs@.nsf/0/6BB427AB9696C225CA2574180004463E")
        setattr(ncds, 'metadata_link', "http://geonetworkrr9.nci.org.au/geonetwork/srv/eng/catalog.search#/home")
        setattr(ncds, 'metadata_uuid', metadata[7])
        setattr(ncds, 'creator_name', "Michael Hutchinson, Jennifer Kesteven, Tingbao Xu")
        setattr(ncds, 'creator_email', "anuclimate@gmail.com")
        setattr(ncds, 'creator_url', "http://anuclimate.wordpress.com")
        setattr(ncds, 'institution', "Australian National University")
        setattr(ncds, 'license', "Copyright 2017 ANU. Rights owned by The Australian National University (ANU). Rights licensed subject to Attribution Licence (CC BY 4.0)  https://creativecommons.org/licenses/by/4.0/legalcode")
        setattr(ncds, 'Conventions', "ACDD-1.3")
        setattr(ncds, 'coverage_content_type', "modelResult")
        setattr(ncds, 'cdm_data_type', "grid")
        setattr(ncds, 'contact', "Michael Hutchinson, Professor of spatial and temporal analysis, 3.23A, Fenner School of Environment & Society, College of Medicine, Biology & Environment, Frank Fenner Building 141, Australian National University, Canberra, Australian Capital Territory, 0200.0, Australia, (+61) 2 6125 4783, Michael.Hutchinson@anu.edu.au, http://orcid.org/0000-0001-8205-6689")
        setattr(ncds, 'history', "Reformatted to NetCDF: "+dt.date.today().isoformat())
        setattr(ncds, 'date_created', dt.date.today().isoformat())
        # Close the file
        print 'Congratulations, your netCDF file is baked! See:', outfile
        ncds.close()
        # Report back
        
    def getLatLon(self,extents):
        '''
        Based on extents arg, calls either old nc file for extents, or uses np.arange
        
        args:
        * self - an instance of the ANUClimateAutoNetcdf class
        * extents - str of 'old' or 'new'
        '''
        
        if extents == 'old':
            fileLL = nc.Dataset('/g/data/rr9/eMAST_data/ANUClimate/ANUClimate_v1-0_digital-elevation-model_terrain_0-01deg/00000000/ANUClimate_v1-0_digital-elevation-model_terrain_0-01deg_00000000_v1m0.nc','r')
            lat = fileLL.variables['lat'][:]
            lon = fileLL.variables['lon'][:]
            fileLL.close()
        else:
            decimal.getcontext().prec=6
            lat = np.arange(-43.995,-8.995,0.01)
            lat = lat[::-1]
            lon = np.arange(112.005,153.995,0.01)

            lat = [decimal.Decimal(x).quantize(decimal.Decimal('.001'), rounding=decimal.ROUND_HALF_UP) for x in lat]
            lon = [decimal.Decimal(x).quantize(decimal.Decimal('.001'), rounding=decimal.ROUND_HALF_UP) for x in lon]

            lat = np.array([str(x) for x in lat])
            lon = np.array([str(x) for x in lon])
        return lat,lon;
        
    def getSpatialExtents(self,fName):
        '''
        Function to derive geospatial extents (cell edges) from header (.hdr) files for each floating point array (.flt) for the ANUClimate model runs conversion into netcdf
    
        args:
    
        self - object instance of ANUClimateNetcdf class
        fName - file being converted (will be .flt, but first step in function will replace .flt as .hdr before reading the file)
        '''
        fname = fName.replace('flt','hdr')
        fullList = []
        f = open(fname,'r')
        for line in f:
            sList = line.split(' ')
            sList = [x.replace('\n','') for x in sList]
            sList = [x for x in sList if x]
            fullList.append(sList)
        df = pd.DataFrame({'item':[x[0] for x in fullList],'entry':[x[-1] for x in fullList]})
        lonMin = float(df.entry.loc[df.item=='XLLCORNER'].iloc[0])
        latMin = float(df.entry.loc[df.item=='YLLCORNER'].iloc[0])
        lonMax = float(lonMin)+(float(df.entry.loc[df.item=='NCOLS'].iloc[0])*float(df.entry.loc[df.item=='CELLSIZE'].iloc[0]))
        latMax = float(latMin)+(float(df.entry.loc[df.item=='NROWS'].iloc[0])*float(df.entry.loc[df.item=='CELLSIZE'].iloc[0]))
        ncols = int(df.entry.loc[df.item=='NCOLS'].iloc[0])
        nrows = int(df.entry.loc[df.item=='NROWS'].iloc[0])
        res = np.round(float(df.entry.loc[df.item=='CELLSIZE'].iloc[0]),2)
        return lonMin,lonMax,latMin,latMax,res,ncols,nrows;
        
    
    
# date var for resubmitting the model for tomorrow
dtNextday = (dt.datetime.combine(dt.datetime.today()+relativedelta(days=1),dt.time(22,0,0))).strftime('%Y%m%d%H%M')
# date var for comparing against next hour so script breaks if after midnight (to prevent infinite loop)
dtToday = dt.datetime.today().strftime('%Y%m%d%H%M')


anc = ANUClimateAutoNetcdf()
dtYr = dt.datetime.today().isoformat()[:4]
for var, varDetails in anc.varDict.iteritems():
    targetDir = anc.baseDir+'/'+varDetails[0]+'/alpha/'+dtYr+'/'
    os.chdir(targetDir)
    # check processed files to see if output array nc file already generated
    doneDtBit = [x.split('_')[-1][:-3] for x in os.listdir(anc.baseDir+'prerelease/ANUClimate_auto/script/nc_output/'+varDetails[1])]
    for fname in os.listdir('.'):
        if fname.endswith('.flt'):
            dtBit = fname.split('_')[-1][:-4]
            print dtBit
            if dtBit not in doneDtBit:
                # find right spatial extents
                # define function netcdf creation
                targetFName = targetDir+fname
                lonMin,lonMax,latMin,latMax,res,ncols,nrows = anc.getSpatialExtents(targetFName)
                if ncols == 4200:
                    lat,lon = anc.getLatLon('new')
                    head = {'samples': ncols,
                        'lines': nrows,
                        'bands': 1,
                        'latsmin': -43.995,
                        'lonsmin':112.005,
                        'latsmax': -9.005,
                        'lonsmax':153.995,
                        'epsg':4326,
                        'resolution':res}
                else:
                    lat,lon = anc.getLatLon('old')
                    head = {'samples': ncols,
                            'lines': nrows,
                            'bands': 1,
                            'latsmin': -43.735,
                            'lonsmin':112.905,
                            'latsmax': -9.005,
                            'lonsmax':153.995,
                            'epsg':4326,
                            'resolution':res}
                
                # process file
                nameVar = []
                nameVar.append(str.split(fname,'_')[-1][:-4])
                # sets up correct name variable for make_nc function
                correctName = varDetails[1]+'_'+nameVar[0]+'.nc'
                print correctName
                # splits out time into datetime processing chunks
                year = int(nameVar[0][:4])
                month = int(nameVar[0][4:6])
                day = int(nameVar[0][6:8])
                # creates datetime object
                t = dt.datetime(year,month,day)
                # uses datetime object to determine number of seconds since 01/01/1970
                fileTime = np.array((t - dt.datetime(1970,1,1)).total_seconds()).reshape(1,)
                #print fileTime
                # take metadata list from xmldict
                metadata = anc.getMeta(varDetails[1],varDetails[-1])
                #print metadata
                # change directory inside source directories to open data arrays
                os.chdir(anc.baseDir+'/'+varDetails[0]+'/alpha/'+str(year))
                # open data file
                dataFile = gdal.Open(fname)
                # create array of data for each file
                data = dataFile.ReadAsArray()
                # move directory to destination folder
                os.chdir(anc.baseDir+'/prerelease/ANUClimate_auto/script/nc_output/'+varDetails[1])
                # call the make_nc function to create the netcdf file
                anc.make_nc(outfile=correctName,data=data,lati=lat,loni=lon,timei=fileTime,header=head,nodata=-999.,metadata=metadata)
            else:
                pass

# add log entry for today's model nc output
anc.logger(dtToday,'completed ANUClimate_nc_output.sh','na','na')                
# call subprocess for next day resubmission
os.chdir(anc.baseDir+'/prerelease/ANUClimate_auto/script/nc_output/')
p = subprocess.Popen(['qsub -a '+dtNextday+' ANUClimate_nc_output.sh'],shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
b = p.communicate()
# log bash command for next day resubmission
anc.logger(dtToday,'qsub -a '+dtNextday+' ANUClimate_nc_output.sh','na',b[0].decode('ascii'))
