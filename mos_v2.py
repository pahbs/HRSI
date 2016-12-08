#-------------------------------------------------------------------------------
# Name:        mos.py
# Purpose:      footprint and mosaic ASTER GDEM data
#
# Author:      pmontesa
#
# Created:     29/06/2015
# Copyright:   (c) pmontesa 2015
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import os, sys, math, time, subprocess
import shapefile as shp
from osgeo import gdal, gdalconst
from osgeo.gdalconst import *
gdal.AllRegister() #register all raster format drivers
import gdalinfo

def mos_gdem(direct='/att/dsc/nobackup03/ppl/cneigh/nga_veg/in_DEM/dem_tmp/tifs',lookFor='dem'):
    """
    Create List
    """
    inputList = open(direct+'/inputList.txt','w')
    roots = []
    filecounter = 0
    for root, dirs, files in os.walk(direct):
        for each in files:
            # Just look for a specific file type or name
            if len(each.split(lookFor)) > 1:
                if each.endswith('.tif'):
                    inFile = root + '/' + each
                    roots.append(inFile)
                    inputList.write(inFile+'\n')

    inputList.close()

    print 'Number of files: ',str(filecounter)
    vrtFile = direct + '/' + direct.split('/')[-1]
    print 'Creating VRT file for: ',
    cmdStr = 'gdalbuildvrt -input_file_list '+ direct+'/inputList.txt' +" " + vrtFile+'.vrt'
    print subprocess.Popen(cmdStr, stdout=subprocess.PIPE,stderr=subprocess.PIPE, shell=True).stderr.read()
    print subprocess.Popen(cmdStr, stdout=subprocess.PIPE,stderr=subprocess.PIPE, shell=True).stdout.read()
    ## http://spectraldifferences.wordpress.com/2014/03/02/recursively-finding-files-in-python-using-the-find-command/
    out = subprocess.Popen(cmdStr, shell=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    (stdout, stderr) = out.communicate()

def foot_tiles(
    direct,
    lookFor,    #='dem',
    srs,
    rasterExt='.tif'):
    #
    ###############################################
    """
    Footprint shapefile goes to dir just above the data
    """

    # Collect file names in working directory and subfolders therein

    namesList = []
    pathroot = []
    roots = []
    for root, dirs, files in os.walk(direct):
        for file in files:
            if file.endswith(rasterExt) or file.endswith(rasterExt.upper()):
                if lookFor in file:
                    root = root.replace('//','/').replace('\\','/')
                    roots.append(root + '/' + file)
                    namesList.append(file)
                    pathroot.append(root)

    ###############################################
    # Use gdalinfo to make metadata txt from each raster

    a = 0
    gdalRan = False
    while a < len(roots):
        textname = roots[a].strip(rasterExt).strip(rasterExt.upper())
        if list(direct)[0] != list(textname)[0]:
            textname = list(direct)[0] + textname
        # If the metadata  txt doesnt exist, run gdalinfo
        if not os.path.isfile(textname+'_metadata.txt'):
            sys.stdout = open(textname+'_metadata.txt','w') #ready to collect gdalinfo output and dump into txt file
            gdal.SetConfigOption('NITF_OPEN_UNDERLYING_DS', 'NO')
            try:
                gdalinfo.main(['foo',roots[a]]) #'foo' is a dummy variable
                gdalRan = True
            except:
                print('cannot run gdalinfo on ' + textname + rasterExt)
        a += 1
    if gdalRan:
        sys.stdout.close()
        reload(sys)
        print(str(a) + ' metadata txt files successfully created')
    else:
        print('Metadata txt files already exist')
    ###############################################
    ###############################################
    # Create csv to hold metadata

    if direct.split('/')[1] == '': #script called at drive root
        fileName = direct.strip('/').strip(':')
    else: #script called at subdirectory
        fileName = direct.split('/')[len(direct.split('/'))-1]

    csvOut = open(direct+'/'+fileName+'_metadata.csv', 'w') #named for folder containing imagery
    csvOut.write('Name,ullon,ullat,lllon,lllat,urlon,urlat,lrlon,lrlat\n') #header file_name,UL1,UL2,LL1,LL2,UR1,UR2,LR1,LR2)

    ###############################################
    # Export metadata.txt info to a directory metadata csv

    print('COORDINATE EXTRACTION')

    # Counter and list to hold names of images with very bad coordinates
    badFileCounter = 0
    badCoordFilesList = []

    # Loop through metadata txt files and/or the .XML files and extract tags and coordinates
    fileCounter = 0
    while fileCounter < len(namesList):
        names = roots[fileCounter].strip(rasterExt).strip(rasterExt.upper())

        if list(direct)[0] != list(names)[0]:
            names = list(direct)[0] + names
        # This depends on having corresponding NTF and metadata files...
        #   This could be problematic if unexpected raster file name format is encountered
        myfile = open(names+'_metadata.txt', 'r')
        lines = myfile.readlines()

        # Initialize variables which should be overwritten below
        coordinateList= []
        UL = ''
        LL = ''
        UR = ''
        LR = ''
        ullon = ''
        ullat = ''
        urlon = ''
        urlat = ''
        lllon = ''
        lllat = ''
        lrlon = ''
        lrlat = ''

        # Look at each line in metadata txt and pull out useful tags
        coordinateList = [] #throw out any and all coordinates recorded so far
        for line in lines:
            if 'Upper Left' in line:
                UL      = line.replace('(',')').split((')'))[1].replace(',','')
                ullon = float(line.replace('(',')').split((')'))[1].split(',')[0])
                ullat = float(line.replace('(',')').split((')'))[1].split(',')[1])
            if 'Lower Left' in line:
                LL      = line.replace('(',')').split((')'))[1].replace(',','')
                lllon = float(line.replace('(',')').split((')'))[1].split(',')[0])
                lllat = float(line.replace('(',')').split((')'))[1].split(',')[1])
            if 'Upper Right' in line:
                UR      = line.replace('(',')').split((')'))[1].replace(',','')
                urlon = float(line.replace('(',')').split((')'))[1].split(',')[0])
                urlat = float(line.replace('(',')').split((')'))[1].split(',')[1])
            if 'Lower Right' in line:
                LR      = line.replace('(',')').split((')'))[1].replace(',','')
                lrlon = float(line.replace('(',')').split((')'))[1].split(',')[0])
                lrlat = float(line.replace('(',')').split((')'))[1].split(',')[1])


        # If UL, LL, UR, LR in hand, write metadata into csv row
    ##        if len(coordinateList) == 4:
    ##            coords = ', '.join(coordinateList)
        csvOut.write(namesList[fileCounter]+','+str(ullon)+','+str(ullat)+','+str(lllon)+','+str(lllat)+','+str(urlon)+','+str(urlat)+','+str(lrlon)+','+str(lrlat)+'\n')

        myfile.close()

        if os.path.exists(names + '.xml') or os.path.exists(names + '.XML'):
            myfileXML.close()

        fileCounter += 1

    print('')
    print('METADATA TXT TO CSV')


    csvOut.close()

    ###############################################
    # Prep to export csv contents to shp

    csvDBF = open(direct+'/'+fileName +'_metadata.csv','r') #output from NTF-2_build_csv.py
    shpOut = shp.Writer(shp.POLYGON)

    ###############################################
    # Copy csv header into shp header

    # Write shp header
    # Note: all fields are cast as strings ('C') with length, 80, by default
    shpHeader = csvDBF.readline()
    i=0
    while i < len(shpHeader.split(',')):
        if i == len(shpHeader.split(','))-1: #if last element
           shpOut.field(shpHeader.split(',')[i].strip('\n'),'C','80')
        else:
           shpOut.field(shpHeader.split(',')[i],'C','80')
        i+=1

    ###############################################
    # Create shp features
    print('')
    print('SHP/KML EXPORTS')
    #print('created footprints for:')

    #holder for shp-formatted kml feature values
    kmlFeatureList = []

    countRast = 0
    for line in csvDBF.readlines():
        # Extract corner coords
##        polyLL = line.split(',')[1].split('  ')
##        polyLL = [float(polyLL[1]),float(polyLL[0])]
##        polyLR = line.split(',')[2].split('  ')
##        polyLR = [float(polyLR[1]),float(polyLR[0])]
##        polyUR = line.split(',')[3].split('  ')
##        polyUR = [float(polyUR[1]),float(polyUR[0])]
##        polyUL = line.split(',')[4].split('  ')
##        polyUL = [float(polyUL[1]),float(polyUL[0])]
        polyUL = [float(line.split(',')[1]),float(line.split(',')[2])]
        polyLL = [float(line.split(',')[3]),float(line.split(',')[4])]
        polyUR = [float(line.split(',')[5]),float(line.split(',')[6])]
        polyLR = [float(line.split(',')[7]),float(line.split(',')[8])]


        # Length of record has to equal number of fields

        file_name=line.split(',')[0]

        #LL=line.split(',')[10].split(' ')[0]+','+line.split(',')[10].split(' ')[1]
        UL1 = float(line.split(',')[1])
        UL2 = float(line.split(',')[2])
        UL = "%.5f" % UL1 + ',' + "%.5f" % UL2

        LL1 = float(line.split(',')[3])
        LL2 = float(line.split(',')[4])
        LL = "%.5f" % LL1 + ',' + "%.5f" % LL2

        UR1 = float(line.split(',')[5])
        UR2 = float(line.split(',')[6])
        UR = "%.5f" % UR1 + ',' + "%.5f" % UR2

        LR1 = float(line.split(',')[7])
        LR2 = float(line.split(',')[8])
        LR = "%.5f" % LR1 + ',' + "%.5f" % LR2

        # Create shp DBF and record geometry
        shpOut.record(file_name,UL1,UL2,LL1,LL2,UR1,UR2,LR1,LR2)
        shpOut.poly(parts=[[polyLL, polyLR, polyUR, polyUL, polyLL]])

        countRast+=1

    print('shpOut: ' + direct+'/'+fileName)
    #shpOut.save(direct+'/'+fileName) #shp name is directory's name
    shpOut.save(os.path.dirname(direct) + '/' + fileName)
    csvDBF.close()


    ###############################################
    # Create the shp prj file
    # http://geospatialpython.com/2011/02/create-prj-projection-file-for.html

    # Using lat-lon by default
    prj = open(os.path.dirname(direct)+'/'+fileName+'.prj', "w")
    print('srs = ' + srs)
    if 'geog' in srs:
        epsg = 'GEOGCS["WGS 84",'
        epsg += 'DATUM["WGS_1984",'
        epsg += 'SPHEROID["WGS 84",6378137,298.257223563]]'
        epsg += ',PRIMEM["Greenwich",0],'
        epsg += 'UNIT["degree",0.0174532925199433]]'
##    if 'sin' in srs:
##        epsg = 'PROJCS["MODIS Sinusoidal",GEOGCS["GCS_WGS_1984",'
##        epsg += 'DATUM["D_WGS_1984",'
##        epsg += 'SPHEROID["WGS_1984",6378137,298.257223563]],'
##        epsg += 'PRIMEM["Greenwich",0],'
##        epsg += 'UNIT["Degree",0.017453292519943295]],'
##        epsg += 'PROJECTION["Sinusoidal"],'
##        epsg += 'PARAMETER["false_easting",0.0],'
##        epsg += 'PARAMETER["false_northing",0.0],'
##        epsg += 'PARAMETER["central_meridian",0.0],'
##        epsg += 'PARAMETER["semi_major",6371007.181],'
##        epsg += 'PARAMETER["semi_minor",6371007.181],UNIT["m",1.0]]'

    if 'sin' in srs:
        epsg = 'PROJCS["unnamed",'
        epsg += 'GEOGCS["unnamed ellipse",'
        epsg += 'DATUM["unknown",'
        epsg += 'SPHEROID["unnamed",6371007.181,0]],'
        epsg += 'PRIMEM["Greenwich",0],'
        epsg += 'UNIT["degree",0.0174532925199433]],'
        epsg += 'PROJECTION["Sinusoidal"],'
        epsg += 'PARAMETER["longitude_of_center",0],'
        epsg += 'PARAMETER["false_easting",0],'
        epsg += 'PARAMETER["false_northing",0],'
        epsg += 'UNIT["metre",1,'
        epsg += 'AUTHORITY["EPSG","9001"]]]'
        #+proj=sinu +lon_0=0 +x_0=0 +y_0=0 +a=6371007.181 +b=6371007.181 +units=m +no_defs

    prj.write(epsg)
    prj.close()

