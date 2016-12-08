#-------------------------------------------------------------------------------
# Name:        workflow_functions.py
# Purpose:
#
# Author:      pmontesa
#
# Created:     21/12/2015
# Copyright:   (c) pmontesa 2015
# Licence:     <your licence>
#-------------------------------------------------------------------------------
# #!/bin/python
# #!/usr/bin/env python
###############################################
# Import and function definitions
import os, sys, math, osgeo, shutil, time, glob, gdalinfo, platform, csv, subprocess as subp
from osgeo import ogr, osr, gdal
from datetime import datetime
from timeit import default_timer as timer
from time import gmtime, strftime
gdal.AllRegister() #register all raster format drivers
import struct
import numpy as np
from collections import defaultdict
import csv

##wf.loop_zonal_stats('/att/nobackup/pmontesa/DSM_ssg/Siberia_FIELD_GLAS_plots_utm47_buf_10m.shp','/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV01_20130604_1020010023E3DB00_1020010024C5D300/out-strip-holes-fill-DEM.tif')


def mos_map_ms(inDir,outDir,catIDleft, inDSM):
    """
    inDir:      dir where the raw, renamed data sits
    outDir:     dir where the outASP data is put: e.g., .../outASP/WV2_20130101_CATID1_CATID2/
    catIDleft:  catalog ID of the left image mosaic (no need to do both)
    inDSM:      the DSM produced from the pan data

    If there are WV2 MS bands available for a catID used for making a DSM
        1. mosaic each band and
        2. maproject each band using DSM
        3. stack bands
        4. save in outDir with name WV2_20130101_CATID1

    Note: run after point2dem
    """
    inSearchCat = "*" + catID + "*M1BS*" + ".tif"
    for b in [1,2,3,4]:
        cmdStr = "dg_mosaic " + inSearchCat + "--band=" + str(b) + " --reduce-percent=100 -o " + WV02_20140412_103001002F6A3D00

## https://www.calazan.com/how-to-check-if-a-file-is-locked-in-python/
def is_locked(filepath):
    """Checks if a file is locked by opening it in append mode.
    If no exception thrown, then the file is not locked.
    """
    locked = None
    file_object = None
    if os.path.exists(filepath):
        try:
            print "Trying to open %s." % filepath
            buffer_size = 8
            # Opening file in append mode and read the first 8 characters.
            file_object = open(filepath, 'a', buffer_size)
            if file_object:
                print "%s is not locked." % filepath
                locked = False
        except IOError, message:
            print "File is locked (unable to open in append mode). %s." % \
                  message
            locked = True
        finally:
            if file_object:
                file_object.close()
                print "%s closed." % filepath
    else:
        print "%s not found." % filepath
    return locked

def wait_for_files(filepaths):
    """Checks if the files are ready.

    For a file to be ready it must exist and can be opened in append
    mode.
    """
    wait_time = 5
    for filepath in filepaths:
        # If the file doesn't exist, wait wait_time seconds and try again
        # until it's found.
        while not os.path.exists(filepath):
            print "%s hasn't arrived. Waiting %s seconds." % \
                  (filepath, wait_time)
            time.sleep(wait_time)
        # If the file exists but locked, wait wait_time seconds and check
        # again until it's no longer locked by another process.
        while is_locked(filepath):
            print "%s is currently in use. Waiting %s seconds." % \
                  (filepath, wait_time)
            time.sleep(wait_time)

def get_raster_extent(raster):
    """
    Input: an opened raster file (from gdal.Open(input_raster))
    Return: the raster extent as an array --> [xmin, ymax, xmax, ymin].
        Note the 'Origin' of the raster is the upper left.
    """
    gt = raster.GetGeoTransform()
    xOrigin = gt[0]
    yOrigin = gt[3]
    pixelWidth = gt[1]
    pixelHeight = gt[5]

    # Get extent of raster
    ## [left, top, right, bottom]
    rasterExtent = [xOrigin, yOrigin, xOrigin + (pixelWidth * raster.RasterXSize), yOrigin + (pixelHeight * raster.RasterYSize)]

    return rasterExtent, xOrigin, yOrigin, pixelWidth, pixelHeight

def loop_zonal_stats(input_zone_polygon, input_value_raster, pointBuf,rasteridtail,outDir):
    ## https://gis.stackexchange.com/questions/77993/issue-trying-to-create-zonal-statistics-using-gdal-and-python
    """
    Input:
        input_zone_polygon      a zone shapefile
        input_value_raster      a raster in UTM
        pointBuf                a distance (m) to add to & substract from both X and Y to create an area
        rasteridtail            a string to help distinguish the output CSV files
    Output:
        a CSV with fields: FID, mn, std
            FID corresponds to FID in input zone polygon
            stored in dir as input_zone_polygon
            name includes combo of input zone poly and raster (might wanna change this to suite your needs)
    Return:
        stat dictionary
    """
    # Reproject input_zone_polygon to srs of raster
    # Get SRS and UTM zone from raster
    raster = gdal.Open(input_value_raster)
    srs = osr.SpatialReference(wkt=raster.GetProjection())
    zone = srs.GetAttrValue('projcs').split('zone ')[1][:-1]        # removes the last char, which is 'N' or 'S'
    print "\n\tUTM Zone for the reprojection: %s" %zone
    if int(zone) < 10:
        zone = "0" + str(zone)

    # Set up tmp zone file that will be projected to srs of raster
    name = input_value_raster.split('/')[-2]
    rasterid = name + rasteridtail   ## eg: 'WV01_20130604_1020010023E3DB00_1020010024C5D300dsm'

    tmpShp = input_zone_polygon.split('.shp')[0] + '_' + platform.node() + '_' + strftime("%Y%m%d_%H%M%S") + "_tmp.shp"
    driver = ogr.GetDriverByName("ESRI Shapefile")

    # Wait for the input_zone_polygon in case it is being used by another file
    wait_for_files([input_zone_polygon])

##    if os.path.exists(rpShp):
##        # Check if UTM zone matches that of raster
##        shp = ogr.Open(rpShp)
##        lyr = shp.GetLayer()
##
##        # if no match, delete and reproject
##        driver.DeleteDataSource(tmpShp)
##        print '\tReprojected version of shapefile exists...deleting'

    ## ogr2ogr targetSRS output input
    rExt, xOrigin, yOrigin, pixelWidth, pixelHeight = get_raster_extent(raster)
    rExtStr = str(rExt[0]) + " " + str(rExt[1]) + " " + str(rExt[2])+ " " + str(rExt[3])
    print '\n\tReproject input poly to UTM zone matching raster AND clip to raster extent...'
    cmdStr = "ogr2ogr -t_srs EPSG:326" + zone + " -clipdst " + rExtStr + " " + tmpShp + " " + input_zone_polygon
    run_wait_os(cmdStr)

##    tmpBox = input_zone_polygon.split('.shp')[0] + '_' + platform.node() + '_' + rasterid + "_tmp.shp"
##    print '\Run gdaltindex to get shapefile bounding box of input raster'
##    cmdStr = "gdaltindex -t_srs EPSG:326" + zone + " " + tmpBox + " " + input_value_raster

    shp = ogr.Open(tmpShp)
    lyr = shp.GetLayer()
    featList = range(lyr.GetFeatureCount())

    # Stats to return
    statsnames = [rasterid + '_mean', rasterid + '_std']
    statDict_mean = {}
    statDict_std = {}

    # Existing attributes to return
    ##fieldnames = ["campaign","rec_ndx","shotn","lat","lon","elev","elvdiff","Centroid","wflen"]
    fieldnames = ["rec_ndx","shotn","lat","lon","elev","elev_ground","wflen"]
    print "Fieldnames: %s" %fieldnames
    """
    TODO    iterate through fieldnames and append multiple fieldname values to a common dict key (FID),
                instead of having a diff dict
    """

    ##fieldDict_camp = {}
    fieldDict_rec = {}
    fieldDict_shot = {}
    fieldDict_lat = {}
    fieldDict_lon = {}
    fieldDict_elev = {}
    fieldDict_elev_ground = {}
    ##fieldDict_elvd = {}
    ##fieldDict_cent = {}
    fieldDict_wflen = {}

    # Loop through features
    for FID in featList:

        feat = lyr.GetFeature(FID)
        print '\tFID: %s' %(FID)

        # Stats to return
        meanValue, stdValue, zValList = zonal_stats(feat, tmpShp, input_value_raster, pointBuf)
        statDict_mean[FID] = meanValue
        statDict_std[FID] = stdValue

        # Existing attributes to return
        ##fieldDict_camp[FID] = feat.GetField("campaign")
        fieldDict_rec[FID]         = feat.GetField("rec_ndx")
        fieldDict_shot[FID]        = feat.GetField("shotn")
        fieldDict_lat[FID]         = feat.GetField("lat")
        fieldDict_lon[FID]         = feat.GetField("lon")
        fieldDict_elev[FID]        = feat.GetField("elev")
        fieldDict_elev_ground[FID] = feat.GetField("elev_groun") # <-- this fieldname is too long and gets truncated
        ##fieldDict_elvd[FID] = feat.GetField("elvdiff")
        ##fieldDict_cent[FID] = feat.GetField("Centroid")
        fieldDict_wflen[FID] = feat.GetField("wflen")

        # Output a csv list of the DSM values in the box around the GLAS centroid
##        if rasteridtail == 'dsm':
##            try:
##                np.savetxt(os.path.join(outDir,name + '_dsm_vals_'+ str(feat.GetField("rec_ndx")) + "_" + str(feat.GetField("shotn"))), zValList, delimiter = ",", fmt='%.2f')
##            except Exception, e:
##                print '\t' + str(e)

    # Header for output CSV
    hdr = ['FID'] + fieldnames + statsnames

    # Combine all attribute and stats dictionaries
    ##Comb_Dicts = fieldDict_camp, fieldDict_rec, fieldDict_shot, fieldDict_lat, fieldDict_lon, fieldDict_elev, fieldDict_elvd, fieldDict_cent, fieldDict_wflen, statDict_mean, statDict_std
    Comb_Dicts = fieldDict_rec, fieldDict_shot, fieldDict_lat, fieldDict_lon, fieldDict_elev, fieldDict_elev_ground, fieldDict_wflen, statDict_mean, statDict_std

    # delete the tmp reproj'd shp
    driver.DeleteDataSource(tmpShp)

    # Write stats to CSV
    path, input_zone_file = os.path.split(input_zone_polygon)

    outCSV = os.path.join(outDir,input_zone_file.split('.shp')[0] + '_' + rasterid + '.csv')

##    # Combine the 2 dictionaries into one using the key (this case, the FID)
##    ## formatting a combined dictionary to CSV is tricky..room for improvement
##    StatDict_Combined = defaultdict(list)
##
##    for d in (statDict_mean, statDict_std): # you can list as many input dicts as you want here
##        for key, value in d.iteritems():
##            StatDict_Combined[key].append(value)
##
##    with open(outCSV, 'w') as f:
##        hdr = 'FID,' + rasterid + '_elev_mn'##+',' + rasterid + '_elev_std'
##        f.write(hdr + '\n')
##        [f.write('{0},{1}\n'.format(key, value )) for key, value in StatDict_Combined.items()]
##        ## Below doesnt work on lists, only tuples..
##        ## https://stackoverflow.com/questions/5530619/python-how-to-write-a-dictionary-of-tuple-values-to-a-csv-file?rq=1
##        ##csv.writer(f).writerows(hdr + '\n')
##        ##csv.writer(f).writerows((k,) + v for k, v in StatDict_Combined.iteritems())

    # Write multiple dicts to CSV
    ## https://stackoverflow.com/questions/22273970/writing-multiple-python-dictionaries-to-csv-file
    with open(outCSV, 'w') as f:
        writer = csv.writer(f, delimiter=',')
        ##writer.writerow(['FID', rasterid + '_elev_mn', rasterid + '_elev_std'])
        writer.writerow(hdr)

        # Here you just need any template dict from above through which you can iterate the keys
        for key in statDict_mean.iterkeys():

            writer.writerow([key] + [d[key] for d in Comb_Dicts])

    return Comb_Dicts, outCSV, hdr

def zonal_stats(feat, input_zone_polygon, input_value_raster, pointBuf):
    ## https://gis.stackexchange.com/questions/77993/issue-trying-to-create-zonal-statistics-using-gdal-and-python
    """
    Does zonal stats on a feature from an input zone polygon
        input zone poly can be a point shp too - in which case it'll use the pointBuf arg
            to detemine the distance in X and Y used for creating a zone from the original point
    """
    # Open raster data
    raster = gdal.Open(input_value_raster)

    # --- Check if current feature intersects with raster extent

    # Now open up that reprojected input_zone_polygon
    shp = ogr.Open(input_zone_polygon)
    lyr = shp.GetLayer()

    # Get raster georeference info
##    gt = raster.GetGeoTransform()
##    xOrigin = gt[0]
##    yOrigin = gt[3]
##    pixelWidth = gt[1]
##    pixelHeight = gt[5]
##
##    # Get extent of raster
##    ## [left, top, right, bottom]
##    rasterExtent = [xOrigin, yOrigin, xOrigin + (pixelWidth * raster.RasterXSize), yOrigin + (pixelHeight * raster.RasterYSize)]
    rasterExtent, xOrigin, yOrigin, pixelWidth, pixelHeight = get_raster_extent(raster)

    # Get extent of feat
    geom = feat.GetGeometryRef()

    if (geom.GetGeometryName() == 'MULTIPOLYGON'):
        count = 0
        pointsX = []; pointsY = []
        for polygon in geom:
            geomInner = geom.GetGeometryRef(count)
            ring = geomInner.GetGeometryRef(0)
            numpoints = ring.GetPointCount()
            for p in range(numpoints):
                lon, lat, z = ring.GetPoint(p)
                pointsX.append(lon)
                pointsY.append(lat)
            count += 1

    elif (geom.GetGeometryName() == 'POLYGON'):
        ring = geom.GetGeometryRef(0)
        numpoints = ring.GetPointCount()
        pointsX = []; pointsY = []
        for p in range(numpoints):
            lon, lat, z = ring.GetPoint(p)
            pointsX.append(lon)
            pointsY.append(lat)

    elif (geom.GetGeometryName() == 'POINT'):
        # Create 3 points:
        #   center (actual xy of point) and an UR & LL based on a buffer distance of pointBuf
        pointsX = []; pointsY = []
        pointsX.append(geom.GetX())
        pointsX.append(geom.GetX() + pointBuf)
        pointsX.append(geom.GetX() - pointBuf)
        pointsY.append(geom.GetY())
        pointsY.append(geom.GetY() + pointBuf)
        pointsY.append(geom.GetY() - pointBuf)

    else:
        sys.exit()

    # Get the extent of the current feature
    xmin = min(pointsX)
    xmax = max(pointsX)
    ymin = min(pointsY)
    ymax = max(pointsY)
    ## [left, bottom, right, top]
    featExtent = [xmin,ymax,xmax,ymin]
##    print '\t        -         Bounds: [xmin,ymin,xmax,ymax]'
##    print '\t        - Feature bounds: %s' % str(featExtent)
##    print '\t        - Raster bounds: %s' % str(rasterExtent)

    # Need to find intersection of featExtent and rasterExtent here
    ## max of the lefts (xmins), [0]
    ## min of the tops
    ## min of the rights
    ## max of the bottoms
    intersection = [max(rasterExtent[0], featExtent[0]) , \
                    min(rasterExtent[1], featExtent[1]) , \
                    min(rasterExtent[2], featExtent[2]) , \
                    max(rasterExtent[3], featExtent[3]) ]

    if rasterExtent != featExtent:
        print '\tLooking for overlap (intersection) b/w feature and raster...'
        # check for any overlap at all...
        if (intersection[2] < intersection[0]) or (intersection[1] < intersection[3]):
            intersection = None
            print '\t***No overlap. Returning np.nan value for zonal statistics'
            return np.nan, np.nan
        else:
            print '\tHere is the overlap (intersection):',intersection
            # Specify offset and rows and columns to read
            xoff = int((xmin - xOrigin)/pixelWidth)
            yoff = int((yOrigin - ymax)/pixelWidth)
            xcount = int((xmax - xmin)/pixelWidth)+1
            ycount = int((ymax - ymin)/pixelWidth)+1

            # print '\t Create memory target raster...'
            target_ds = gdal.GetDriverByName('MEM').Create('', xcount, ycount, gdal.GDT_Byte)
            target_ds.SetGeoTransform((
                xmin, pixelWidth, 0,
                ymax, 0, pixelHeight,
            ))

            # Create for target raster the same projection as for the value raster
            raster_srs = osr.SpatialReference()
            raster_srs.ImportFromWkt(raster.GetProjectionRef())
            target_ds.SetProjection(raster_srs.ExportToWkt())

            # print '\t Rasterize zone polygon to raster, fill with 1's...'
            gdal.RasterizeLayer(target_ds, [1], lyr, burn_values=[1])

            # print '\tRead raster as arrays...'
            banddataraster = raster.GetRasterBand(1)
            try:
                dataraster = banddataraster.ReadAsArray(xoff, yoff, xcount, ycount).astype(np.float)
            except Exception, e:
                print '\t' + str(e)
                dataraster = banddataraster.ReadAsArray(xoff, yoff, xcount, ycount)

##            print 'dataraster'
##            print dataraster

            # Set up datamask that is filled with 1's
            bandmask = target_ds.GetRasterBand(1)
            datamask = bandmask.ReadAsArray(0, 0, xcount, ycount)##.astype(np.float)
            if geom.GetGeometryName() == 'POINT':
                # For points, this has to be done, otherwise you get 0s for all but the center position...
                datamask.fill(1)
##                print 'datamask'
##                print datamask

##            print '/t xoff + xcount = ' + str(xoff) + ' + ' + str(xcount) + ' = ' + str(np.add(xoff,xcount))
##            print '/t raster X size = ' + str(raster.RasterXSize)
##            print '/t yoff + ycount = ' + str(yoff) + ' + ' + str(ycount) + ' = ' + str(np.add(yoff,ycount))
##            print '/t raster Y size = ' + str(raster.RasterYSize)

            # Mask zone of raster
            try:
                zoneraster = np.ma.masked_array(dataraster,  np.logical_not(datamask))
                zoneraster[zoneraster <= -99.] = np.nan

                try:
                    # Get a masked array that prevents nans from interfering
                    ##https://stackoverflow.com/questions/5480694/numpy-calculate-averages-with-nans-removed
                    m_zoneraster = np.ma.masked_array(zoneraster,np.isnan(zoneraster))

                    # Calculate statistics of zonal raster
                    ##print '\t std: ' + str(round(np.std(zoneraster),2))
                    ##return round(np.mean(zoneraster),2), round(np.std(zoneraster),2)
                    print '\t std: ' + str(round(m_zoneraster.std(),2))

                    zValList = dataraster.flatten()
                    return round(m_zoneraster.mean(),2), round(m_zoneraster.std(),2), zValList

                except Exception, e:
                    print '\t' + str(e)

                    return np.nan, np.nan, None

            except Exception, e:
                print '\t No stats for features straddling edge of raster.'

                return np.nan, np.nan, None


def run_wait_os(cmdStr, print_stdOut=True):
    """
    Initialize OS command
    Wait for results (Communicate results i.e., make python wait until process is finished to proceed with next step)
    """
    import subprocess as subp

    Cmd = subp.Popen(cmdStr.rstrip('\n'), stdout=subp.PIPE, shell=True)
    stdOut, err = Cmd.communicate()

    if print_stdOut:
        print ("\tInitialized: %s" %(cmdStr))
        print ("\t..Waiting for command to run...")
        print("\t" + str(stdOut) + str(err))
        print("\tEnd of command.")

def run_os(cmdStr):
    """
    Initialize OS command
    Don't wait for results (don't communicate results i.e., python code proceeds immediately after initializing script)
    """
    import subprocess as subp

    Cmd = subp.Popen(cmdStr.rstrip('\n'), stdout=subp.PIPE, shell=True)
    #stdOut, err = Cmd.communicate()

    print ("\tInitialized: %s" %(cmdStr))
    print ("\t\tMoving on to next step.")

def findFile(name, path):
    # For finding a specific file within a top level dir
    for root, dirs, files in os.walk(path):
        if name in files:
            return os.path.join(root, name)

def any(iterable):
    # Define the 'any' function which isnt available in Python 2.4
    for element in iterable:
        if element:
            return True
    return False

def filtFunc(s, stringList):
    # Dont list images with certain strings in their names
    return not any(x in s for x in stringList)

## https://stackoverflow.com/questions/4417546/constantly-print-subprocess-output-while-process-is-running
def execute(cmd):
    popen = subp.Popen(cmd, stdout=subp.PIPE, universal_newlines=True)
    stdout_lines = iter(popen.stdout.readline, "")
    for stdout_line in stdout_lines:
        yield stdout_line

    popen.stdout.close()
    return_code = popen.wait()
    if return_code != 0:
        raise subp.CalledProcessError(return_code, cmd)

def stereoAngles(alpha1,theta1,alpha2,theta2,x1,y1,z1,x2,y2,z2,lat,lon):
    """
    alpha1  =   meanSatEl of image 1
    theta1  =   meanSatAz of image 1
    alpha2  =   "" image 2
    theta2  =   "" image 2
    x,y,z   = satellite empheris

    http://www.mdpi.com/2072-4292/7/4/4549/remotesensing-07-04549.pdf
    http://www.geoimage.com.au/media/brochure_pdfs/DEMBrochure_FEB2015.pdf
    www.isprs.org/proceedings/XXXVII/congress/1_pdf/195.pdf

    In the case of the VHR satellites with their pointable telescopes, the B/H ratio is not appropriate as a measure
    of the effectiveness of the stereo pair for DEM generation. In such cases, three angular measures of
    convergent stereo imaging geometry: the convergence angle, the asymmetry angle, and the bisector elevation angle (BIE) are used.

    These measure the geometrical relationship between two rays that intersect at a common ground point, one
    from the fore image and one from the aft image as shown in the diagram.

    Convergence Angle:
    The angle between two rays of a stereo pair
    The most important of the three stereo angles is the convergence and is the angle between the two rays in the
    convergence or epipolar plane. An angle between 30 and 60 degrees is ideal (<--- ideal for measuring what?? which heights? short trees??)

    Asymetry Angle:
    Asymmetry describes the apparent offset from the centre view that a stereo pair has. For instance, a stereo pair
    with an asymmetry of 0? will have parallax due to elevations that appear equivalent in the left and right images.
    An asymmetrical collection is preferred as it gives a different look angle to discern ground features more
    accurately but should be under 20 deg.

    Bisector Elevation Angle:
    The obliqueness of the epipolar plane. BIE = 90 is orthogonal to ground surface
    The elevation angle of the bisector of the convergence angle
    The BIE angle is the angle between the horizontal plane and the epipolar plane and defines the amount of parallax that will
    appear in the vertical direction after alignment. The angle should be between 60 and 90 degrees.
    """
    # Converts degrees to radians
    dtr = math.atan(1.0)/45.0

    # Set Earth Radius
    r = 6378137   # WGS84 equatorial earth radius in meters

    a = math.sin(alpha1*dtr) * math.sin(alpha2*dtr)+ math.cos(alpha1*dtr) * math.cos(alpha2*dtr)* math.cos((theta1-theta2)*dtr)
    con_ang = math.acos(a)/dtr

    x0 = r * math.cos(lat*dtr) * math.cos(lon*dtr)
    y0 = r * math.cos(lat*dtr) * math.sin(lon*dtr)
    z0 = r * math.sin(lat*dtr)

    a = det3(y0,z0,1.0,y1,z1,1.0,y2,z2,1.0)
    b = -det3(x0,z0,1.0,x1,z1,1.0,x2,z2,1.0)
    c = det3(x0,y0,1.0,x1,y1,1.0,x2,y2,1.0)

##    print alpha1,alpha2,theta1,theta2
##    print a,b,c
##    print x0,y0,z0

    if int(a) == 0 or int(b) == 0 or int(c) == 0:
        return (-99999,-99999,-99999)
    else:
        sc = abs(a*x0 + b*y0 + c*z0)/(math.sqrt(x0*x0+y0*y0+z0*z0) * math.sqrt(a*a + b*b + c*c))
        bie_ang = math.asin(sc)/dtr
        a = x1+x2-2*x0
        b = y1+y2-2*y0
        c = z1+z2-2*z0
        sc = abs(a*x0 + b*y0 + c*z0)/(math.sqrt(x0*x0+y0*y0+z0*z0) * math.sqrt(a*a + b*b + c*c))
        asym_ang = math.asin(sc) / dtr
        return (con_ang,asym_ang,bie_ang)
