#!/usr/bin/python

# Import and function definitions
import os, sys, math, osgeo, shutil, time, glob, gdalinfo, platform, csv, subprocess as subp
from osgeo import ogr, osr, gdal
from datetime import datetime
from timeit import default_timer as timer
from time import gmtime, strftime

import struct
import numpy as np
from collections import defaultdict
import csv
import argparse

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

    # Need to find intersection of featExtent and rasterExtent here

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

            # Set up datamask that is filled with 1's
            bandmask = target_ds.GetRasterBand(1)
            datamask = bandmask.ReadAsArray(0, 0, xcount, ycount)##.astype(np.float)

            if geom.GetGeometryName() == 'POINT':
                # For points, this has to be done, otherwise you get 0s for all but the center position...
                datamask.fill(1)

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

def getparser():
    parser = argparse.ArgumentParser(description="Get zonal stats for input points")
    parser.add_argument('in_shp', default=None, help='Path to shapefile')
    parser.add_argument('value_ras_fn', default='None', help='Value raster feature name')
    parser.add_argument('-buf_dist', default=15, type=int, help='Buffer distance for points (raster cs units)')
    parser.add_argument('-outDir', default='None', type=str, help='Output directory for csv')
    return parser

def main():

    parser = getparser()
    args = parser.parse_args()

    ras_dir = args.in_shp
    value_ras_fn = args.value_ras_fn
    buf_dist = args.buf_dist
    outDir = args.outDir

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
    fieldnames = ["site","lat","lon"]
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
        ##fieldDict_rec[FID]         = feat.GetField("rec_ndx")
        ##fieldDict_shot[FID]        = feat.GetField("shotn")
        fieldDict_lat[FID]         = feat.GetField("site")
        fieldDict_lat[FID]         = feat.GetField("lat")
        fieldDict_lon[FID]         = feat.GetField("lon")
        ##fieldDict_elev[FID]        = feat.GetField("elev")
        ##fieldDict_elev_ground[FID] = feat.GetField("elev_groun") # <-- this fieldname is too long and gets truncated
        ##fieldDict_elvd[FID] = feat.GetField("elvdiff")
        ##fieldDict_cent[FID] = feat.GetField("Centroid")
        #fieldDict_wflen[FID] = feat.GetField("wflen")

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
if __name__ == '__main__':
    main()
