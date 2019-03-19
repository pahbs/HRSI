"""
Purpose: To create a GLAS point shapefile and corresponding buffered shapefile that can be used in zonal stats
Process: Get GCS extent from UTM raster, find corresponding GLAS .csv files, filter points to make a point shapefile for GLAS shots, then buffer resulting point shapefile

* Currently does not work stand-alone, but with run_GLAS_zonal_database.py

Notes:
   - To run the buffer portion you must have GEOS support enabled. On ADAPT: source /opt/PGSCplus-2.2.2/init-gdal.sh
   - GLAS shots are excluded from output shapefile if they: are outside of raster extent, are missing columns, have longitude > 360, do not pass the 3 flag qualifications
   - Four columns are added to output shapefile: uniqueID for zonal stats, laserID, shot year, and shot day (julian day). And the 0-360 longitude is replace by (-180, 180) long.
"""

import os, sys
import shapefile as shp
from rasterstats import point_query
from shapely.geometry import Point
import csv
from osgeo import gdal,ogr,osr

from GLAS_functions import Parameters as params
import GLAS_functions as functions

import datetime

def create_pointShp_fromRasterExtent(rasterStack, outShpDir):

    # import variables, get list of csv, etc
    GLAS_csv_dir = params.GLAS_csv_dir
    GLAS_csv_list = functions.make_GLAS_csv_list(rasterStack, GLAS_csv_dir)
##    print GLAS_csv_list # temp
    (xmin, ymin, xmax, ymax) = functions.get_gcs_extent(rasterStack)
    raster_prj, raster_srs, raster_epsg = functions.get_proj_info(rasterStack)
    stackExt = os.path.splitext(rasterStack)[1] # could be either tif or vrt
    stackName = os.path.basename(rasterStack).strip('_stack{}'.format(stackExt)).strip(stackExt)
    outShpPath = os.path.join(outShpDir, '{}.shp'.format(stackName))
    outShpPath_wgs = outShpPath.replace('.shp', '_WGS84.shp')

    noDataMask = rasterStack.replace(stackExt, '_mask_proj.tif')

    print "Processing shapefile for raster {}".format(rasterStack)
    if os.path.isfile(noDataMask): print " NoData Mask used: {}".format(noDataMask)
    print " GLAS csv directory: {}".format(GLAS_csv_dir)
    print " GLAS csv list: {}".format(GLAS_csv_list)
    print " Output shapefile: {}\n".format(outShpPath)


    # Create the framework for the shapefile
#    outShp = shp.Writer(shp.POINT)
    outShp = shp.Writer(outShpPath_wgs, shapeType = shp.POINT) # 11/27 new version of pyshp requires this
    outShp.autoBalance = 1

##    # temp to write to .csv
##    testC = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/test.csv'

    hdr_row = None # this will be only gotten once for the first GLAS csv
    uid = 0 # this will get increminted by 1 each time a point gets added to shp
    for inCsv in GLAS_csv_list:
        with open(inCsv, 'r') as csvF:

            # first get the column names from header, and create a list of fields (header plus others -- laserID, shotYear, shotDay, uniqueID) for output
            if not hdr_row: # Only do this is we are on the first csv
                hdr_row = csvF.readline().strip()
                fld_row = 'uniqueID,laserID,shotYear,shotDay,{}'.format(hdr_row)
                hdr_list = hdr_row.split(',')
                fld_list = fld_row.split(',')
                for f in fld_list: outShp.field(f)
            else: h = csvF.readline().strip() # still need to skip the row

            for row in csvF.readlines():

                row = row.strip().strip(',') # some erroneous commas at the end
                row_list = row.split(',')

        	    # Now we need to do some filtering to decide whether or not to add to the shp. Throw out if: lon > 360 or if lat/lon is outside of padded by 0.05 deg. extent
                lat = float(row_list[hdr_list.index('lat')])
                lon_uncorr = float(row_list[hdr_list.index('lon')])


                if lon_uncorr > 360: # Check 1: If longitude is > 360, throw it out
##                    print "cannot use row {}".format(row) # temp
                    continue # continue to the next point

                # only "correct" longitude for eastern hemisphere
                if lon_uncorr > 180:
                    lon = lon_uncorr - 360
                else: lon = lon_uncorr


                # Now throw out point if it is outside a buffer of extent
                extBuff = 0.00000000003 # in degrees
                if (lat > ymax+extBuff) or (lat < ymin-extBuff) or \
                  (lon < xmin-extBuff) or (lon > xmax+extBuff):
##                    print "cannot use point {}, {}. outside of AOI extent".format(lat, lon) # temp
                    continue

                # if a no data mask () is supplied, make sure points are not in no data area (1 or None = NoData). keep where mask = 0
                if os.path.isfile(noDataMask):
                    pt_geom = Point(lon, lat)
                    if point_query([pt_geom], noDataMask)[0] != 0.0: # 0 = Data. 1 and None = NoData
                        continue # skip, don't include point

                # this needs to be before the filtering because it will fail if all cols arent there for a row
                if len(row_list) != len(hdr_list): continue # temporary for now. Figure out with paul/guoqing

                # Lastly, throw out point if the three conditions are not all met:
                # FRir_qa_flg = 15 and satNdx < 2 and cld1_mswf_flg < 15
                if int(row_list[hdr_list.index('FRir_qaFlag')]) != 15 or \
                   int(row_list[hdr_list.index('satNdx')]) >=2 or \
                   int(row_list[hdr_list.index('cld1_mswf')]) >= 15:

##                    print "cannot use point with flags:" # temp
##                    print row_list[hdr_list.index('FRir_qaFlag')], row_list[hdr_list.index('satNdx')], row_list[hdr_list.index('cld1_mswf')] # temp
                    continue

                # get the additional columns and create the output row
                rndx = int(row_list[hdr_list.index('rec_ndx')])
                lID, year = functions.get_year_laserID_from_recndx(rndx) # this might not return anything if recndx is not in list

                # get the shot day (julian) and year from the date column
                # Date column represents days since January 1, 2013
                daysSinceStart = float(row_list[hdr_list.index('date')])
                startDate = datetime.datetime.strptime('2003-01-01', "%Y-%m-%d")
                shotDate = startDate + datetime.timedelta(days=daysSinceStart)
                shotYear = shotDate.timetuple().tm_year
                shotDay = shotDate.timetuple().tm_yday

##                # temp block
##                outRow = '{},{},{},{}'.format(uid, lID, year, row)
##                outRow = outRow.replace(str(lon_uncorr), str(lon))
##                with open(testC, 'a') as tc:
##                    tc.write('{}\n'.format(outRow))

##                if not lID: continue #lID, year = '2aa', 2000 # temporary for now. Figure out with paul/guoqing # don't skip anymore since we do have date

                # At this point, we know we are adding this point to the shp
                uid += 1

                outRow = '{},{},{},{},{}'.format(uid, lID, shotYear, shotDay, row)
                outRow = outRow.replace(str(lon_uncorr), str(lon)) # also replace the uncorrected longtidue with the corrected one.
                outRow_list = outRow.split(',')

                # now we can use the point/row to build the shp
                outShp.point(lon,lat) # create point geometry
                outShp.record(*tuple([outRow_list[f] for f, j in enumerate(fld_list)]))

    if uid == 0:
        sys.exit("There were 0 GLAS shots within stack, cannot process. Quitting program")
    print "\n{} features added to shp".format(uid)

    # Save the shp. and .prj # 11/27 no longer need this, already written to outShpPath_wgs with new version of pyshp (line ??)
    print "\nClosing output shp {}...".format(outShpPath_wgs)
#    outShp.save(outShpPath_wgs.strip('.shp'))
    outShp.close() # 11/27 does not work without it. maybe garbage isnt colllected at this point yet ?

    # write the prj file
    with open(outShpPath_wgs.replace('.shp', '.prj'), 'w') as prjFile:
        prjFile.write(functions.getWKT_PRJ(4326)) # Write in WGS84 since coordinates are in decimal degrees

    # 11/27/2018 still want to "copy" outShpPath_wgs to outShpPath - via ogr2ogr below
    # And convert to UTM using ogr2ogr
    print "Reprojecting {} to {}:".format(outShpPath_wgs, outShpPath)
    reproj_cmd = 'ogr2ogr -q -t_srs EPSG:{} {} {}'.format(raster_epsg, outShpPath, outShpPath_wgs)
    print "\nReprojecting to input UTM proj (EPSG:{})....".format(raster_epsg)
    print reproj_cmd
    os.system(reproj_cmd)

    os.system('rm {}'.format(outShpPath_wgs.replace('.shp', '.*'))) # remove WGS output

    return outShpPath

def create_bufferShp_fromPointShp(inShpPath, bufferSize):

    buffShpPath = inShpPath.replace('.shp', '_buffer-{}m.shp'.format(bufferSize))

    print "\nCreating buffered shapefile..."

    inputds = ogr.Open(inShpPath)
    inputlyr = inputds.GetLayer()

    shpdriver = ogr.GetDriverByName('ESRI Shapefile')
    if os.path.exists(buffShpPath):
        shpdriver.DeleteDataSource(buffShpPath)
    outputBufferds = shpdriver.CreateDataSource(buffShpPath)
    bufferlyr = outputBufferds.CreateLayer(buffShpPath, geom_type=ogr.wkbPolygon)
    featureDefn = bufferlyr.GetLayerDefn()

    # Create new fields in the output shp and get a list of field names for feature creation
    fieldNames = []
    for i in range(inputlyr.GetLayerDefn().GetFieldCount()):
        fieldDefn = inputlyr.GetLayerDefn().GetFieldDefn(i)
        bufferlyr.CreateField(fieldDefn)
        fieldNames.append(fieldDefn.name)

    for feature in inputlyr:
        ingeom = feature.GetGeometryRef()
        fieldVals = [] # make list of field values for feature
        for f in fieldNames: fieldVals.append(feature.GetField(f))

        outFeature = ogr.Feature(featureDefn)
        geomBuffer = ingeom.Buffer(bufferSize)
        outFeature.SetGeometry(geomBuffer)
        for v, val in enumerate(fieldVals): # Set output feature attributes
            outFeature.SetField(fieldNames[v], val)
        bufferlyr.CreateFeature(outFeature)

        outFeature = None

    # Copy the input .prj file
    from shutil import copyfile
    copyfile(inShpPath.replace('.shp', '.prj'), buffShpPath.replace('.shp', '.prj'))
#    os.system('rm {}'.format(inShpPath.replace('.shp', '.*'))) # remove point shp

    print "\nCreated {}\n".format(buffShpPath)

    return buffShpPath

def main(inStack, bufferDist, shpDir, logFile = None):

    if logFile: # if log file not None
        print "See {} for log".format(logFile)
        so = se = open(logFile, 'a', 0) # open our log file
        sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0) # re-open stdout without buffering
        os.dup2(so.fileno(), sys.stdout.fileno()) # redirect stdout and stderr to the log file opened above
        os.dup2(se.fileno(), sys.stderr.fileno())

    print "Begin creating buffered shp: {}\n".format(datetime.datetime.now().strftime("%m%d%Y-%H%M"))

    pointShp = create_pointShp_fromRasterExtent(inStack, shpDir)
    outBuffShp = create_bufferShp_fromPointShp(pointShp, bufferDist)

    print "Finished creating buffered shp: {}\n----------------------------------------------------------------------------\n".format(datetime.datetime.now().strftime("%m%d%Y-%H%M"))

    return outBuffShp


if __name__ == '__main__':

    # get the arguments from input. only raster stack is required. others use defaults from functions py/ combo. Arguments: rasterStack, bufferSize, shpDir, logFile (optional)
    # At this point, regardless of what use inputs in run_GLAS_zonal_database.py, there  will be 4 args from sys.argv
    # **will need some editing to make this stand-alone (i.e. if len(sys.argv) < 4: if len(srs.arg) == 2: main(sys.argv[1], sys.argv[2], defaultLog, defaultShpDir) etc.**
    # if making stand alone, log file must be given as argument, not dir. Can also be None to not log maybe idk
    if len(sys.argv) == 5:
        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    if len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2], sys.argv[3])
