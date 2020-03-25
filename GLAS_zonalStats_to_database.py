import sys, os
from rasterstats import zonal_stats
import datetime
from osgeo import gdal, ogr
import GLAS_functions as functions
from GLAS_functions import Parameters as params
import shutil
import tempfile

"""
This script takes as input a zone polygon (buffered GLAS shots) and raster stack from which statistics will be retrived and writes them + attributes to an ouput csv
Could work standalone if you provide the following command line inputs:
    *dataStack, *buffShp, *outDir, zstats (defaults to stats in functions.py), logFile (defaults to None if not supplied explicity i.e. logFile = 'path/to/log.txt'), outputShapefile = True
    outputShapefile is set to True. can turn it off by explictly calling outputShapefile = False.
    * = Required parameter
"""

# Added 3/24 for temp ATL08 processing
def clipZonalToExtent(zonalFc, extent):
    
    # extent should be (xmin, ymin, xmax, ymax)
    
    # Unpack extent
    #() = extent # or will ' '.join(extent) work in format?
    clip = tempfile.mkdtemp()
    print os.path.join(clip, 'ATL08.shp')
    cmd = 'ogr2ogr -clipsrc {} -f "ESRI Shapefile" {} {}'.format(' '.join(map(str,extent)), clip, zonalFc)
    os.system(cmd)

    """
    cmd = 'ogr2ogr'                        + \
      ' -clipsrc'                      + \
#      ' ' + str(ulx)                   + \
#      ' ' + str(lry)                   + \
#      ' ' + str(lrx)                   + \
#      ' ' + str(uly)                   + \
      ' -f "ESRI Shapefile"'           + \
      ' "' + clipFile   + '"'          + \
      ' "' + zonalFc + '"'
    """
    
    return os.path.join(clip, 'ATL08.shp')

def get_pathrows(lat, lon):
    import get_wrs

    result = get_wrs.ConvertToWRS().get_wrs(lat, lon)
    pr_list = ['{}{}'.format(str(i["path"]).zfill(3), str(i["row"]).zfill(3)) for i in result]

    #return '"{}"'.format(','.join(pr_list))
    return ';'.join(pr_list)

def get_nmad(a, c=1.4826): # this gives the same results as the dshean's method but does not need all the other functions
    import numpy as np
    import warnings

    arr = np.ma.array(a).compressed() # should be faster to not use masked arrays
    with warnings.catch_warnings(): # ignore nan warnings
        warnings.simplefilter("ignore", category=RuntimeWarning)
        med = np.median(arr)
        nmad = np.median(np.abs(arr - med))*c
    if np.isnan(nmad): nmad = None

    return nmad

def database_to_shp(inCsv, outEPSG = 4326, latField = 'lat', lonField = 'lon'): # input csv needs to have lat and lon fields, and they need to be in decimal degrees

    import shapefile as shp

    # Create the framework for the shapefile -- 11/27/2018: create shp in WGS84 first, new way with pyshp
    outShpPath = inCsv.replace('.csv', '.shp')
    outShpPath_wgs = outShpPath.replace('.shp', '_WGS84.shp')

    outShp = shp.Writer(outShpPath_wgs, shapeType = shp.POINT)
    outShp.autoBalance = 1
    r=0
    with open(inCsv, 'r') as csvF:
        hdr_row = csvF.readline().strip() # get the header/fields
        fld_list = hdr_row.split(',')

        if (latField or lonField) not in fld_list:
            print "{} or {} field is missing from .csv. Cannot continue".format(latField, lonField)
            return None

        for f in fld_list:
            if f == 'stackDir': # need length of this field to be 150
                outShp.field(f, size = 150)
            else: # default of 50
                outShp.field(f) # set output fields in shp

        for row in csvF.readlines(): # now iterate through the rest of the points
            row_list = row.strip().split(',')

            try: # in case, for whatever reason, this field cannot be got, skip row
                lat = float(row_list[fld_list.index(latField)])
                lon = float(row_list[fld_list.index(lonField)])
            except ValueError:
                continue

            outShp.point(lon, lat) # create point geometry
            outShp.record(*tuple([row_list[f] for f, j in enumerate(fld_list)]))
            r+=1
    print "\nClosing output shp {}...".format(outShpPath_wgs)
    #outShp.save(outShpPath_wgs.strip('.shp')) # 11/27 - do not need this anymore, only close
    outShp.close()

    # still need to write prj file
    with open(outShpPath_wgs.replace('.shp', '.prj'), 'w') as prjFile: # write prj file
        prjFile.write(functions.getWKT_PRJ(4326)) # Write in WGS84 since coordinates are in decimal degrees

    if str(outEPSG) == '4326': # if our final output is WGS84
        return outShpPath_wgs # return WGS shp

    else: # if not, project to outEPGS using ogr2ogr
        reproj_cmd = 'ogr2ogr -q -t_srs EPSG:{} {} {}'.format(outEPSG, outShpPath, outShpPath_wgs)
        print "\nReprojecting to input UTM proj (EPSG:{})....".format(outEPSG)
        print reproj_cmd
        os.system(reproj_cmd)
        os.system('rm {}'.format(outShpPath_wgs.replace('.shp', '.*'))) # remove WGS output
        print 'rm {}'.format(outShpPath_wgs.replace('.shp', '.*'))
        return outShpPath

def add_to_db(outDbCsv, outDbShp, inCsv): # given an input csv we wanna add, add to the output Csv, then write contents to output Shp
    import csv

    # First write the database csv
    if not os.path.isfile(outDbCsv): # if csv does not already exist...
        shutil.copy(inCsv, outDbCsv) # make a copy of the single csv to the output db
    else: # if the csv does exist, add unique lines to it
        with open(outDbCsv, 'r') as odc: existingDb = list(csv.reader(odc)) # read exising db into a list
        with open(inCsv, 'r') as ic: addDb = list(csv.reader(ic)) # read csv to be added into list
        dbHdrLen = len(existingDb[0]) # length of first line from existing db (assuming hdr/flds)

        for line in addDb: # for each line, if line does not already exist in db, append it to csv
            if line in existingDb:
##                print '{} already in db'.format(line)
                continue
##            print 'adding: {}'.format(line)

            if dbHdrLen != len(line):
                print "\nCannot add current output ({}) to {} OR the output .shp because the number of columns is different.".format(inCsv, outDbCsv)
                return None

            with open(outDbCsv, 'a') as odc: odc.write('{}\n'.format(','.join(line)))

    # lastly, write the accumulated output csv db to shp
    if os.path.exists(outDbShp): os.rename(outDbShp, outDbShp.replace('.shp', '__old.shp')) # first rename the existing shp db if it exists
    database_to_shp(outDbCsv, outEPSG='4326') # then create shp, outDbShp will be same name/path as .csv but with .shp extension. give it WGS84 epsg when creating big database
    return "added"

def main(input_raster, input_polygon, bufferSize, outDir, zstats = params.default_zstats, logFile = None, mainDatabasePrefix = params.default_mainDatabase, addWrs2 = True, outputShapefile = True):

    print "Begin running zonal stats: {}\n".format(datetime.datetime.now().strftime("%m%d%Y-%H%M"))

    # Added 3/24 - clip ATL08 gdb to shp using extent of raster stack
    (xmin, ymin, xmax, ymax) = functions.get_gcs_extent(input_raster)
    input_polygon = clipZonalToExtent(input_polygon, (xmin, ymin, xmax, ymax))  
    print "Using {} as zonal fc\n".format(input_polygon)

    # set up the output csv/shp
    if not mainDatabasePrefix.endswith('.csv') and not mainDatabasePrefix.endswith('.shp'):
        outDatabaseCsv = '{}_{}m.csv'.format(mainDatabasePrefix, bufferSize)
        outDatabaseShp = '{}_{}m.shp'.format(mainDatabasePrefix, bufferSize)
    else:
        if mainDatabasePrefix.endswith('.csv'):
            outDatabaseCsv = mainDatabasePrefix
            outDatabaseShp = outDatabaseCsv.replace('.csv', '.shp')
        elif mainDatabasePrefix.endswith('.shp'):
            outDatabaseShp = mainDatabasePrefix
            outDatabaseCsv = outDatabaseShp.replace('.shp', '.csv')

    stackExt = os.path.splitext(input_raster)[1] # could be either tif or vrt
    stackName = os.path.basename(input_raster).strip('_stack{}'.format(stackExt)).strip(stackExt)
    stackDir = os.path.dirname(input_raster)
    stack_inputLog = input_raster.replace(stackExt, '_Log.txt')
    if not os.path.isfile(stack_inputLog):
        print "Log for stack {} does not exist. Running without\n".format(stack_inputLog)
        inKeyExists = False

    else: inKeyExists = True

    # Get the contents of the stack log in a list. This will change depending on new method going forward. numbers, key, etc?
    addSunAngle = False
    addEcoregion = False # add simplified ecoregion column if this layer exists --> relies on the log to have: ecoregionLayer = PCAkmn_type_warp.tif
    if inKeyExists:
        with open(stack_inputLog, 'r') as sil:
            stackList = sil.readlines()
        stackList = [s.strip('\n') for i, s in enumerate(stackList) if i>0] # skip header line # at this point, stack list should have number of layers +1 (end time still there, removed first two lines)
        if stackList[-1].split(',')[0] == '*':
            addSunAngle = True
            useXml = stackList[-1].split(',')[1]
            sunAngle = functions.getSunAngle(useXml)
        #TD
        # if ecoregionLayer is in stack list:
            #addEcoregion = True
            #store ecoregion layer number

    if logFile:
        print "See {} for log".format(logFile)
        so = se = open(logFile, 'a', 0) # open our log file
        sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0) # re-open stdout without buffering
        os.dup2(so.fileno(), sys.stdout.fileno()) # redirect stdout and stderr to the log file opened above
        os.dup2(se.fileno(), sys.stderr.fileno())

    outCsvFile = os.path.join(outDir, '{}__stats.csv'.format(stackName))
    if os.path.isfile(outCsvFile): os.remove(outCsvFile)

    n_stats = len(zstats.split(' '))

    # Get the number of layers from the input stack and number of features from shp:
    n_layers = gdal.Open(input_raster).RasterCount
    print "\nInput raster stack: {}".format(input_raster)
    print " Begin: {}".format(datetime.datetime.now().strftime("%m%d%Y-%H%M"))
    print " Statistics: {}".format(zstats)
    print " Number of layers = {}".format(n_layers)
    print " Output database for stacks = {}".format(outDatabaseCsv)
#    import pdb; pdb.set_trace()
    majority_bnames = params.majority_basenames # if layer's basename is in this list, use majority for zonal stats

    # loop through layers, run zonal stats and start to build the dictionary for the csv
    for l in range(0, n_layers):

        if inKeyExists:
            layerN = stackList[l].split(',')[0]
            layerName = os.path.basename(stackList[l].split(',')[1]).strip('.tif') # Get the layer name from text file
##            if layerName in majority_bnames:
##                print "using majority"
##                zstats = ["majority"]
        else:
            layerN = str(l+1)
            layerName = str(l+1)

        print "\nLayer {}: {}".format(l+1, layerName)

        if layerName in majority_bnames or layerName.endswith('standage_warp'):
            print "using majority", layerName
            zonalStatsDict = zonal_stats(input_polygon, input_raster, stats="majority", geojson_out=True, band=l+1)
        else: # in any other case, run like normal
            print "using default stats", layerName
            if "nmad" in zstats:
                zonalStatsDict = zonal_stats(input_polygon, input_raster, stats=zstats.replace("nmad", ""), add_stats={'nmad':get_nmad}, geojson_out=True, band=l+1)
            else:
                zonalStatsDict = zonal_stats(input_polygon, input_raster, stats=zstats, geojson_out=True, band=l+1)

        # before iterating through rows, just get the field information from the first feature
        fields = [str(s) for s in zonalStatsDict[0]['properties'].keys()]
        # split the fields and values based on attributes and statistics
        attr_fields = fields[0:(len(fields)-n_stats)]
        attr_fields.extend(['stackName', 'stackDir', 'bufferSize']) # Add fields: stackName, bufferSize, stackDir
        if addWrs2: attr_fields.append('wrs2')
        if addSunAngle: attr_fields.append('sunElAngle')
        stat_fields = fields[-n_stats:] # get just the stat field names
        stat_fields = ['{}__{}'.format(layerN, s) for s in stat_fields] # rename with layer number appended to stat

        # dict method. the header is the first entry in the output dictionary. uniqueID is the key, others are the values
        key_name = attr_fields[0]
        if l == 0: # if we are on the first layer, create the Dict and add attribute field names
            outDict = {}
            outDict[key_name] = attr_fields[1:]
        outDict[key_name].extend(stat_fields) # always append stat_fields regardless of layer number

        # Iterate through the row results to add to outCsvRow
        for i in range(0, len(zonalStatsDict)):

            fields = [str(s) for s in zonalStatsDict[i]['properties'].keys()]
            vals = [str(s) for s in zonalStatsDict[i]['properties'].values()]
            attr_vals = vals[0:(len(fields)-n_stats)]
            stat_vals = vals[-n_stats:] # get just the statistics

            if l == 0: # if we are on layer 1, get & add other attributes for row:
                attr_vals.extend([stackName, stackDir, bufferSize]) # add attributes field names: stackName, bufferSize
                if addWrs2:
                    lat,lon = [float(vals[fields.index('lat')]), float(vals[fields.index('lon')])]
                    pathrows = get_pathrows(lat,lon)
                    attr_vals.append(pathrows)
                if addSunAngle:
                    attr_vals.append(sunAngle)
                outDict[attr_vals[0]] = attr_vals[1:]

            outDict[attr_vals[0]].extend(stat_vals) # always add stat vals


        functions.write_dict_toDatabase(outCsvFile, outDict, key_name)



        print '-----------'

    print "\nFinished: {}".format(datetime.datetime.now().strftime("%m%d%Y-%H%M"))

    raster_epsg = functions.get_proj_info(input_raster)[2]
    if outputShapefile:
        outShpPath = database_to_shp(outCsvFile, raster_epsg)
    else: outShpPath = None

    # lastly, append to running db
    db_result = add_to_db(outDatabaseCsv, outDatabaseShp, outCsvFile)
    if db_result =='added': print "\nAdded outputs to {} and {}\n".format(outDatabaseCsv, outDatabaseShp)

    print "\nFinished running zonal stats: {}\n----------------------------------------------------------------------------\n".format(datetime.datetime.now().strftime("%m%d%Y-%H%M"))
    return outCsvFile, outShpPath

if __name__ == "__main__":

    # Arguments passed to this process: dataStack, buffShp, bufferSize, outDir, zstats (optional, defaults to median, mean, std, nmad), logFile (optional), addWrs2 (default is True), outputShp, database prefix. refer to buffer script for changes needed to make this standalone
##    poly = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal/zonal_data/WV02_20160904_103001005CB63300_103001005CB00300_Bonanza_Creek_300kHz_Jul2014_l9s622_buffer-15m.shp'
##    rast = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal/zonal_data/WV02_20160904_103001005CB63300_103001005CB00300_Bonanza_Creek_300kHz_Jul2014_l9s622_stack.tif'
    if len(sys.argv) == 10:
        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8], sys.argv[9])
    if len(sys.argv) == 9:
        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8])
    if len(sys.argv) == 8:
        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7])
    if len(sys.argv) == 7:
        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
    if len(sys.argv) == 6:
        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    if len(sys.argv) == 5:
        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    if len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2], sys.argv[3])
