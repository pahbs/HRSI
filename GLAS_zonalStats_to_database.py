import sys, os
from rasterstats import zonal_stats
import datetime
from osgeo import gdal, ogr
import GLAS_functions as functions
from GLAS_functions import Parameters as params

"""
This script takes as input a zone polygon (buffered GLAS shots) and raster stack from which statistics will be retrived and writes them + attributes to an ouput csv
Could work standalone if you provide the following command line inputs:
    *dataStack, *buffShp, *outDir, zstats (defaults to stats in functions.py), logFile (defaults to None if not supplied explicity i.e. logFile = 'path/to/log.txt'), outputShapefile = True
    outputShapefile is set to True. can turn it off by explictly calling outputShapefile = False.
    * = Required parameter
"""

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

    # Create the framework for the shapefile
    outShpPath = inCsv.replace('.csv', '.shp')
    outShp = shp.Writer(shp.POINT)
    outShp.autoBalance = 1

    with open(inCsv, 'r') as csvF:
        hdr_row = csvF.readline().strip() # get the header/fields
        fld_list = hdr_row.split(',')
##        print fld_list

        if (latField or lonField) not in fld_list:
            print "{} or {} field is missing from .csv. Cannot continue".format(latField, lonField)
            return None

        for f in fld_list: outShp.field(f) # set output fields in shp

        for row in csvF.readlines(): # now iterate through the rest of the points
            row_list = row.strip().split(',')

            lat = float(row_list[fld_list.index(latField)])
            lon = float(row_list[fld_list.index(lonField)])

            outShp.point(lon, lat) # create point geometry
            outShp.record(*tuple([row_list[f] for f, j in enumerate(fld_list)]))

    print "\nSaving to output shp {}...".format(outShpPath)

    # create shp in WGS84 first
    outShpPath_wgs = outShpPath.replace('.shp', '_WGS84.shp')
    outShp.save(outShpPath_wgs.strip('.shp'))
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


def main(input_raster, input_polygon, outDir, zstats = params.default_zstats, logFile = None, outputShapefile = True):

    stackName = os.path.basename(input_raster).strip('_stack.tif').strip('.tif')
    stack_inputLog = input_raster.replace('.tif', '_Log.txt')
    if not os.path.isfile(stack_inputLog):
        print "Log for stack {} does not exist. Running without\n".format(stack_inputLog)
        inKeyExists = False
        #return None # cannot run without log
    else: inKeyExists = True

    # Get the contents of the stack log in a list. This will change depending on new method going forward. numbers, key, etc?
    if inKeyExists:
        with open(stack_inputLog, 'r') as sil:
            stackList = sil.readlines()
        stackList = [s.strip('\n') for i, s in enumerate(stackList) if i>1] # at this point, stack list should have number of layers +1 (end time still there, removed first two lines)

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


    # loop through layers, run zonal stats and start to build the dictionary for the csv
    for l in range(0, n_layers):

        if inKeyExists: layerName = os.path.basename(stackList[l]).strip('.tif') # Get the layer name from text file
        else: layerName = str(l+1)

        print "\nLayer {}: {}".format(l+1, layerName)

        if "nmad" in zstats: zonalStatsDict = zonal_stats(input_polygon, input_raster, stats=zstats.replace("nmad", ""), add_stats={'nmad':get_nmad}, geojson_out=True, band=l+1)
        else: zonalStatsDict = zonal_stats(input_polygon, input_raster, stats=zstats, geojson_out=True, band=l+1)

        # before iterating through rows, just get the field information from the first feature
        fields = [str(s) for s in zonalStatsDict[0]['properties'].keys()]
        # split the fields and values based on attributes and statistics
        attr_fields = fields[0:(len(fields)-n_stats)]
        attr_fields.append('stackName') # want to add a field that has the name of the stack in case we want to combine later
        stat_fields = fields[-n_stats:] # get just the stat field names
        stat_fields = ['{}__{}'.format(layerName, s) for s in stat_fields] # rename with layer name appended to stat


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
            attr_vals.append(stackName) # add name of stack to attribute values list
            stat_vals = vals[-n_stats:] # get just the statistics

            if l == 0: outDict[attr_vals[0]] = attr_vals[1:] # if we are on layer 1, start dict entry with attribute values
            outDict[attr_vals[0]].extend(stat_vals) # always add stat vals


        functions.write_dict_toDatabase(outCsvFile, outDict, key_name)
        print '-----------'

    print "\nFinished: {}".format(datetime.datetime.now().strftime("%m%d%Y-%H%M"))

    raster_epsg = functions.get_proj_info(input_raster)[2]
    if outputShapefile:
        outShpPath = database_to_shp(outCsvFile, raster_epsg)
    else: outShpPath = None

    return outCsvFile, outShpPath

if __name__ == "__main__":

    # Arguments passed to this process: dataStack, buffShp, outDir, zstats (optional, defaults to median, mean, std, nmad), logFile (optional). refer to buffer script for changes needed to make this standalone
##    poly = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal/zonal_data/WV02_20160904_103001005CB63300_103001005CB00300_Bonanza_Creek_300kHz_Jul2014_l9s622_buffer-15m.shp'
##    rast = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal/zonal_data/WV02_20160904_103001005CB63300_103001005CB00300_Bonanza_Creek_300kHz_Jul2014_l9s622_stack.tif'
    if len(sys.argv) == 7:
        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
    if len(sys.argv) == 6:
        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    if len(sys.argv) == 5:
        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    if len(sys.argv) == 4:
        main(sys.argv[1], sys.argv[2], sys.argv[3])