"""
Given a raster stack input (with coinciding log file that details layer names)*:
    Create polygon shp from GLAS csv's, based on extent and buffered by given distance (default = 15m)
    Feed polygon and raster stack into zonal stats tool which will write to and return an output csv and output shp
    * If the stack has no coinciding log file, numbers will be used to label the columns i.e. 1__mean for layer 1 mean
"""

#from GLAS_functions import Dictionaries as dicts # dont need (for now)
from GLAS_functions import Parameters as params
import GLAS_functions
import GLAS_rasterExtent_to_bufferShp
import GLAS_zonalStats_to_database
import os, sys

def main(dataStack, bufferSize, shpDir, outDir, zstats, logDir, mainDatabasePrefix):

    if not os.path.isfile(dataStack):
        print "Input raster {} does not exist. Quitting program".format(dataStack)
        return None, None

    GLAS_csv_dir = params.GLAS_csv_dir # where input GLAS csv's are located (Northern latitudes)

    # create directories:
    for d in [shpDir, outDir, logDir]:
        if not os.path.exists(d): os.makedirs(d)

    # get log file name from rasterStack and begin logging:
    logFile = os.path.join(logDir, '{}_zonalStats_log.txt'.format(os.path.basename(dataStack).strip('_stack.tif').strip('.tif')))
    if os.path.isfile(logFile): os.remove(logFile)

    # make GLAS shp:
    buffShp = GLAS_rasterExtent_to_bufferShp.main(dataStack, bufferSize, shpDir, logFile)

    # get zonal stats and write to csv:
    outCsvFile, outShp = GLAS_zonalStats_to_database.main(dataStack, buffShp, bufferSize, outDir, zstats, logFile, mainDatabasePrefix)

    print "\n\nOutput csv: {}".format(outCsvFile)
    if outShp: print "Output shapefile: {}".format(outShp)

    return outCsvFile, outShp

if __name__ == '__main__':

    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("dataStack", help = "Path to input data stack") # required
    ap.add_argument("-bufferSize", default = params.default_buffSize, type = int, help = "Size of buffer around GLAS points (meters) Default = {}".format(params.default_buffSize))
    ap.add_argument("-shpDir", default = params.default_shpDir, help = "Directory for output shapefiles. Default = {}".format(params.default_shpDir))
    ap.add_argument("-outDir", default = params.default_outCsvDir, help = "Output csv directory. Default = {}".format(params.default_outCsvDir)) # where the output csv's will go. default dir is stored in functions.py and can be changed if need be
    ap.add_argument("-zstats", default = params.default_zstats, help = "Zonal stats. Default = {}".format(params.default_zstats))
    ap.add_argument("-logDir", default = params.default_logdir, help = "Directory for logging output. Default = {}".format(params.default_logdir))
    ap.add_argument("-mainDatabasePrefix", default = params.default_mainDatabase, help = "Path and file prefix for running database csv/shp. If extension is provided, exact file will be use. Otherwise, buffer will be appended. Default = {}".format(params.default_mainDatabase))
    #ap.add_argument("-slopeThresh", default = 10, type = int, help = "Slope threshold")
    # addWrs2 ? default True
    # outputShapefile ? default True
    kwargs = vars(ap.parse_args()) # parse args and convert to dict

    main(**kwargs) # run main with arguments


