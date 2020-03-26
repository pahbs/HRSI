# -*- coding: utf-8 -*-
import os, sys
import numpy as np
from osgeo import gdal, osr#, ogr
#from osgeo.osr import SpatialReference
#from osgeo.osr import CoordinateTransformation
import tempfile
import argparse
import pandas as pd

from RasterStack import RasterStack

"""
3/20/2020:
    
Redo of zonal Stats code, which was a mess

Process:
    main --> 
Inputs:
    Raster Stack
    
"""
# Set global variables:

# Convert a lat, lon point to a different projection (i.e. UTM --> WGS84 geog.)
def convertCoordinates(inLon, inLat, sourceEpsg, targetEpsg):

    sourceSrs = osr.SpatialReference()
    sourceSrs.ImportFromEPSG(int(sourceEpsg)) # or 4326 for WGS84 lat/lon  
    
    targetSrs = osr.SpatialReference()
    targetSrs.ImportFromEPSG(int(targetEpsg))
    
    coordTrans = osr.CoordinateTransformation(sourceSrs, targetSrs)
    lon, lat = coordTrans.TransformPoint(inLon, inLat)[0:2]
    
    return lon, lat
    
def clipZonalToExtent(zonalFc, extent):
    
    # Expect extent to be tuple = (xmin, ymin, xmax, ymax)
    
    clip = '{}.shp'.format(tempfile.mkdtemp())
    
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
    
    return clip
"""
def callZonalStats(raster, vector, layerDict, addPathRows = False):
    
    # Iterate through layers, run zonal stats
    # run/call dict to pandas
    
    # get column names from vector layer to use below
    
    # On initial layer:
        # set df = pandas result, try to ONLY do for attributes so we can add stats after
        # do other stuff
    
        # if addPathRows: add pathrows for each point since this varies for lat/lon
        # go ahead and call addOtherAttributes() here
        
    # for all layers:
        # or go ahead and do zonal stats stuff here?
        # add stat columns to pandas, rename
        # majority --> 1_majority, i.e.
    
    return zonalStatsDf
    
    
    
    return zonalDf

def getStatsList(layerName):

    # define majority dictionary
    # define zonal stats list
    # if statements
    # get nmad
    
    return statsList
"""
  
def getNmad(a, c=1.4826): # this gives the same results as the dshean's method but does not need all the other functions

    import warnings

    arr = np.ma.array(a).compressed() # should be faster to not use masked arrays
    with warnings.catch_warnings(): # ignore nan warnings
        warnings.simplefilter("ignore", category=RuntimeWarning)
        med = np.median(arr)
        nmad = np.median(np.abs(arr - med))*c
    if np.isnan(nmad): nmad = None

    return nmad

def getPathRows(lat, lon):
    
    import get_wrs

    result = get_wrs.ConvertToWRS().get_wrs(lat, lon)
    pr_list = ['{}{}'.format(str(i["path"]).zfill(3), str(i["row"]).zfill(3)) for i in result]

    #return '"{}"'.format(','.join(pr_list))
    return ';'.join(pr_list)

def main(args):
  
    # Unpack arguments
    # input raster stack, input zonal shapefile, output directory, log directory, 
    import pdb; pdb.set_trace()
    inRaster = args.rasterStack
    
    # Testing:
    inShp = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/ATL08/flight_shps/ATL08_boreal.shp'
    inStack = '/att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/projects/3dsi/stacks/Out_SGM/WV01_20160708_10200100516C7900_102001005082D500/WV01_20160708_10200100516C7900_102001005082D500_stack.vrt'    
    
    # Start log if True
    
    # Set some variables, sanitize inputs
    # Input shapefile
    # Input raster stack
    # Output directories
     
    # Get stack key dictionary
    stackKey = inStack.replace('.vrt', '_Log.txt')
    # layerDict = buildLayerDict(stack) # if no stack log, return default dict w/ default stats
    # combine with setZonalStatistics to get big dict of:
    # no log means 0: [0, [min, max, etc]]
    # return {layerNumber: [layerName, [statistics]]}
    # maybe add something to indicate an xml file for sun angle and no datalayer
  
    # Get raster extent from input (in lat/lon?)
    # do i even need extent outside of clipping the input shp?
    # extent = getRasterExtent(inputRaster/Stack)
    # epsg = getRasterEpsg(inputRaster/Stack)
    
    # Clip large zonal shp to raster extent (with buffer)
    # clipZonalShpToExtent()
        # return updated shp
 
    # filterData(shp, zonalName) # filter dataframe based on GLAS or ATL08 using OGR - faster?
        # and return filtered shp OR (later...)
        
    # Apply NoData mask to Shp, remove points that are outside of it
    # applyNoDataMask(zonalShp, noDataMask)
        # return final updated shp
    
    # callZonalStats(raster, vector, layerDict, addPathRows) # will set up 3DSI specific stuff and call zonal stats for each layer
        # will return a pandas dataframe 
        
    # Add additional attributes that are same for entire raster: addSunAngle, etc. see row 150
    #* ACTUALLY this will happen in callZonalStats
    # addExtraAttributes()
        # will return updated pandas dataframe
      
    #* prefer to do this earlier
    # filterData(df, zonalName) # OR filter dataframe based on GLAS or ATL08 using pandas - easier?
    
    #* once df is ready to write to csv, can we reorganize/clean up column order?
    # do we need to if we do stuff before?
    
    # dataFrameToCsv() # write pandas df to csv
        # call csvToShp for small csv
        
    # updateMainDb() # append to big csv and convert to shp
        # call csvToShp for big csv
    

    return None

def parse_arguments():
    
    # maybe keep this, maybe put it in below if
    #argument_parse_code
    parser = argparse.ArgumentParser()
    parser.add_argument("-stack", "--rasterStack", type=str, help="Input stack")
    
    return arguments

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-stack", "--rasterStack", type=str, help="Input stack")
    args = parser.parse_args()
    
    main(*args)
    