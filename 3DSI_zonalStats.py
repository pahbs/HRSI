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
from ZonalFeatureClass import ZonalFeatureClass


"""
3/20/2020:
    
Redo of zonal Stats code, which was a mess

Process:
    main --> 
Inputs:
    RasterStack
    ZonalFeatureClass (will be clipped to extent)
    
"""

# Set global variables:
baseDir = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/ZonalStats/'


"""
This is not very stable because these stack_log.txt files might not be 
exactly in line with the .vrt/stack. This function is meant to keep all of the
variables/messiness together in one place
    # return {layerNumber: [layerName, [statistics]]}
"""
def buildLayerDict(stackObject): 

    stackKey = stackObject.stackKey() # could be None if No log
    layerDict = {}
    
    # Some things are hardcoded for Stacks
    defaultZonalStats = ['median', 'mean', 'std', 'nmad', 'min', 'max']
    majorityNames = ['C2C_change_year_warp', 'C2C_change_type_warp', 
                     'AK_NWCanada_Fire1965_2013_ras_warp', 
                     'MCD12Q1_A2017001_LC_Type1_warp',
                     'boreal_clust_25_100_2019_10_8_warp', 
                     'PCA_NaN_1_093019_warp', 
                     'PCA_NaN_2_093019_warp', 'PCA_NaN_3_093019_warp',
                     'NA_standage_v2_warp']
    
    # If there is no Log, build layerDict like --> {0: ['0', [defaultStats]]}
    if not stackKey:
        
        nLayers = stackObject.nLayers()
        
        for i in range(nLayers):
            layerDict[int(i+1)] = [str(i+1), defaultZonalStats]
    
        return layerDict
    
    # If there is a Log, read stackKey into list
    with open(stackKey, 'r') as sil:
        stackIn = sil.readlines()
    stackList = [s.strip('\n') for i, s in enumerate(stackIn) if i>0] 

    # layerDict --> key = layerN, value = [layerName, [statsList]]
    for l in stackList: 
        
        layerN = int(l.split(',')[0])
        layerName = os.path.basename(l.split(',')[1]).replace('.tif', '')
        layerName = layerName.replace('{}_'.format(stackObject.stackName), '')  # Remove any stack-related name (ie pairname) from layerName
        
        # Determine which stats to use
        if layerName in majorityNames: #or layerName.endswith('standage_warp'):
            zonalStats = ["majority"]
        else:
            zonalStats = defaultZonalStats
            
        layerDict[layerN] = [layerName, zonalStats]
    import pdb; pdb.set_trace() 
    #return layerDict
    # subset for testing
    return {key: layerDict[key] for key in range(1,4)}

def callZonalStats(raster, vector, layerDict, addPathRows = False):
    
    # Iterate through layers, run zonal stats and build dataframe
    for layerN in layerDict:
        
        layerName = layerDict[layerN][0]
        statsList = layerDict[layerN][1]

        # Run/call dict to pandas    
        if "nmad" in statsList:
            statsList.remove("nmad")
            zonalStatsDict = zonal_stats(vector, raster, 
                        stats=' '.join(statsList), add_stats={'nmad':getNmad}, 
                        geojson_out=True, band=layerN)
        else:
            zonalStatsDict = zonal_stats(vector, raster, 
                        stats=' '.join(statsList), geojson_out=True, band=layerN)
    

    
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
    
    
    
#    return zonalDf
  
def getNmad(a, c=1.4826): # this gives the same results as the dshean's method but does not need all the other functions

    import warnings

    arr = np.ma.array(a).compressed() # should be faster to not use masked arrays
    with warnings.catch_warnings(): # ignore nan warnings
        warnings.simplefilter("ignore", category=RuntimeWarning)
        med = np.median(arr)
        nmad = np.median(np.abs(arr - med))*c
    if np.isnan(nmad): nmad = None

    return nmad

"""
# Add Landsat pathrows to dataframe with lat/lon columns (decimal degrees)
def getPathRows(lat, lon):
    
    import get_wrs
    
    # Iterate through dataframe, extract lat/lon, get pathrows and add to new column

    result = get_wrs.ConvertToWRS().get_wrs(lat, lon)
    pr_list = ['{}{}'.format(str(i["path"]).zfill(3), str(i["row"]).zfill(3)) for i in result]

    #return '"{}"'.format(','.join(pr_list))
    return ';'.join(pr_list)
"""

def main(args):

    # Unpack arguments
    # input raster stack, input zonal shapefile, output directory, log directory,    
    inRaster = args['rasterStack']
    inZonalFc = args['zonalFc']
    
    stack = RasterStack(inRaster)
    inZones = ZonalFeatureClass(inZonalFc) # This will be clipped

    # Set the output directory
    # outDir = baseDir / zonalType (ATL08_na or GLAS_buff30m) --> stackType / stackName
    zonalType = inZones.zonalName
    outDir = stack.outDir(os.path.join(baseDir, zonalType))

    # 1. Start log if doing so
    #import pdb; pdb.set_trace()
    
    # 2. Clip large input zonal shp to raster extent (with buffer?)
    stackExtent = stack.extent()
    stackEpsg   = stack.epsg()
    stackName   = stack.stackName
    
    # outDir/zonalType__stackName.shp
    clipZonal = os.path.join(outDir, '{}__{}.shp'.format(zonalType, stackName))
    inZones.clipToExtent(stackExtent, stackEpsg, clipZonal) 
    
    if not os.path.isfile(clipZonal):
        raise RuntimeError('Could not perform clip of input zonal feature class')
    
    # now zones is the clipped input ZFC object:
    zones = ZonalFeatureClass(clipZonal)   
    
    # 3. Filter NoData points and points based on attributes
    #* if i have to iterate through points to filter below, then combine with applyNoDataMask:
    #* in ZFC: def filterData(self, noData, filterAttributesKey/Dict or hardcoded in ZFC)
    
    # filterData(shp, zonalName) # filter dataframe based on GLAS or ATL08 using OGR - faster?
        # and return filtered shp OR (later...)
     
    # 3-4. Remove footprints under noData mask 
    noDataMask = stack.noDataLayer()
    
    # Check first that noDataMask is in same projection as zonal fc:
    if int(RasterStack(noDataMask).epsg()) != int(zones.epsg()):
        # Eventually reproject mask to same epsg, but for now just raise error
        raise RuntimeError("In order to apply noDataMask, mask and zonal fc must be in same projection")
    
    outFilteredShp = zones.applyNoDataMask(noDataMask)
    zones = ZonalFeatureClass(outFilteredShp) # Now zones is the filtered fc obj
    
    # 4-5. Get stack key dictionary    
    layerDict = buildLayerDict(stack) # {layerNumber: [layerName, [statistics]]}
    #** maybe add something to indicate an xml file for sun angle and no datalayer

    callZonalStats(stack.filePath, zones.filePath, layerDict)
 
    
    # 5-6. Call zonal stats and return a pandas dataframe ready to go    
    
    # callZonalStats(raster, vector, layerDict, addPathRows) # will set up 3DSI specific stuff and call zonal stats for each layer
        # will return a pandas dataframe 
        
    # Add additional attributes that are same for entire raster: addSunAngle, etc. see row 150
    #* ACTUALLY this will happen in callZonalStats
    # addExtraAttributes()
        # will return updated pandas dataframe
      
    #* once df is ready to write to csv, can we reorganize/clean up column order?
    # do we need to if we do stuff before?
    
    # dataFrameToCsv() # write pandas df to csv
        # call csvToShp for small csv
        
    # updateMainDb() # append to big csv and convert to shp
        # call csvToShp for big csv
    

    return None

"""
def parseArguments():
    
    # maybe keep this, maybe put it in below if
    #argument_parse_code
    parser = argparse.ArgumentParser()
    parser.add_argument("-stack", "--rasterStack", type=str, help="Input stack")
    
    return arguments
"""

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--rasterStack", type=str, help="Input raster stack")
    parser.add_argument("-z", "--zonalFc", type=str, help="Input zonal shp/gdb")

    args = vars(parser.parse_args())

    main(args)
    