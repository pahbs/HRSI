# -*- coding: utf-8 -*-
import os, sys
import numpy as np
from osgeo import osr, ogr#gdal, osr#, ogr
#from osgeo.osr import SpatialReference
#from osgeo.osr import CoordinateTransformation
#import tempfile
import argparse
import pandas as pd

from rasterstats import zonal_stats

from RasterStack import RasterStack
from ZonalFeatureClass import ZonalFeatureClass
from SpatialHelper import SpatialHelper

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

def addStatsToShp(df, shp):   
    # Add the stat columns from df to shp
    
    # Dict to go from pandas type to GDAL type
    typeMap = {'float64': ogr.OFTReal, 'int64': ogr.OFTInteger, 
                                               'object': ogr.OFTString}

    # Open shp in write mode (shpObj.layer gives us read-mode layer)        
    shpObj = ZonalFeatureClass(shp)
    dataset = shpObj.driver.Open(shpObj.filePath, 1)
    layer = dataset.GetLayer()
    
    # addCols is list of columns that we want to add to shp
    addCols = [f for f in df.columns if f not in shpObj.fieldNames()]   
    
    # Loop over addCols and: create fields, then add value for all features
    for col in addCols:
        
        colName = str(col)
        colType = typeMap[str(df[col].dtype)]

        layer.CreateField(ogr.FieldDefn(colName, colType))

        # Loop over features in layer and add corresponding value from df
        i = 0
        print colName
        for feature in layer:
            print ' {}'.format(feature.GetFID())
            
            if str(feature.GetField('lat')) != str(df['lat'][i]):
                import pdb; pdb.set_trace()
            
            feature.SetField(colName, df[col][i])
            
            i+=1
        
    dataset = layer = feature = None
    
    return shp
    
    


def addSunAngleColumn(df, stackXml):
    
    sunAngle = getSunAngle(stackXml)
    
    if not sunAngle: # if sunAngle could not be retrieved
        return None # do nothing
    
    # If it was retrieved, add as column to the df
    df['sunAngle'] = [float(sunAngle) for x in range(0, len(df))]
    
    return df
    
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
        layerName = layerName.replace('{}_'.format(stackObject.stackName), '')  # Remove any stack-related name (ie pairname_) from layerName
        
        # Determine which stats to use
        if layerName in majorityNames: #or layerName.endswith('standage_warp'):
            zonalStats = ["majority"]
        else:
            zonalStats = defaultZonalStats
            
        layerDict[layerN] = [layerName, zonalStats]

    #return layerDict
    # subset for testing
    return {key: layerDict[key] for key in range(5,8)}

def callZonalStats(raster, vector, layerDict, addPathRows = False):
    
    # Iterate through layers, run zonal stats and build dataframe
    firstLayer = True
    
    for layerN in layerDict:
        
        #layerName = layerDict[layerN][0]
        statsList = layerDict[layerN][1]

        # Run/call dict to pandas    
        if "nmad" in statsList:
            statsList.remove("nmad")
            zonalStatsDict = zonal_stats(vector, raster, 
                        stats=' '.join(statsList), add_stats={'nmad':getNmad}, 
                        geojson_out=True, band=layerN)
            statsList.append("nmad")
        else:
            zonalStatsDict = zonal_stats(vector, raster, 
                        stats=' '.join(statsList), geojson_out=True, band=layerN)

        if firstLayer: # build the dataframe with just the attributes

            firstLayer = False
                  
            columns = [str(s) for s in zonalStatsDict[0]['properties'].keys()]
            attrCols = [i for i in columns if i not in statsList]
                        
            zonalStatsDf = pd.DataFrame(columns = attrCols)
            
            # Add feature attributes to df
            for col in attrCols:
                
                zonalStatsDf[col] = [zonalStatsDict[i]['properties'][col] \
                                        for i in range(0, len(zonalStatsDict))]
            
        # Now for every layer, add the statistic outputs to df:
        for col in statsList:
            
            outCol = '{}_{}'.format(layerN, col)
            
            zonalStatsDf[outCol] = [zonalStatsDict[i]['properties'][col] \
                                        for i in range(0, len(zonalStatsDict))]
    
    return zonalStatsDf

"""
def dfToCsv(df, outShp, sEpsg, tEpsg):
    
    # df should have lat and lon columns
    # sEpsg is the source EPSG, i.e. the projection of the points in the df
    # tEpsg is the target EPSG, i.e. the projection the outShp should be in
    
    sr = osr.SpatialReference()
    sr.ImportFromEPSG(int(tEpsg)) # Set SR object to target EPSG
    drv = ogr.GetDriverByName('ESRI Shapefile')
    ds = drv.CreateDataSource(outShp) #so there we will store our data
    layer = ds.CreateLayer('layer', sr, ogr.wkbPoint)
    layer_defn = layer.GetLayerDefn() 
"""    
     
def getNmad(a, c=1.4826):

    import warnings

    arr = np.ma.array(a).compressed() # should be faster to not use masked arrays
    with warnings.catch_warnings(): # ignore nan warnings
        warnings.simplefilter("ignore", category=RuntimeWarning)
        med = np.median(arr)
        nmad = np.median(np.abs(arr - med))*c
    if np.isnan(nmad): nmad = None

    return nmad

def getSunAngle(useXml):

    import xml.etree.ElementTree as ET
    
    tree = ET.parse(useXml)
    IMD = tree.getroot().find('IMD')

    try:
        return str(float(IMD.find('IMAGE').find('MEANSUNEL').text))
    except:
        return None 
    
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
    inRaster  = args['rasterStack']
    inZonalFc = args['zonalFc']
  
    stack   = RasterStack(inRaster)
    inZones = ZonalFeatureClass(inZonalFc) # This will be clipped

    stackExtent = stack.extent()
    stackEpsg   = stack.epsg()
    stackName   = stack.stackName
    
    # Set the output directory
    # outDir = baseDir / zonalType (ATL08_na or GLAS_buff30m) --> stackType / stackName
    zonalType = inZones.zonalName
    outDir    = stack.outDir(os.path.join(baseDir, zonalType))

    # Stack-specific outputs
    stackCsv = os.path.join(outDir, '{}__zonalStats.csv'.format(stackName))
    stackShp = stackCsv.replace('.csv', '.shp')
    
    # 1. Start log if doing so
    #import pdb; pdb.set_trace()
    
    # 2. Clip large input zonal shp to raster extent  
    # outDir/zonalType__stackName.shp
    clipZonal = os.path.join(outDir, '{}__{}.shp'.format(zonalType, stackName))
    if not os.path.isfile(clipZonal):
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

    # stackShp is filtered shp and will eventually have the stats added    
    stackShp = zones.applyNoDataMask(noDataMask, stackShp)
    zones = ZonalFeatureClass(stackShp) # Now zones is the filtered fc obj
    
    # 4-5. Get stack key dictionary    
    layerDict = buildLayerDict(stack) # {layerNumber: [layerName, [statistics]]}

    # 5-6. Call zonal stats and return a pandas dataframe    
    zonalStatsDf = callZonalStats(stack.filePath, zones.filePath, layerDict)
    
    # If there is an xml layer for stack, get sun angle and add as column to df
    stackXml = stack.xmlLayer()
    if stackXml:
        zonalStatsDf = addSunAngleColumn(zonalStatsDf, stackXml)
    
    # If addPathRows is True, get pathrows for each point & add as column to df

    # 7. Now write the stack csv, and add stats from the df to stack shp     
    zonalStatsDf.to_csv(stackCsv, sep=',', index=False, header=True, na_rep="NoData")
    import pdb; pdb.set_trace()
    print ogr.OFTReal
    # Create the output stack-specific shp by appending new stats columns to fc:    
    stackShp = addStatsToShp(zonalStatsDf, stackShp)
    
    # Update the big csv and big output gdb by appending to them:
    
    # FRIDAY:
    # - dfToCsv()
    # - updateMainCsv (which will call dfToCsv)
    
    #* Other inputs: big CSV/SHP or GDB; log file
    
    
    #* once df is ready to write to csv, can we reorganize/clean up column order?
    # do we need to if we do stuff before?
    
    # dataFrameToCsv() # write pandas df to csv
        # call csvToShp for small csv
        
    # updateMainDb() # append to big csv and convert to shp
        # call csvToShp for big csv
    

    return None

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--rasterStack", type=str, help="Input raster stack")
    parser.add_argument("-z", "--zonalFc", type=str, help="Input zonal shp/gdb")
    
    #* Other args ?
    #parser.add_argument("-o", "--bigCsv", type=str, help="Output csv for all stacks. GDB will also be created")

    args = vars(parser.parse_args())

    main(args)
    