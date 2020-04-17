# -*- coding: utf-8 -*-
import os, sys
import numpy as np
import argparse
import pandas as pd
import time
import platform

from osgeo import ogr#gdal, osr#, ogr
#from osgeo.osr import SpatialReference
#from osgeo.osr import CoordinateTransformation

from rasterstats import zonal_stats

from RasterStack import RasterStack
from ZonalFeatureClass import ZonalFeatureClass
#from SpatialHelper import SpatialHelper

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
    # Edit shp to add the stat columns from df

    print "\nWriting output from Zonal Stats to {}".format(shp)
    
    # Open shp in write mode (shpObj.layer gives us read-mode layer)        
    shpObj = ZonalFeatureClass(shp)
    dataset = shpObj.driver.Open(shpObj.filePath, 1)
    layer = dataset.GetLayer()    
    
    # addCols is list of columns that we want to add to shp
    addCols = [f for f in df.columns if f not in shpObj.fieldNames()]   
    
    # Add fields to shapefile - all added stat fields except stackName should be float64
    for col in addCols:
        
        colType = ogr.OFTReal
        if col == 'stackName':
            colType = ogr.OFTString
        
        layer.CreateField(ogr.FieldDefn(str(col), colType))

    # Iterate over features and add values for the new columns
    i = 0
    for feature in layer:
            
        if str(feature.GetField('lat')) != str(df['lat'][i]):
            import pdb; pdb.set_trace()
            
        for col in addCols: 

            feature.SetField(str(col), df[col][i])

        layer.SetFeature(feature)             
        i += 1
        
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
    return {key: layerDict[key] for key in range(10,11)}

def callZonalStats(raster, vector, layerDict, addPathRows = False):

    print "\nRunning zonal stats for {} layers".format(len(layerDict))
    print " Input Raster: {}".format(raster)
    print " Input Vector: {}".format(vector)
    
    # Iterate through layers, run zonal stats and build dataframe
    firstLayer = True
    
    for layerN in layerDict:
        
        layerName = layerDict[layerN][0]
        statsList = layerDict[layerN][1]

        print "\n Layer {} ({}): {}".format(layerN, layerName, statsList)

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
    
def logOutput(logFile):
    
    print "See {} for log".format(logFile)
    so = se = open(logFile, 'a', 0) # open our log file
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0) # re-open stdout without buffering
    os.dup2(so.fileno(), sys.stdout.fileno()) # redirect stdout and stderr to the log file opened above
    os.dup2(se.fileno(), sys.stderr.fileno())
    
    return None
        
def updateOutputCsv(outCsv, df):
    # Append a dataframe to an output CSV - assumes columns are the same

    print "\nUpdating the big output csv {}".format(outCsv)
    
    hdr = False # Only add the header if the file does not exist
    if not os.path.isfile(outCsv):
        hdr = True
    
    df.to_csv(outCsv, sep=',', mode='a', index=False, header=hdr)
    
    return None
    
def updateOutputGdb(outGdb, inShp, outEPSG = 4326):
    # Append a shp to output GDB - assumes fields are the same

    print "\nUpdating the big output GDB {}".format(outGdb)
    
    layerName = os.path.basename(outGdb).replace('.gdb', '')
    cmd = 'ogr2ogr -nln {} -a_srs EPSG:4326 -t_srs EPSG:4326'.format(layerName)
    
    if os.path.exists(outGdb):
        cmd += ' -update -append'
        
    cmd += ' -f "FileGDB" {} {}'.format(outGdb, inShp)

    print '', cmd
    os.system(cmd)

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

    # Start clock
    start = time.time()
    
    # Unpack arguments   
    inRaster  = args['rasterStack']
    inZonalFc = args['zonalFc']
    outCsv    = args['outputCsv']
    logOut    = args['logOutput']
    
    stack   = RasterStack(inRaster)
    inZones = ZonalFeatureClass(inZonalFc) # This will be clipped

    # Set some variables from inputs
    stackExtent = stack.extent()
    stackEpsg   = stack.epsg()
    stackName   = stack.stackName

    # Get the output directory
    # outDir = baseDir / zonalType (ATL08_na or GLAS_buff30m) --> stackType / stackName
    zonalType = inZones.zonalName
    outDir    = stack.outDir(os.path.join(baseDir, zonalType))

    # Stack-specific outputs
    stackCsv = os.path.join(outDir, '{}__{}__zonalStats.csv'.format(zonalType, stackName))
    stackShp = stackCsv.replace('.csv', '.shp')
    
    # "Big" outputs (unique for zonal/stack type combos)
    outGdb = outCsv.replace('.csv', '.gdb')
    
    # 1. Start stack-specific log if doing so
    if logOut: 
        logFile = stackCsv.replace('.csv', '__Log.txt')
        logOutput(logFile)
    
    # print some info
    print "BEGIN: {}\n".format(time.strftime("%b-%d-%y %h:%M:%S"))
    print "Input raster stack: {}".format(inRaster)
    print " n layers = {}".format(stack.nLayers)
    print "Input zonal feature class: {}".format(inZonalFc)
               
    # 2. Clip input zonal shp to raster extent. Output proj = that of stack  
    clipZonal = os.path.join(outDir, '{}__{}.shp'.format(zonalType, stackName))
    if not os.path.isfile(clipZonal):
        print "\nClipping input feature class to extent..."
        inZones.clipToExtent(stackExtent, stackEpsg, stackEpsg, clipZonal) 
    
    if not os.path.isfile(clipZonal):
        raise RuntimeError('Could not perform clip of input zonal feature class')
    
    # now zones is the clipped input ZFC object:
    zones = ZonalFeatureClass(clipZonal)
    if zones.nFeatures == 0:
        print " There were 0 features after clipping to stack extent"
        return None
    print "\nZonal feature class after clip: {}".format(clipZonal)
    print " n features after clip = {}".format(zones.nFeatures) 
    
    # 3. Filter footprints based on attributes
    #* if i have to iterate through points to filter below, then combine with applyNoDataMask:
    #* in ZFC: def filterData(self, noData, filterAttributesKey/Dict or hardcoded in ZFC)
    
    # filterData(shp, zonalName) # filter dataframe based on GLAS or ATL08 using OGR - faster?
        # and return filtered shp OR (later...)
     
    # 4. Remove footprints under noData mask 
    noDataMask = stack.noDataLayer()
    rasterMask = RasterStack(noDataMask)

    # If noDataMask is NOT in same projection as zonal fc, supply correct EPSG
    transEpsg = None
    if int(rasterMask.epsg()) != int(zones.epsg()):
        transEpsg = rasterMask.epsg() # Need to transform coords to that of mask

    print "\nMasking out NoData values using {}".format(noDataMask)        
    stackShp = zones.applyNoDataMask(noDataMask, transEpsg = transEpsg,
                                                             outShp = stackShp)
               
    zones = ZonalFeatureClass(stackShp) # Now zones is the filtered fc obj, will eventually have the stats added as attributes
    print "\nZonal feature class after masking ND values: {}".format(stackShp)
    if zones.nFeatures == 0:
        print " There were 0 features after masking ND values"
        return None
    print " n features after masking = {}".format(zones.nFeatures)
    
    # 5. Get stack key dictionary    
    layerDict = buildLayerDict(stack) # {layerNumber: [layerName, [statistics]]}
    import pdb; pdb.set_trace()
    # 6. Call zonal stats and return a pandas dataframe    
    zonalStatsDf = callZonalStats(stack.filePath, zones.filePath, layerDict)
    
    # 7. If there is an xml layer for stack, get sun angle and add as column to df
    stackXml = stack.xmlLayer()
    if stackXml:
        zonalStatsDf = addSunAngleColumn(zonalStatsDf, stackXml)
    
    # Replace "None" values with our NoData value from stack:
    zonalStatsDf = zonalStatsDf.fillna(stack.noDataValue)

    # 8. Now write the stack csv, and add stats from the df to stack shp     
    zonalStatsDf.to_csv(stackCsv, sep=',', index=False, header=True)#), na_rep="NoData")

    # 9. Finish the output stack-specific shp by adding new stats columns to fc:
    #    But first, add stackName column
    zonalStatsDf['stackName'] = [stackName for i in range(len(zonalStatsDf))]
    stackShp = addStatsToShp(zonalStatsDf, stackShp)
       
    # 10. Update the big csv and big output gdb by appending to them:
    updateOutputCsv(outCsv, zonalStatsDf)
    updateOutputGdb(outGdb, stackShp)

    elapsedTime = round((time.time()-start)/60, 4)
    print "\nEND: {}\n".format(time.strftime("%b-%d-%y %h:%M:%S"))
    print " Completed in {} minutes".format(elapsedTime)

    # Lastly, record some info to a batch-level csv:
    batchCsv = os.path.join(baseDir, '_timing', 
                        '{}_{}__timing.csv'.format(zonalType, stack.stackType))
    if not os.path.isfile(batchCsv):
        with open(batchCsv, 'w') as bc:
            bc.write('stackName,n layers,n zonal features,node,minutes\n')
    with open(batchCsv, 'a') as bc:
        bc.write('{},{},{},{},{}\n'.format(stackName, stack.nLayers, zones.nFeatures, platform.node(), elapsedTime))

    return None

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-rs", "--rasterStack", type=str, required=True, help="Input raster stack")
    parser.add_argument("-z", "--zonalFc", type=str, required=True, help="Input zonal shp/gdb")
    parser.add_argument("-o", "--outputCsv", type=str, required=True, help="Output csv for all stacks. GDB will also be created")
    parser.add_argument("-log", "--logOutput", action='store_true', help="Log the output")
    
    args = vars(parser.parse_args())

    main(args)
    