#!/usr/bin/env python
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
    
    
6/5: Changing csv argument to be either csv or gdb
     If gdb is passed, create .csv and .gdb
     If .csv is passed, create only .csv
    
"""


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
            import pdb; pdb.set_trace() # We have a problem that needs checking
            
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
                     'NA_standage_v2_warp', 'warp'] # careful with warp, that it doesnt get mixed up with other layers (meant to be for first Lsat layer)
    
    # If there is no Log, build layerDict like --> {0: ['0', [defaultStats]]}
    if not stackKey:
        
        nLayers = stackObject.nLayers
        
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
        if layerName in majorityNames or layerName.endswith('mask_warp') or \
        layerName.endswith('std_warp') or layerName.endswith('year_n0_m1h2_warp'):
            zonalStats = ["majority"]
        else:
            zonalStats = defaultZonalStats
            
        layerDict[layerN] = [layerName, zonalStats]

    return layerDict
    # subset for testing
    #return {key: layerDict[key] for key in range(9,11)}

def callZonalStats(rasterObj, vectorObj, layerDict, addPathRows = False):
    
    raster = rasterObj.filePath
    vector = vectorObj.filePath
    
    # Determine if stack type is tandemx/Landsat or not
    # If it is, use all_touched = True
    allTouched = False
    if rasterObj.stackType() == 'Tandemx' or rasterObj.stackType() == 'Landsat':
        allTouched = True

    print " Input Raster: {}".format(raster)
    print " Input Vector: {}".format(vector)
    print "  all_touched = {}".format(allTouched)
    
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
                        geojson_out=True, all_touched = allTouched, band=layerN)
            statsList.append("nmad")
        else:
            zonalStatsDict = zonal_stats(vector, raster, 
                        stats=' '.join(statsList), geojson_out=True, 
                        all_touched = allTouched, band=layerN)

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
            
            outCol = 'L{}_{}'.format(layerN, col) #gdb's don't like columns to start with numbers
            
            # Shorten "majority" in case it's in in column name
            outCol = outCol.replace('majority', 'mjrty')
            
            zonalStatsDf[outCol] = [zonalStatsDict[i]['properties'][col] \
                                        for i in range(0, len(zonalStatsDict))]
            
    # Lastly, clean up unnecessary columns:
    # This cleans up the .csv but not the shapefile
    import pdb; pdb.set_trace()
    #dropColumns = ['SHAPE_Leng', 'SHAPE_Area', 'SHAPE_Length', 'keep']
    #zonalStatsDf = zonalStatsDf.drop(dropColumns, axis=1, errors='ignore')
    
    return zonalStatsDf

def checkZfcResults(zfc, activity):

    print "\nZonal feature class after {}: {}".format(activity, zfc.filePath)

    if zfc.nFeatures == 0:
        print "\nThere were 0 features after {}. Exiting ({})".format(activity, time.strftime("%m-%d-%y %I:%M:%S"))
        return None

    print " n features now = {}".format(zfc.nFeatures)
    return 'continue'

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

def removeExtraColumns(fc, cols):
    
    for col in cols: 
        fc.removeField(col)
    
    fc = ZonalFeatureClass(fc.filePath) # reinitiate fc object
    return fc
   
def updateOutputCsv(outCsv, df):
    # Append a dataframe to an output CSV - assumes columns are the same

    print "\nUpdating the big output csv {}".format(outCsv)
    
    hdr = False # Only add the header if the file does not exist
    if not os.path.isfile(outCsv):
        hdr = True
    
    df.to_csv(outCsv, sep=',', mode='a', index=False, header=hdr)
    
    return None
"""    
def updateOutputGdb(output, inFile, outEPSG = 4326):
    # Append a shp to output GDB/GPKG - assumes fields are the same

    # Get driver based off output extension
    ext = os.path.splitext(output)[1]   
    if ext == '.gdb':
        outDrv = 'FileGDB'
    elif ext == '.gpkg':
        outDrv = 'GPKG'
    else:
        print "\nUnrecognized output extension '{}'".format(ext)
        return None
    
    print "\nUpdating the big output {}".format(output)
    
    layerName = os.path.basename(output).replace(ext, '')
    cmd = 'ogr2ogr -nln {} -a_srs EPSG:4326 -t_srs EPSG:4326'.format(layerName)
    
    if os.path.exists(output):
        cmd += ' -update -append'
        
    cmd += ' -f "{}" {} {}'.format(outDrv, output, inFile) 
    
    print '', cmd
    os.system(cmd)

    return None       
"""
             
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
    
    ogr.UseExceptions() # Unsure about this, but pretty sure we want errors to cause exceptions
    # "export CPL_LOG=/dev/null" -- to hide warnings, must be set from shell or in bashrc

    # Start clock
    start = time.time()
    
    # Set main directory:
    baseDir = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/ZonalStats/'
    
    # Unpack arguments   
    inRaster  = args['rasterStack']
    inZonalFc = args['zonalFc']
    bigOutput = args['bigOutput']
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
   
    # Figure out if we are writing to .gdb/.gpkg and .csv or just .csv
    bigExt = os.path.splitext(bigOutput)[1]
    if bigExt == '.gdb' or bigExt == '.gpkg': # Write to both
        # Assume gdb/gpkg is node specific (eg. output-crane101.gdb)
        outCsv = bigOutput.replace('-{}{}'.format(platform.node(), bigExt), '.csv')
        if not outCsv.endswith('.csv'): # If that assumption is wrong and the above didn't work
            outCsv = bigOutput.replace(bigExt, '.csv') # then replace extension as is
        outGdb = bigOutput # Keep gdb as is
    elif bigExt == '.csv': # Write only to .csv
        outCsv = bigOutput
        outGdb = None
    
    # Create directory where output is supposed to go:
    os.system('mkdir -p {}'.format(os.path.dirname(outCsv)))

    # Stack-specific outputs
    stackCsv = os.path.join(outDir, '{}__{}__zonalStats.csv'.format(zonalType, stackName))
    stackShp = stackCsv.replace('.csv', '.shp')
    
    # "Big" outputs (unique for zonal/stack type combos)
    """ Need to come up with better/automated solution for locking issue when
        writing to the output gdb. For now, just write to a node-specific 
        output GPKG and merge by hand when all are done
    """
   
    # Start stack-specific log if doing so
    if logOut: 
        logFile = stackCsv.replace('.csv', '__Log.txt')
        logOutput(logFile)
   
    # print some info
    print "BEGIN: {}\n".format(time.strftime("%m-%d-%y %I:%M:%S"))
    print "Input zonal feature class: {}".format(inZonalFc)
    print "Input raster stack: {}".format(inRaster)
    print "Output stack .csv: {}".format(stackCsv)
    print "Output aggregate fc: {}".format(outGdb)
    print "Output aggregate csv: {}".format(outCsv)
    print " n layers = {}".format(stack.nLayers)

    # 10/20/20: 
    #   ATL08 .gdb has already been filtered on can_open, so do not filter 
    #   GLAS .gdb has been filtered on everything *except* wflen, so filter on wflen
    if zonalType == 'ATL08':
        filterStr = None #"can_open != {}".format(float(340282346638999984013312))       
    elif zonalType == 'GLAS':
        filterStr = 'wflen < 50'        
    else:
        print "Zonal type {} not recognized".format(zonalType)
        return None
               
    # 1. Clip input zonal shp to raster extent. Output proj = that of stack  
    # 6/5 Try filtering src data in clip
    #tableName = inZones.baseName
    #sqlQry = 'SELECT * FROM {} WHERE {};'.format(tableName, filterStr.replace('!=', '<>'))
   
    clipZonal = os.path.join(outDir, '{}__{}.shp'.format(zonalType, stackName))
    if not os.path.isfile(clipZonal):
        print "\n1. Clipping input feature class to extent..."
        inZones.clipToExtent(stackExtent, stackEpsg, stackEpsg, 
                             clipZonal)#, sqlQry)
    else: print "\n1. Clipped feature class {} already exists...".format(clipZonal)
    
    # now zones is the clipped input ZFC object:
    zones = ZonalFeatureClass(clipZonal)
    # if checkResults == None, there are no features to work with
    if not checkZfcResults(zones, "clipping to stack extent"): 
        return None
    
    # Clean up the shapefile by removing unnecessary columns
    import pdb; pdb.set_trace()
    removeColumns = ['SHAPE_Leng', 'SHAPE_Area', 'SHAPE_Length', 'keep']
    zones = removeExtraColumns(zones, removeColumns)

    # 2. Filter footprints based on attributes - filter GLAS, not ATL08
    #    (10/20/2020): If filterStr is not None, filter on attributes
    if filterStr: # aka zonal type = GLAS
        print '\n2. Filtering on attributes using statement = "{}"...'.format(filterStr)
        filterShp = zones.filterAttributes(filterStr)
        
        zones = ZonalFeatureClass(filterShp)
        if not checkZfcResults(zones, "filtering on attributes"): 
            return None
        # zones is filtered shp
        
    else: # filterStr is None, aka zonal type = ATL08
        print "\n2. Not running attribute filter step"
        # zones is still the clipZonal shp
            
    # 3. Remove footprints under noData mask 
    noDataMask = stack.noDataLayer()

    # Mask out NoDataValues if there is a noDataMask. 
    if noDataMask:
        
        print "\n3. Masking out NoData values using {}...".format(noDataMask) 
        rasterMask = RasterStack(noDataMask)

        # If noDataMask is NOT in same projection as zonal fc, supply correct EPSG
        transEpsg = None
        if int(rasterMask.epsg()) != int(zones.epsg()):
            transEpsg = rasterMask.epsg() # Need to transform coords to that of mask
        
       
        zones.applyNoDataMask(noDataMask, transEpsg = transEpsg,
                                                             outShp = stackShp)
        
    # If there is not, just copy the clipped .shp to our output .shp 
    else:
        print "\n3. No NoDataMask. Not masking out NoData values." 
        cmd = 'ogr2ogr -f "ESRI Shapefile" {} {}'.format(stackShp, zones.filePath)
        print ' ', cmd #TEMP 10/7
        os.system(cmd)
           
        
    zones = ZonalFeatureClass(stackShp)
    if not checkZfcResults(zones, "masking out NoData values"):
        return None
    # Now zones is the filtered fc obj, will eventually have the stats added as attributes

    # Get stack key dictionary    
    layerDict = buildLayerDict(stack) # {layerNumber: [layerName, [statistics]]}
    
    # 4. Call zonal stats and return a pandas dataframe    
    print "\n4. Running zonal stats for {} layers".format(len(layerDict))
    zonalStatsDf = callZonalStats(stack, zones, layerDict)
   
    # 5. Complete the ZS DF by:
    #    adding stackName col, sunAngle if need be
    #    replacing None vals
    #    *removing columns if they exist: keep,SHAPE_Leng,SHAPE_Area
    zonalStatsDf = zonalStatsDf.fillna(stack.noDataValue)
    zonalStatsDf['stackName'] = [stackName for i in range(len(zonalStatsDf))]
    # *Do not do this, because it will result in different fields for 
    # individual shp vs larger gdb/csv. Until I can figure out better solution
    #for col in ['keep', 'SHAPE_Leng', 'SHAPE_Area']: 
        #zonalStatsDf = removeExtraColumns(zonalStatsDf, col)
    
    # Then add the zonal statistics columns from df to shp
    stackShp = addStatsToShp(zonalStatsDf, stackShp)

    # If there is an xml layer for stack, get sun angle and add as column to df
    stackXml = stack.xmlLayer()
    if stackXml:
        zonalStatsDf = addSunAngleColumn(zonalStatsDf, stackXml)
        
    # 6. Now write the stack csv, and finish stack-specific shp by adding 
    #    new stats columns to ZFC     
    zonalStatsDf.to_csv(stackCsv, sep=',', index=False, header=True)#), na_rep="NoData")
       
    # 7. Update the big csv and big output gdb (if True) by appending to them:
    updateOutputCsv(outCsv, zonalStatsDf)

    if outGdb:
        fc = ZonalFeatureClass(stackShp) # Update GDB now a method in FC.py
        fc.addToFeatureClass(outGdb)#, moreArgs = '-unsetFID')

    elapsedTime = round((time.time()-start)/60, 4)
    print "\nEND: {}\n".format(time.strftime("%m-%d-%y %I:%M:%S"))
    print " Completed in {} minutes".format(elapsedTime)

    # 8. Lastly, record some info to a batch-level csv:
    batchCsv = os.path.join(baseDir, '_timing', 
                    '{}_{}__timing.csv'.format(zonalType, stack.stackType()))
    os.system('mkdir -p {}'.format(os.path.dirname(batchCsv)))
    
    if not os.path.isfile(batchCsv):
        with open(batchCsv, 'w') as bc:
            bc.write('stackName,n layers,n zonal features,node,minutes\n')
    with open(batchCsv, 'a') as bc:
        bc.write('{},{},{},{},{}\n'.format(stackName, stack.nLayers, 
                                zones.nFeatures, platform.node(), elapsedTime))

    sys.stdout.flush()
    
    return None

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-rs", "--rasterStack", type=str, required=True, help="Input raster stack")
    parser.add_argument("-z", "--zonalFc", type=str, required=True, help="Input zonal shp/gdb")
    parser.add_argument("-o", "--bigOutput", type=str, required=True, help="Output for all stacks. If .gdb/.gpkg is provided, output and csv are created. If .csv, only .csv is created")
    parser.add_argument("-log", "--logOutput", action='store_true', help="Log the output")
    
    args = vars(parser.parse_args())

    main(args)
    