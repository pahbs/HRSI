# -*- coding: utf-8 -*-
"""
Created on Fri Jun  5 01:05:33 2020
@author: mwooten3

See 3DSI/GLAS_gdb_notes.txt

Purpose: To build a GDB of buffered GLAS shots from csv(s)

Process:
    Iterate through big boreal GLAS .csv
    For each row, convert lat/lon to OGR point object
    Buffer the point object by X m and save to output polygon .gdb
    Write attributes from row to .gdb
    
Intermediate data: /att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS/
Final .gdb will be moved to: /att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_naBoreal.gdb

NOTES:
    Working with GPKG, because I think the FileGDB driver is finicky.
    If input is .gdb, will write to .gpkg then convert to .gdb
    Added overwrite step (7/30) so that we can pick up if we left off. Still testing though
    
"""
import os
import csv
import argparse
import datetime
import platform

#import pandas as pd
from collections import OrderedDict

from osgeo import ogr# gdal, ogr, osr
from FeatureClass import FeatureClass
from SpatialHelper import SpatialHelper

# Breaking up the creation into batches between the 10 crane nodes:
# node --> (x, y) where the y is not inclusive (skip when count = y)
countDict = {'crane101': '0,5000000',
             'crane102': '5000000,10000000',
             'crane103': '10000000,15000000',
             'crane104': '15000000,20000000',
             'crane105': '20000000,25000000',
             'crane106': '25000000,30000000',
             'crane107': '30000000,35000000',
             'crane108': '35000000,40000000',
             'crane109': '40000000,45000000',
             'crane110': '45000000,50000000'             
            }

def addBufferToOutput(rowDict, layer, bufferDist):
    
    # Add a lat/lon point with X m buffer + attributes to a layer
    
    layerDefn = layer.GetLayerDefn()
    layerSrs = layer.GetSpatialRef()
    layerEpsg = layerSrs.GetAuthorityCode(None)
    
    lat = float(rowDict['lat'])
    lon = float(rowDict['lon'])

    # Create point OGR point from lat/lon
    pointGeom = ogr.Geometry(ogr.wkbPoint)
    pointGeom.AddPoint(lon, lat)
    
    # Convert point to meter proj
    utmEpsg = SpatialHelper().determineUtmEpsgFromShape(pointGeom, layerEpsg)
    pointProj = SpatialHelper().reprojectShape(pointGeom, layerEpsg, utmEpsg)
    
    # Get buffered polygon geometry from projected point
    polyProj = pointProj.Buffer(int(bufferDist))
    
    # Then convert polygon back to lat/lon proj
    polyBuffer = SpatialHelper().reprojectShape(polyProj, utmEpsg, layerEpsg)
    
    # Create buffered polygon feature
    polyFeature = ogr.Feature(layerDefn)
    polyFeature.SetGeometry(polyBuffer)
    
    # Add attributes to OGR columns for feature
    for colName in rowDict.keys():
        polyFeature.SetField(colName, rowDict[colName])
        
    # Add feature with attributes to layer
    layer.CreateFeature(polyFeature)
    
    layer = polyFeature = pointGeom = None
    
    return None    

def addShotDate(rowDict):

    # Get the shot day (julian) and year from the date column
    # "date" column represents days since January 1, 2003
    daysSinceStart = float(rowDict['date'])
    startDate = datetime.datetime.strptime('2003-01-01', "%Y-%m-%d")
    shotDate = startDate + datetime.timedelta(days=daysSinceStart)
    
    rowDict['shotYear']  = shotDate.timetuple().tm_year
    rowDict['shotMonth'] = shotDate.timetuple().tm_mon
    rowDict['shotDay']   = shotDate.timetuple().tm_mday # day of the month
    rowDict['shotJD']    = shotDate.timetuple().tm_yday # Julian day

    return rowDict
    
def convertGpkgToGdb(gpkg, gdb):
    
    # Call ogr2ogr and convert to .gdb
    cmd = "ogr2ogr -f 'FileGDB' {} {}".format(gdb, gpkg)
    print " {}".format(cmd)
    os.system(cmd)
    
    return gdb

def createFields(layer, columns):

    # If column is string type (ogr.OFTString), it will be in this list
    # all others should be float64 (ogr.OFTReal)
    stringColumns  = ['datatake']
    intColumns = ['bufferSize', 'shotYear', 'shotMonth', 'shotDay', 'shotJD']
    
    # Add unique ID separately so that it's first
    layer.CreateField(ogr.FieldDefn('uniqueID', ogr.OFTInteger))

    for col in columns:        
        
        # Determine column type (default is Float)
        colType = ogr.OFTReal
        
        if col in stringColumns: 
            colType = ogr.OFTString
            
        if col in intColumns:
            colType = ogr.OFTInteger
        
        layer.CreateField(ogr.FieldDefn(col, colType))

    return None

def rowDictToCsv(rowDict, writeCsv):
    
    # Write header if .csv does not exist;
    if not os.path.isfile(writeCsv):
         with open(writeCsv, 'w') as wc: 
             wc.write('{}\n'.format(','.join(rowDict.keys())))
             
    # Write row
    with open(writeCsv, 'a') as wc: 
        wc.write('{}\n'.format(','.join(rowDict.values())))
  
    return None
           
def filterRow(rowDict):
    
    # Only use points if the following criteria is True
    # 'FRir_qaFlag' == 15
    # 'satNdx' < 2
    # 'cld1_mswf' < 15
    
    removePoint = True
    
    if int(rowDict['FRir_qaFlag']) == 15 and int(rowDict['satNdx']) < 2 and \
        int(rowDict['cld1_mswf']) < 15:
    
        removePoint = False
           
    return removePoint

def main(args):
    
    startTime = datetime.datetime.now()

    # Arguments: Input GLAS .csv; output directory; buffer distance (m)
    inCsv      = args['inputCsv']
    outGdb     = args['outputGdb']
    bufferDist = args['bufferDistance']
    
    overwrite = True # Set to False if we want to append to gdb/gpkg

    node = platform.node()
    
    """
    inCsv = '/att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/data/glas/circ_boreal/gla01-boreal50up-fix2-data.csv'
    outGdb = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS/GLAS_boreal.gdb'
    bufferDist = 15
    """
    
    # Get the header. Chaotic, but the column names for the fix2-data .csv are here
    hdrFile = '/att/nobackup/mwooten3/3DSI/GLAS/GLAS_csv_columns.txt'
    with open(hdrFile, 'r') as hf:
        hdr = hf.readline().strip('\n').split(',')
        
    # To save rows with bad lat/lon values to csv:
    #badCoordCsv = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS/badCoords.csv'
    
    # To save info about batches (already made header in file)
    batchCsv = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS/batchTrack.csv'
    
    # Hardcode the additional columns we want to add to .gdb
    # Doing uniqueID separate because we want it to be first
    extraColumns = ['shotYear', 'shotMonth', 'shotDay', 'shotJD', 'bufferSize']
    
    # Get basename (aka eventual layer name) of output .gdb
    ext = os.path.splitext(outGdb)[1] 
    bname = os.path.basename(outGdb).strip(ext)
    
    # Get the output .gpkg because that's what we are writing to 
    if ext == '.gdb':
        outGpkg = outGdb.replace('.gdb', '__{}.gpkg'.format(node))
        convertToGdb = True
    elif ext == '.gpkg':
        outGpkg = outGdb.replace('.gpkg', '__{}.gpkg'.format(node))
        convertToGdb = False
    convertToGdb = False #TEMP - set to False until last batch
    
    driver = ogr.GetDriverByName('GPKG')
    
    # Create the output if overwrite is True or file does not exist
    if overwrite or not os.path.isfile(outGpkg):    
        print "\nCreating {}...".format(outGpkg)
                
        # Set up the output gdb
        ds = driver.CreateDataSource(outGpkg)
        srs = ogr.osr.SpatialReference()
        srs.ImportFromEPSG(4326) # WGS84 Lat/Lon
        
        # Create file with srs 4326; layerName = basename
        layer = ds.CreateLayer(bname, srs, ogr.wkbPolygon)
        
        # Create the fields in the output .gdb using hdr contents and addl columns   
        createFields(layer, hdr + extraColumns)
     
    # Open the layer if not overwriting
    else:
        print "\nOpening and appending to {}...".format(outGpkg)
        
        ds = driver.Open(outGpkg)
        layer = ds.GetLayer()
        
    # 8/14 Now that we are hardcoding batches for crane nodes, get that info
    cntStart = int(countDict[node].split(',')[0])
    cntEnd   = int(countDict[node].split(',')[1])
        
    # Iterate through rows in .csv and write buffered polygons to output
    totCnt = 0 # count all of the points, not just the unfiltered ones
    badCnt = 0 # count of points with missing columns
    fltrCnt = 0 # count of points that were filtered
    cnt = 0 # count of points added to gdb
    
    badLon = [] # list of longitudes that are < or > 180
    #import pdb; pdb.set_trace()
 
    with open(inCsv, 'r') as c:
        
        data = csv.reader(c)
        
        for row in data:
            
            totCnt += 1
            
            # split into batch depending on crane node
            if totCnt < cntStart or totCnt >= cntEnd:
                continue

            # Print count at every half millionth point (total not just unfiltered)
            if totCnt % 50000 == 0:  # count ever 5,000 row now
                print " Total count so far = {}".format(totCnt)
                print "  (Added count = {})".format(cnt)
                #layer.SyncToDisk() # Hopefully save info from memory to gpkg - dont think this worked            
        
            # Sometimes a row has fewer columns than the header. Skip those
            if len(row) != len(hdr):
                badCnt += 1
                continue
                               
            # Create dict that corresponds to the hdr with contents in row:
            rowDict = OrderedDict() # keep column order
            for c in hdr: rowDict[c] = row[hdr.index(c)]

            # Some rows have a too large/small longitude. Record info in list
            if float(rowDict['lon']) > 180 or float(rowDict['lon']) < -180:            
                badLon.append('{}: {}'.format(rowDict['lon'], totCnt))
                rowDict['totCnt'] = str(totCnt)
                #rowDictToCsv(rowDict, badCoordCsv) # save to .csv
                continue # and skip
                
            # Check for lat too just in case
            if float(rowDict['lat']) > 90 or float(rowDict['lat']) < -90:
                badLon.append('{}: {}'.format(rowDict['lat'], totCnt))
                rowDict['totCnt'] = str(totCnt)
                #rowDictToCsv(rowDict, badCoordCsv)                
                continue # and skip
                
            # Filter the row. Will return False if we want to keep the point
            if filterRow(rowDict):
                fltrCnt += 1
                continue # if True, skip point                

            rowDict['uniqueID']  = cnt + 1 # Add unique ID to dict
            rowDict['bufferSize'] = bufferDist
            rowDict = addShotDate(rowDict)
            
            # Now send the row to function to buffer and add to output
            addBufferToOutput(rowDict, layer, bufferDist)     

            cnt += 1 # only add to count if point is added
                        
    nFeat1 = FeatureClass(outGpkg).nFeatures

    # Try destroying the ds to free up memory and save:
    ds.Destroy() 

    endTime1 = datetime.datetime.now()
    elapsedTime1 = endTime1 - startTime
    
    # Record info:
    # node,start,end,totCnt,addCnt,nFeat,missingCols,badCoords,filtered, time (minutes)
    rec = '{},{},{},{},{},{},{},{},{},{}\n'.format(node, cntStart, cntEnd, totCnt,
                                   cnt, nFeat1, badCnt, len(badLon), fltrCnt, 
                                   *divmod(elapsedTime1.total_seconds(), 60))
    with open(batchCsv, 'a') as bc:
        bc.write(rec)
    
    print "\nFinished creating .gpkg ({} features) in ~{} minutes".           \
                      format(nFeat1, *divmod(elapsedTime1.total_seconds(), 60))
                      
    print "\nRow counts:"
    print " Total rows = {}".format(totCnt)
    print " Filtered rows = {}".format(fltrCnt)
    print " Rows with missing columns = {}".format(badCnt)
    print " Rows with longitude value too large = {}".format(len(badLon))
    print " Added rows = {}".format(cnt)
    
    if convertToGdb:
        
        print "\nConverting output .gpkg to .gdb ({})".format(outGdb)
        
        convertGpkgToGdb(outGpkg, outGdb)
        
        nFeat2 = FeatureClass(outGdb).nFeatures
        
        endTime2 = datetime.datetime.now()
        elapsedTime2 = endTime2 - endTime1
        
        print "\nFinished converting to .gdb ({} features) in ~{} minutes\n". \
                      format(nFeat2, *divmod(elapsedTime2.total_seconds(), 60))
        
        return outGdb
        
    return outGpkg
    
    
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    
    # Maybe this will be a raster not a shp. or a table with lat/lon
    parser.add_argument("-i", "--inputCsv", type = str, required = True,
                                                      help = "Input GLAS .csv")
    parser.add_argument("-o", "--outputGdb", type = str, required = True, 
                                                           help = "Output GDB")
    parser.add_argument("-b", "--bufferDistance", type = str, required = False,
                       default = 15, help = "Buffer radius in m; default = 15")
    
    args = vars(parser.parse_args())

    main(args)





