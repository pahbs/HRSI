#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import numpy as np

from osgeo import osr, ogr
#from osgeo.osr import SpatialReference
from osgeo.osr import CoordinateTransformation

import time
import tempfile
import argparse
import platform
import pandas as pd

from FeatureClass import FeatureClass

"""

1/15/2021:
Repurposing old h5ToShp script to skip the .h5 to .csv conversion and go 
straight from .csv to .shp

Also adding beam_type addition to attributes


To convert an ATL08 .csv  file to a polygon shp

Steps:
    From coordinates in the csv, get the UTM zone
    Convert lat/lon coords to UTM coords
    Use coords as input to function from Eric, which will make the shapefile
    Add the other columns from the .csv as attributes in the shapefile

    Added 5/11 (lines ~327+):
        Writing number of points for each h5 file to spreadsheet
        Calling the update GDB function on outShp
        
    Added/edited 5/14 (~342): 
        Output gdb is now platform-specific GPKG for parallel processing
        Will merge together after
        
        
    Added 5/27: Continent argument (default na); editing output dirs to include
        Also, changing 14m to 11m for polygon creation 
"""

###############################################################################
###############################################################################
"""
Part I: Functions to convert a list of UTM points into ICESat2 polygons
# GLAM, Applied Research Laboratories at the University of Texas
# @author: Eric Guenther

    Notes:

    # Example: White Sands Missle Range, WGS/UTM Zone 13
#    easting = np.array([370674.2846469 ,
#       370664.88296774,
#       370655.48708123,])
#    
#    northing = np.array([3640352.68651837,
#       3640452.21808673,
#       3640552.17262566,])
    
    # 2. Reproject ATL08 points to UTM Easting and Northings then run this
    createShapefiles(easting, northing, 14, 100, utmEpsg, "atl08_example.shp")
"""
###############################################################################
# calculateangle - Eric
def calculateangle(x1,x2,y1,y2):
    if (x2 - x1) == 0:
        slope = np.inf
    else:
        slope = (y2 - y1)/(x2 - x1)
    degree = np.rad2deg(np.arctan(slope))
    return degree

# calculategrounddirection - Eric
def calculategrounddirection(xx,yy):
    degree = np.zeros(len(xx))
    for i in range(0,len(xx)):
        if i == 0:
            degree[i] = calculateangle(xx[i], xx[i+1], yy[i], yy[i+1])
        elif i == (len(xx))-1:
            degree[i]  = calculateangle(xx[i-1], xx[i], yy[i-1], yy[i])
        else:
            degree[i]  = calculateangle(xx[i-1], xx[i+1], yy[i-1], yy[i+1])
    return degree
    
# rotatepoint - Eric
def rotatepoint(degree,xpos,ypos):
    angle = np.deg2rad(degree)
    xrot = (xpos * np.cos(angle)) - (ypos * np.sin(angle)) 
    yrot = (xpos * np.sin(angle)) + (ypos * np.cos(angle))
    return xrot, yrot

# calculatecorners - Eric
def calculatecorners(degree,xcenter,ycenter,width,height):
    # Set corner values
    xul = -width / 2
    yul = height / 2
    xur = width / 2
    yur = height / 2
    xll = -width / 2
    yll = -height / 2
    xlr = width / 2
    ylr = -height / 2
    
    # Rotate based on the angle degree
    xul, yul = rotatepoint((degree-90),xul,yul)
    xur, yur = rotatepoint((degree-90),xur,yur)
    xll, yll = rotatepoint((degree-90),xll,yll)
    xlr, ylr = rotatepoint((degree-90),xlr,ylr)
    
    # Add corner values to centeroid
    xul = xcenter + xul
    yul = ycenter + yul
    xur = xcenter + xur
    yur = ycenter + yur
    xll = xcenter + xll
    yll = ycenter + yll
    xlr = xcenter + xlr
    ylr = ycenter + ylr
    
    return xul, yul, xur, yur, xll, yll, xlr, ylr

# addAttributeColumns - create columns in GDAL layer
def addAttributeColumns(layer, attributeDf):
    
    typeMap = {'float64': ogr.OFTReal, 'int64': ogr.OFTInteger, 
                                               'object': ogr.OFTString}

    for col in attributeDf.columns:
        colName = str(col)
        colType = typeMap[str(attributeDf[col].dtype)]

        layer.CreateField(ogr.FieldDefn(colName, colType))

    return None

# createShapefiles - Eric, with additions from Maggie
# 1/19: Do not actually need to use the xx and yy lists, because those will be 
# in the attributes DF and the xx lists are from before filtering. As a 
# compromise, to not interfere too much, create the xx and yy lists from the df
# instead of sending to function
def createShapefiles(width, height, epsg, attributes, outfile):
    
    xx = np.asarray(attributes.utmLon)
    yy = np.asarray(attributes.utmLat)
    # Now xx and yy are only the coords from rows that were not filtered
    
    # Generate list of degrees
    degreelist = calculategrounddirection(xx,yy)
    
    # Define Esri Shapefile output
    driver = ogr.GetDriverByName('Esri Shapefile')
    
    # Name output shape file (foo.shp)
    ds = driver.CreateDataSource(outfile)
    
    # Define spatial reference based on EPSG code 
    # https://spatialreference.org/ref/epsg/
    srs = ogr.osr.SpatialReference()
    srs.ImportFromEPSG(epsg)
    
    # Create file with srs
    layer = ds.CreateLayer('', srs, ogr.wkbPolygon)
    
    # Create arbitary id field
    layer.CreateField(ogr.FieldDefn('id', ogr.OFTInteger))
    defn = layer.GetLayerDefn()
    
    addAttributeColumns(layer, attributes)
    
    # Create a new feature (attribute and geometry)
    for i in range(0,len(xx)):
        # Generate the corner points
        xul, yul, xur, yur, xll, yll, xlr, ylr  = \
        calculatecorners(degreelist[i],xx[i],yy[i],width,height)     
        
        # Create rectangle corners
        ring = ogr.Geometry(ogr.wkbLinearRing)
        ring.AddPoint(xul, yul)
        ring.AddPoint(xur, yur)
        ring.AddPoint(xlr, ylr)
        ring.AddPoint(xll, yll)
        ring.AddPoint(xul, yul)
        
        # Create polygon from corners
        poly = ogr.Geometry(ogr.wkbPolygon)
        poly.AddGeometry(ring)
        
        # Export well-known binary
        wkb = poly.ExportToWkb()
        
        # Assign arbitary number to field ID
        feat = ogr.Feature(defn)
        feat.SetField('id', i)
        
        # Assign a row of attributes to attribute columns
        # pdf.at[i, col] is equivalent of pdf.iloc[[i]][col].values[0] but doesn't always work
        # I believe it does not work if you've removed any data from the df (ie the index no longer starts at 0)
        # With resetting the index after filtering, this should still work
        for col in attributes.columns: 
            feat.SetField(col, attributes.at[i, col])
                
        # Make a geometry, from Shapely object
        geom = ogr.CreateGeometryFromWkb(wkb)
        feat.SetGeometry(geom)
        
        # Write out geometry
        layer.CreateFeature(feat)
        
        # Remove ring and poly
        ring = poly = None
    
    # Remove feat and geom
    feat = geom = None
    
    # Save and close everything
    ds = layer = feat = geom = None   
###############################################################################
###############################################################################
  
"""
Functions to do conversion of lat/lon points in csv to appropriate UTM projection
"""
# add UTM coords to pandas dataframe
def addAttributesToDf(pdf, utmLonList, utmLatList, epsg, bname):
    
    # Add beam_type column
    pdf.loc[( (pdf.orb_orient == 1 ) & (pdf['gt'].str.contains('r')) ), "beam_type"] = 'Strong' 
    pdf.loc[( (pdf.orb_orient == 1 ) & (pdf['gt'].str.contains('l')) ), "beam_type"] = 'Weak'
    pdf.loc[( (pdf.orb_orient == 0 ) & (pdf['gt'].str.contains('r')) ), "beam_type"] = 'Weak'
    pdf.loc[( (pdf.orb_orient == 0 ) & (pdf['gt'].str.contains('l')) ), "beam_type"] = 'Strong'

    # Add UTM coordinates
    pdf['utmLon'] = utmLonList
    pdf['utmLat'] = utmLatList
    
    # Add EPSG code
    pdf['epsg']  = [epsg for i in range(0,len(utmLonList))]
    
    # Add full path to input h5 file
    pdf['ATLfile']  = [bname for i in range(0,len(utmLonList))]
    
    return None

# 1/19: Added this to Remove NoData rows from the dataframe
def filterRows(pdf):
    
    outDf = pdf[pdf.can_open != 3.402823466385289e+38] # Must be written exactly like this
    
    # After filtering we need to reset the index in the df
    # Drop the index column because it's just going to be moot when combined with others
    outDf.reset_index(drop=True, inplace=True)    
    
    nFiltered = len(pdf) - len(outDf)
    
    return outDf, nFiltered

# 1/19: Added this to fix the columns that were encoded improperly and have the b'...' issue
def fixColumns(pdf):
    
    # Columns that have issues and the output types we want them as:
    # yr (int), m (int), d (int), gt (str)
    
    pdf['yr'] = pdf['yr'].str.strip("b'").astype("int")
    pdf['m'] = pdf['m'].str.strip("b'").astype("int")
    pdf['d'] = pdf['d'].str.strip("b'").astype("int")
    pdf['gt'] = pdf['gt'].str.strip("b'").astype("str")

    return pdf

# Get largest overlapping UTM zone for a bounding box
def getUTM(ulx, uly, lrx, lry):
    
    # If we are outside of utm lat. shp bounds, reset to bound
    if uly >= 84.0: uly = 84.0
    if lry <= -80.0: lry = -80.0
    
    """ Script fails for other reasons if there is  only one footprint.
        Instead of forcing it through, just skip if only one footprint.
        Can delete this:
    # It may be the case that there is just one ATL footprint for file. If so,
    # we need to enlarge the extent by a bit to ensure we can get UTM zone
    if ulx == lrx:
        lrx += 0.003
    if uly == lry:
        uly += 0.003
    """
    
    # Clip UTM to shp according to extent
    utmShp = '/att/gpfsfs/briskfs01/ppl/mwooten3/GeneralReference/' + \
                                        'UTM_Zone_Boundaries.shp'
    clipFile = tempfile.mkdtemp()
    
    cmd = 'ogr2ogr -clipsrc {} {} {} {} '.format(ulx, lry, lrx, uly) + \
    ' -f "ESRI Shapefile" -select "Zone_Hemi" "{}" "{}"'.format(clipFile, utmShp)
      
    os.system(cmd)
    
    # Read clipped shapefile
    driver = ogr.GetDriverByName("ESRI Shapefile")
    ds = driver.Open(clipFile, 0)
    layer = ds.GetLayer()

    # Find zone with largest area of overlap
    maxArea = 0
    for feature in layer:
        area = feature.GetGeometryRef().GetArea()
        if area > maxArea:
            maxArea = area
            zone, hemi = feature.GetField('Zone_Hemi').split(',')

    proj4 = '+proj=utm +zone={} +ellps=WGS84 +datum=WGS84 +units=m +no_defs'.format(zone)
    epsg = '326{}'.format(zone.zfill(2))
    if hemi.upper() == 'S':
        proj4 += ' +south' 
        epsg = '327{}'.format(zone.zfill(2))
        
    return epsg
    
# Convert a lat/lon point to UTM easting/northing point
def latLonToUtmPoint(lon, lat, targetEpsg):
    
    targetSrs = osr.SpatialReference()
    targetSrs.ImportFromEPSG(int(targetEpsg))
    
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    
    coordTrans = CoordinateTransformation(srs, targetSrs)
    utmLon, utmLat = coordTrans.TransformPoint(lon, lat)[0:2]
    
    return utmLon, utmLat

# Convert a list of lat/lon points to list of UTM points  
def latLonToUtmLists(lonList, latList, targetEpsg):
    
    utmList = []
    for i in range(0, len(lonList)):
        utmList.append(latLonToUtmPoint(lonList[i], latList[i], targetEpsg))
    
    easting, northing = zip(*utmList)
    utmLonList = np.asarray(easting, dtype=np.float64)
    utmLatList = np.asarray(northing, dtype=np.float64)
    
    return utmLonList, utmLatList  
        
def main(args):

    # Unpack args, check inputs and set up vars  
    inCsv  = args['input']
    outGdb = args['outGdb']
    cont = args['continent'] # eu or na (default na)
        
    if cont != 'na' and cont != 'eu': # Check that continent is na or eu
        raise RuntimeError("Continent must be 'na' or 'eu'")
        
    if not inCsv.endswith('.csv'):
        sys.exit('Input file must have an .csv extension')  
    
    if outGdb is not None:
        if not outGdb.endswith('.gdb') and not outGdb.endswith('.gpkg')       \
        and not outGdb.endswith('.shp'):
            sys.exit('Output GDB must have an .gdb, .gpkg, or .shp extension') 
        
    bname = os.path.basename(inCsv).replace('.csv', '')
    
    # Set output dir variables and make dirs
    baseOutdir = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/ATL08/{}'.format(cont)
    #outCsvDir  = os.path.join(baseOutdir, 'flight_csvs')
    outShpDir  = os.path.join(baseOutdir, 'flight_shps')
    outLogDir  = os.path.join(baseOutdir, 'flight_logs')
    trackDir   = os.path.join(baseOutdir, 'tracking_csvs')
    
    for d in [outShpDir, outLogDir, trackDir]:
        os.system('mkdir -p {}'.format(d))
        
    # Tracking .csv:
    trackCsv = os.path.join(trackDir, 'ATL08_{}_v3__featureCount-{}.csv'.format(cont, platform.node()))
        
    # Log output:
    logFile = os.path.join(outLogDir, 'ATL08-csv_to_shp__{}__Log.txt'.format(bname))
    print "See {} for log".format(logFile)
    so = se = open(logFile, 'a', 0) # open our log file
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0) # re-open stdout without buffering
    os.dup2(so.fileno(), sys.stdout.fileno()) # redirect stdout and stderr to the log file opened above
    os.dup2(se.fileno(), sys.stderr.fileno())
    
    print "BEGIN: {}".format(time.strftime("%m-%d-%y %I:%M:%S"))
    print "Continent: {}".format(cont)
    print "Node: {}".format(platform.node())
    print ".csv File: {}".format(inCsv)
    print ".gdb/.gpkg: {}\n".format(outGdb)
    
    #outCsv = os.path.join(outCsvDir, '{}.csv'.format(bname))
    outShp = os.path.join(outShpDir, '{}.shp'.format(bname))
          
    # Check if output shp already exists
    if os.path.isfile(outShp): 
        print "\n Output {} already exists".format(outShp)
        return None
    
    """
    # Skipping .h5 file portion now
    # 1. H5 file --> outCsv (Paul's extract code):
    extractCode = '/att/home/mwooten3/code/icesat2/extract_atl08.py'
    cmd = 'python {} -i {} -o {}'.format(extractCode, inH5, outCsvDir)
    os.system(cmd)
    
    # Check if ICESAT2GRID (extract) failed
    if not os.path.isfile(outCsv): 
        print "\n Output {} was not created".format(outCsv)
        return None
    """
    
    # 1. Import csv into pandas df and extract lat/lon columns into arrays    
    pdf = pd.read_csv(inCsv)
    latArr = np.asarray(pdf['lat'])
    lonArr = np.asarray(pdf['lon']) 
    
    nInputRows = len(pdf)
    
    # calculategrounddirection() fails if there is only one footprint in csv.
    # Skip if only one footprint:
    if len(latArr) <= 1:
        print "\n CSV {} has fewer than two rows. Skipping".format(inCsv)
        return None 
    
    # 2. Convert lat/lon lists to appropriate UTM zone
    epsg = getUTM(np.min(lonArr), np.max(latArr), np.max(lonArr), np.min(latArr))
    utmLonList, utmLatList = latLonToUtmLists(lonArr, latArr, epsg)
   
    # 3. Add more information to attributes/pandas df
    addAttributesToDf(pdf, utmLonList, utmLatList, epsg, bname)
    
    # 4. Remove NoData rows (h_can = 3.402823e+23)
    pdf, nFiltered = filterRows(pdf)
    
    # Cannot continue if at least two points do not exist
    if len(pdf) <= 1:
        print "\n CSV {} has fewer than two rows after filtering. Skipping".format(inCsv)
        return None  
    
    # 5. Edit some columns
    # 1/19: Added this to fix the columns that were encoded improperly and have the b'...' issue
    pdf = fixColumns(pdf)
    
    # 6. Run Eric's functions to get polygon shp - 5/27 using 11m
    #     xx and yy no longer match the filterd dataframe. Recreating these 
    #     lists in the function now
    createShapefiles(11, 100, int(epsg), pdf, outShp)
    
    # 7. Track info: .csv file, node, number of input .csv features, number of filtered features, number of output .shp features
    outFc = FeatureClass(outShp)
    
    with open(trackCsv, 'a') as c:
        c.write('{},{},{},{},{}\n'.format(inCsv, platform.node(), nInputRows, nFiltered, outFc.nFeatures))
    
    # 8. If output is specified, update the output .gdb (or .gpkg?)
    if outGdb is not None:
        outFc.addToFeatureClass(outGdb)
    outFc = None # Close feature class
    
    print "\nEND: {}\n".format(time.strftime("%m-%d-%y %I:%M:%S"))

    return outShp

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    
    parser.add_argument("-i", "--input", required=True, type=str, 
                        help="Specify the input ICESAT H5 file")
    parser.add_argument("-gdb", "--outGdb", required=False, type=str, 
                        help="Specify the output GDB, if you wish to write to one")
    parser.add_argument("-continent", "--continent", type=str, required=False,
                                default = 'na', help="Continent (na or eu)")
    
    args = vars(parser.parse_args())
    
    main(args)
    

    
    
    
    
    
    
    
    
    
