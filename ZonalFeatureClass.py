#!/usr/bin/env python
"""
Created on Tue Mar 24 02:03:17 2020
@author: mwooten3

ZonalFeatureClass describes a polygon .shp or .gdb
With methods designed specifically for Zonal Stats process

EVENTUALLY: Build a general Raster class and ZFC can inherit from it
"""
import os
#import tempfile

from osgeo import ogr
#from osgeo.osr import CoordinateTransformation

#from SpatialHelper import SpatialHelper

from rasterstats import zonal_stats

from FeatureClass import FeatureClass

"""
ZonalFeatureClass inherits the following from FeatureClass:
    self.filePath; self.extension; self.baseName; self.baseDir; self.dataset
    self.noDataValue; self.ogrDataType; self.ogrGeotransform; self.ogrProjection
    self.nColumns; self.nRows; self.nLayers   
    
    convertExtent(self, targetEpsg)
    epsg(self)
    extent(self)
    extractBand(self, bandN, outTif = None)
    toArray(self):   
"""


#------------------------------------------------------------------------------
# class ZonalFeatureClass
#------------------------------------------------------------------------------
class ZonalFeatureClass(FeatureClass):
    
    #--------------------------------------------------------------------------
    # __init__
    #--------------------------------------------------------------------------
    def __init__(self, filePath):

        # Initialize the base class
        super(FeatureClass, self).__init__(filePath)
        
        """
        # Check that the file is SHP or GDB
        extension = os.path.splitext(filePath)[1]       
        
        if extension != '.gdb' and extension != '.shp':
            raise RuntimeError('{} is not a SHP or GDB file'.format(filePath))

        self.filePath = filePath
        self.extension = extension
        """
        
        #zonalName = os.path.basename(self.filePath).strip(extension)
        self.zonalName = self.baseName#zonalName

        """            
        self.baseDir = os.path.dirname(self.filePath)

        # Set self.driver depending on the extention
        if self.extension == '.gdb':
            self.driver = ogr.GetDriverByName("FileGDB")
        else:
            self.driver = ogr.GetDriverByName("ESRI Shapefile")     
        
        self.dataset = self.driver.Open(self.filePath)
        self.layer = self.dataset.GetLayer()
        
        self.nFeatures = self.layer.GetFeatureCount()
        """
        
    #--------------------------------------------------------------------------
    # applyNoDataMask()
    #--------------------------------------------------------------------------    
    def applyNoDataMask(self, mask):

        # Expecting mask to be 0 and 1, with 1 where we want to remove data
        # This is specific to 3DSI and therefore is not kept in FeatureClass
        
        # Get name for output filtered shp:
        outShp = self.filePath.replace(self.extension, '__filtered-ND.shp')
        
        drv = ogr.GetDriverByName("ESRI Shapefile")
        ds = drv.Open(self.filePath)
        layer = ds.GetLayer()
        
        # Collect list of FIDs to keep
        keepFIDs = []
        for feature in layer:

            # Get polygon geometry and export to WKT for ZS 
            wktPoly = feature.GetGeometryRef().ExportToIsoWkt()
            
            # Get info from mask underneath feature
            z = zonal_stats(wktPoly, mask, stats="mean")
            out = z[0]['mean']            
            if out >= 0.6 or out == None: # If 60% of pixels or more are NoData, skip
                continue
            
            # Else, add FID to list to keep
            keepFIDs.append(feature.GetFID())

        # Filter and write the features we want to keep to new output DS:
        ## Pass ID's to a SQL query as a tuple, i.e. "(1, 2, 3, ...)"
        layer.SetAttributeFilter("FID IN {}".format(tuple(keepFIDs)))

        dsOut = drv.CreateDataSource(outShp)
        layerOutName = os.path.basename(outShp).replace('.shp', '')
        layerOut = dsOut.CopyLayer(layer, layerOutName)
        
        if not layerOut: # If CopyLayer failed for whatever reason
            print "Could not remove NoData polygons"
            return self.filePath
        
        ds = layer = dsOut = layerOut = feature = None
        
        return outShp
    """
    #--------------------------------------------------------------------------
    # clipToExtent()
    #--------------------------------------------------------------------------    
    def clipToExtent(self, clipExtent, extentEpsg, outClip = None):
        
        # Expect extent to be tuple = (xmin, ymin, xmax, ymax)

        if not outClip:
            clipFile = '{}.shp'.format(tempfile.mkdtemp())
        else:
            clipFile = outClip

        # If EPSG of given coords is different from the EPSG of the feature class
        if str(extentEpsg) != str(self.epsg()):
            
            # Then transform coords to correct epsg
            (ulx1, lry1, lrx1, uly1) = clipExtent
            ulx, uly = SpatialHelper().convertCoords((ulx1, uly1), extentEpsg, self.epsg())
            lrx, lry = SpatialHelper().convertCoords((lrx1, lry1), extentEpsg, self.epsg())
            
            clipExtent = (ulx, lry, lrx, uly)
        
        extent = ' '.join(map(str,clipExtent))
    
        cmd = 'ogr2ogr -clipsrc {} -spat {} -f '.format(extent, extent) + \
                    '"ESRI Shapefile" {} {}'.format(clipFile, self.filePath)
        os.system(cmd)
        
        return clipFile

    #--------------------------------------------------------------------------
    # createCopy()
    #--------------------------------------------------------------------------    
    def createCopy(self, copyName):
        
        cmd = 'ogr2ogr {} {}'.format(copyName, self.filePath)
        os.system(cmd)
        
        return copyName  
        
    #--------------------------------------------------------------------------
    # epsg() [projection] **CHECK THIS!!!**
    #--------------------------------------------------------------------------
    def epsg(self):         

        srs = self.layer.GetSpatialRef()
        
        return srs.GetAuthorityCode(None)    
    
    #--------------------------------------------------------------------------
    # extent()
    #--------------------------------------------------------------------------
    def extent(self):
        
        (ulx, lrx, lry, uly) = self.layer.GetExtent()
        
        return (ulx, lry, lrx, uly)    
    """
    """ just use zonalName for now   
    #--------------------------------------------------------------------------
    # zonalType() # ATL08 or GLAS
    #--------------------------------------------------------------------------
    def extent(self):
        
        if 
    
    """
    
    
    """  ###################################################################
    Stuff from RasterStack, might not be applicable here:
    #--------------------------------------------------------------------------
    # convertExtent()
    #--------------------------------------------------------------------------
    def convertExtent(self, targetEpsg):
                
        sourceSrs = osr.SpatialReference()
        sourceSrs.ImportFromEPSG(int(self.epsg())) 
    
        targetSrs = osr.SpatialReference()
        targetSrs.ImportFromEPSG(int(targetEpsg))
        
        (ulx, lry, lrx, uly) = self.extent()
    
        coordTrans = osr.CoordinateTransformation(sourceSrs, targetSrs)
        ulxOut, ulyOut = coordTrans.TransformPoint(ulx, uly)[0:2]
        lrxOut, lryOut = coordTrans.TransformPoint(lrx, lry)[0:2]
    
        return (ulxOut, lryOut, lrxOut, ulyOut)

    #--------------------------------------------------------------------------
    # epsg() [projection]
    #--------------------------------------------------------------------------
    def epsg(self):            

        prj = self.dataset.GetProjection()
        srs = osr.SpatialReference(wkt=prj)
        
        return srs.GetAuthorityCode(None)
            
    #--------------------------------------------------------------------------
    # extent()
    #--------------------------------------------------------------------------
    def extent(self):
        
        ulx, xres, xskew, uly, yskew, yres  = self.dataset.GetGeoTransform()
        lrx = ulx + (self.dataset.RasterXSize * xres)
        lry = uly + (self.dataset.RasterYSize * yres)
        
        return (ulx, lry, lrx, uly)

    #--------------------------------------------------------------------------
    # nLayers()
    #--------------------------------------------------------------------------      
    def nLayers(self):
        
        try:
            nLayers = self.dataset.RasterCount
        except:
            nLayers = None
            
        return nLayers
     """   
