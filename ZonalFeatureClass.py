#!/usr/bin/env python
"""
Created on Tue Mar 24 02:03:17 2020
@author: mwooten3

ZonalFeatureClass describes a polygon .shp or .gdb
With methods designed specifically for Zonal Stats process

EVENTUALLY: Build a general Raster class and ZFC can inherit from it
"""
import os
import tempfile

from osgeo import ogr
#from osgeo.osr import CoordinateTransformation

from SpatialHelper import SpatialHelper

from rasterstats import point_query
from shapely.geometry import Point

#------------------------------------------------------------------------------
# class ZonalFeatureClass
#------------------------------------------------------------------------------
class ZonalFeatureClass(object):
    
    #--------------------------------------------------------------------------
    # __init__
    #--------------------------------------------------------------------------
    def __init__(self, filePath):
        
        """
        # Probably doesn't make sense to keep this here but whatever
        self.zonalDir = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/ZonalStats/'
        """
        
        # Check that the file is SHP or GDB
        extension = os.path.splitext(filePath)[1]       
        
        if extension != '.gdb' and extension != '.shp':
            raise RuntimeError('{} is not a SHP or GDB file'.format(filePath))

        self.filePath = filePath
        self.extension = extension

        zonalName = os.path.basename(self.filePath).strip(extension)
        self.zonalName = zonalName
        
        self.inDir = os.path.dirname(self.filePath)

        #For now, ignore driver stuff so we can test with gdb
        if self.extension == '.gdb':
            self.driver = ogr.GetDriverByName("FileGDB") # ???
        else:
            self.driver = ogr.GetDriverByName("ESRI Shapefile")     
        
        self.dataset = self.driver.Open(self.filePath)
        self.layer = self.dataset.GetLayer() # **CHECK this might not work. may have to do this every time
        
        self.nFeatures = self.layer.GetFeatureCount()

        
    #--------------------------------------------------------------------------
    # applyNoDataMask()
    #--------------------------------------------------------------------------    
    def applyNoDataMask(self, mask):
        
        # Expecting mask to be 0s and 1s where we want to remove data
        
        """
        for feature in self.layer:
            get lat/lon
            ptGeom = Point(lon, lat)
            ptVal = point_query([ptGeom], mask)[0]

            if ptVal >= 0.99 or ptVal == None: # 0 = Data. 1 and None = NoData. some results might be float if within 2m of data. .99 cause some no data points were returning that
                # Point not under NoData should be removed
                layer.DeleteFeature(feature.GetFID())
                #continue # skip, don't include point              
        
        """
        

        return None # will be same shp


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
        
        """ 
        if below block doesnt work
        layer = self.dataset.GetLayer()
        srs = layer.GetSpatialRef()
        """

        srs = self.layer.GetSpatialRef()
        
        return srs.GetAuthorityCode(None)    
    
    #--------------------------------------------------------------------------
    # extent()
    #--------------------------------------------------------------------------
    def extent(self):
        
        """ 
        if below block doesnt work
        layer = self.dataset.GetLayer()
        (xmin, xmax, ymin, ymax) = layer.GetExtent()
        """
        
        (ulx, lrx, lry, uly) = self.layer.GetExtent()
        
        return (ulx, lry, lrx, uly)    

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
