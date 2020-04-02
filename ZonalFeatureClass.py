# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 02:03:17 2020
@author: mwooten3

ZonalFeatureClass describes a polygon .shp or .gdb
With methods designed specifically for Zonal Stats process

EVENTUALLY: Build a general Raster class and ZFC can inherit from it
"""
import os

from osgeo import ogr, osr
#from osgeo.osr import CoordinateTransformation

import tempfile

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
        
        if self.extension == '.gdb':
            self.driver = ogr.GetDriverByName("FileGdb") # ???
        else:
            self.driver = ogr.GetDriverByName("ESRI Shapefile")     
        
        self.dataset = self.driver.Open(self.filePath)
        self.layer = self.dataset.GetLayer() # **CHECK this might not work. may have to do this every time

        self.nFeatures = self.layer.GetFeatureCount()
        
    #--------------------------------------------------------------------------
    # clipToExtent() **CHECK**
    #--------------------------------------------------------------------------    
    def clipToExtent(self, clipExtent):
        
        # Expect extent to be tuple = (xmin, ymin, xmax, ymax)
        extent = ' '.join(map(str,clipExtent))
        
        clipFile = '{}.shp'.format(tempfile.mkdtemp())
        
    
        cmd = 'ogr2ogr -clipsrc {} -spat {} -f '.format(extent, extent) + \
                        '"ESRI Shapefile" {} {}'.format(clipFile, self.fileName)
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
        
        return clipFile
 
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
