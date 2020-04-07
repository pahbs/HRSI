# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 02:03:17 2020
@author: mwooten3

RasterStack describes a raster geoTIFF or VRT with mutliple layers
With methods designed specifically for Zonal Stats process

EVENTUALLY: Build a general FeatureClass class and RasterStack can inherit from it
"""

import os

from osgeo import gdal, osr
#from osgeo.osr import CoordinateTransformation

from SpatialHelper import SpatialHelper

#------------------------------------------------------------------------------
# class RasterStack
#------------------------------------------------------------------------------
class RasterStack(object):
    
    #--------------------------------------------------------------------------
    # __init__
    #--------------------------------------------------------------------------
    def __init__(self, filePath):
        
        # Probably doesn't make sense to keep this here but whatever
        #* MIGHT SHOULD BE IN ZonalStats.py and combined with 'stackType' then
        #   sent to self.outDir below like
        #   def outDir(self, baseDir): outDir = join(baseDir, self.stackName)
        # stackType could be a method in here as well
        #self.zonalDir = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/ZonalStats/'
        
        # Check that the file is TIF or VRT
        extension = os.path.splitext(filePath)[1]       
        
        if extension != '.vrt' and extension != '.tif':
            raise RuntimeError('{} is not a VRT or TIF file'.format(filePath))

        self.filePath = filePath
        self.extension = extension
         
        stackName = os.path.basename(self.filePath).strip(extension).strip('_stack')
        self.stackName = stackName
        
        self.inDir = os.path.dirname(self.filePath)
        
        self.dataset = gdal.Open(self.filePath, gdal.GA_ReadOnly)          

    """
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
    """

    #--------------------------------------------------------------------------
    # convertExtent()
    #--------------------------------------------------------------------------
    def convertExtent(self, targetEpsg):
        
        (ulx, lry, lrx, uly) = self.extent()

        ulxOut, ulyOut = SpatialHelper().convertCoords((ulx, uly), self.epsg(), targetEpsg)
        lrxOut, lryOut = SpatialHelper().convertCoords((lrx, lry), self.epsg(), targetEpsg)
    
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

    #--------------------------------------------------------------------------
    # noDataLayer()
    #--------------------------------------------------------------------------
    def noDataLayer(self):
        
        noDataLayer = self.filePath.replace('stack.vrt', 'mask_proj.tif')
        
        if os.path.isfile(noDataLayer):
            return noDataLayer
        
        else:
            return None
        
    #--------------------------------------------------------------------------
    # outDir()
    #--------------------------------------------------------------------------
    def outDir(self, baseDir):
        
#        if stackType not in ['LVIS', 'GLiHT', 'SGM']:
#            print "Stack type must be LVIS, GLiHT, or SGM"
#            return None
        
        # zonalStatsDir --> zonalType --> DSM/LVIS/GLiHT --> stackIdentifier
        outDir = os.path.join(baseDir, self.stackType(), self.stackName)
        
        os.system('mkdir -p {}'.format(outDir))
        
        return outDir
        
    #--------------------------------------------------------------------------
    # stackKey()
    #--------------------------------------------------------------------------
    def stackKey(self):
        
        stackKey = self.filePath.replace('.vrt', '_Log.txt')
        
        if os.path.isfile(stackKey):
            return stackKey
        
        else:
            return None
        
    #--------------------------------------------------------------------------
    # stackType() # SGM, LVIS, or GLiHT
    #--------------------------------------------------------------------------
    def stackType(self):
        
        if 'Out_SGM' in self.inDir:
            return 'SGM'
        
        elif 'out_lvis' in self.inDir:
            return 'LVIS'
        
        elif 'out_gliht' in self.inDir:
            return 'GLiHT'
        
        else:
            return None
        
    #--------------------------------------------------------------------------
    # xmlLayer()
    #--------------------------------------------------------------------------
    def xmlLayer(self):
        
        xmlLayer = self.filePath.replace('_stack.vrt', '.xml')
        
        if os.path.isfile(xmlLayer):
            return xmlLayer
        
        else:
            return None    
        
        
        