# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 02:03:17 2020

@author: mwooten3
"""
import os

from osgeo import gdal, osr
#from osgeo.osr import CoordinateTransformation

#------------------------------------------------------------------------------
# class RasterStack
#------------------------------------------------------------------------------
class RasterStack(object):
    
    #--------------------------------------------------------------------------
    # __init__
    #--------------------------------------------------------------------------
    def __init__(self, filePath):
        
        # Probably doesn't make sense to keep this here but whatever
        self.zonalDir = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/ZonalStats/'
        
        # Check that the file is TIF or VRT
        extension = os.path.splitext(filePath)[1]       
        
        if extension != '.vrt' and extension != '.tif':
            raise RuntimeError('{} is not a VRT or TIF file'.format(filePath))

        self.filePath = filePath
        self.extension = extension
            
        self.dataset = gdal.Open(self.filePath, gdal.GA_ReadOnly)    
                  
        stackName = os.path.basename(self.filePath).strip(extension).strip('_stack')
        self.stackName = stackName

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
    
    #--------------------------------------------------------------------------
    # outDir()
    #--------------------------------------------------------------------------
    def outDir(self, stackType):
        
        if stackType not in ['LVIS', 'GLiHT', 'SGM']:
            print "Stack type must be LVIS, GLiHT, or SGM"
            return None
        
        # zonalStatsDir --> DSM/LVIS/GLiHT --> stackIdentifier
        outDir = os.path.join(self.zonalDir, stackType, self.stackName)
        
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
        
