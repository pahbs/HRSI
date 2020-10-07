# -*- coding: utf-8 -*-
"""
Created on Tue Mar 24 02:03:17 2020
@author: mwooten3

RasterStack describes a raster geoTIFF or VRT with mutliple layers
Inherits from Raster,
With methods designed specifically for 3DSI Zonal Stats process

RasterStack inherits the following from Raster:
    self.filePath; self.extension; self.baseName; self.baseDir; self.dataset
    self.noDataValue; self.ogrDataType; self.ogrGeotransform; self.ogrProjection
    self.nColumns; self.nRows; self.nLayers   
    
    convertExtent(self, targetEpsg)
    epsg(self)
    extent(self)
    extractBand(self, bandN, outTif = None)
    toArray(self) 
"""

import os

from Raster import Raster

#------------------------------------------------------------------------------
# class RasterStack
#------------------------------------------------------------------------------
class RasterStack(Raster):
    
    #--------------------------------------------------------------------------
    # __init__
    #--------------------------------------------------------------------------
    def __init__(self, filePath):
        
        # Initialize the base class
        super(RasterStack, self).__init__(filePath)
        
        """
        # Check that the file is TIF or VRT            
        if self.extension != '.vrt' and self.extension != '.tif':
            raise RuntimeError('{} is not a VRT or TIF file'.format(filePath))
        """
        
        self.stackName = self.baseName.strip('_stack')     

    #--------------------------------------------------------------------------
    # noDataLayer()
    #--------------------------------------------------------------------------
    def noDataLayer(self):
        
        noDataLayer = self.filePath.replace('stack.vrt', 'mask.tif')
        
        if os.path.isfile(noDataLayer):
            return noDataLayer
        
        else:
            return None
        
    #--------------------------------------------------------------------------
    # outDir()
    #--------------------------------------------------------------------------
    def outDir(self, baseDir):

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
    # stackType()
    #--------------------------------------------------------------------------
    def stackType(self):
        
        # SGM, LVIS, or GLiHT
        
        if 'Out_SGM' in self.baseDir:
            return 'SGM'
        
        elif 'out_lvis' in self.baseDir:
            return 'LVIS'
        
        elif 'out_gliht' in self.baseDir:
            return 'GLiHT'
        
        elif 'esta_year' in self.baseDir:
            return 'Landsat'
        
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
        
        