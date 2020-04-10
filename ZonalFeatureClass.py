#!/usr/bin/env python
"""
Created on Tue Mar 24 02:03:17 2020
@author: mwooten3

ZonalFeatureClass describes a polygon .shp or .gdb for 3DSI ZonalStats
Inherits from FeatureClass,
with additional methods designed specifically for Zonal Stats process

ZonalFeatureClass inherits the following from FeatureClass:
    self.filePath; self.extension; self.baseName; self.baseDir; self.driver;
    self.dataset; self.layer; self.layerDefn; self.nFeatures; self.nFields

    clipToExtent(self, clipExtent, extentEpsg, outClip = None)
    createCopy(self, copyName)
    epsg(self)
    extent(self)
    fieldNames(self)
"""

import os

from osgeo import ogr

from rasterstats import zonal_stats

from FeatureClass import FeatureClass

#------------------------------------------------------------------------------
# class ZonalFeatureClass
#------------------------------------------------------------------------------
class ZonalFeatureClass(FeatureClass):
    
    #--------------------------------------------------------------------------
    # __init__
    #--------------------------------------------------------------------------
    def __init__(self, filePath):

        # Initialize the base class
        super(ZonalFeatureClass, self).__init__(filePath)

        self.zonalName = self.baseName
        
    #--------------------------------------------------------------------------
    # applyNoDataMask()
    #--------------------------------------------------------------------------    
    def applyNoDataMask(self, mask, outShp = None):

        # Expecting mask to be 0 and 1, with 1 where we want to remove data
        # This is specific to 3DSI and therefore is not kept in FeatureClass
        
        # Get name output shp: 
        if not outShp:
            outShp = self.filePath.replace(self.extension, '__filtered-ND.shp')
        
        drv = ogr.GetDriverByName("ESRI Shapefile")
        ds = drv.Open(self.filePath)
        layer = ds.GetLayer()
        import pdb; pdb.set_trace()
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
