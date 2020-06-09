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

from osgeo import ogr, osr

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

        bname = self.baseName
        
        if 'ATL08' in bname:
            self.zonalName = 'ATL08'
        elif 'GLAS' in bname:
            self.zonalName = 'GLAS'
        else: 
            self.zonalName = None
        
    #--------------------------------------------------------------------------
    # applyNoDataMask()
    #--------------------------------------------------------------------------    
    def applyNoDataMask(self, mask, transEpsg = None, outShp = None):
        
        # Expecting mask to be 0 and 1, with 1 where we want to remove data
        # This is specific to 3DSI and therefore is not kept in FeatureClass 
        
        # if transformEpsg is supplied, convert points to correct SRS before running ZS
        # if not supplied, will assume projection of mask and ZFC are the same
        
        # Get name output shp: 
        if not outShp:
            outShp = self.filePath.replace(self.extension, '__filtered-ND.shp')
        
        drv = ogr.GetDriverByName("ESRI Shapefile")
        ds = drv.Open(self.filePath)
        layer = ds.GetLayer()
     
        # This will work even if not needed. If needed and not supplied, could fail
        outSrs = osr.SpatialReference()
        if transEpsg:
            outSrs.ImportFromEPSG(int(transEpsg))
        else:
            outSrs.ImportFromEPSG(int(self.epsg())) # If transformation EPSG not supplied, keep coords as is
        
        # Collect list of FIDs to keep
        keepFIDs = []
        for feature in layer:

            # OPTION A
            # Get polygon geometry and transform to outSrs just in case
            geom = feature.GetGeometryRef()
            geom.TransformTo(outSrs)

            # Then export to WKT for ZS             
            wktPoly = geom.ExportToIsoWkt()

            # Get info from mask underneath feature
            z = zonal_stats(wktPoly, mask, stats="mean")
            out = z[0]['mean']            
            if out >= 0.6 or out == None: # If 60% of pixels or more are NoData, skip
                continue
            
            # Else, add FID to list to keep
            keepFIDs.append(feature.GetFID())

        if len(keepFIDs) == 0: # If there are no points remaining, return None
            return None
        
        if len(keepFIDs) == 1: # tuple(listWithOneItem) wont work in Set Filter
            query = "FID = {}".format(keepFIDs[0])
            
        #if len(keepFIDs) > maxQueryFeat: # If too many features, split query
            #query = self.iterQuery(keepFIDs, maxQueryFeat)
                    
        else: # If we have more than 1 item, call getFidQuery
            #query = "FID IN {}".format(tuple(keepFIDs))
            import pdb; pdb.set_trace()
            query = self.getFidQuery(keepFIDs)
            
            

        """ In the event that there are too many features to Set Filter with, 
        run pair twice with this following block uncommented, and manually 
        setting keepFIDs to A or B depending on which iteration you're on:
        halfway = len(keepFIDs)/2 # if len is odd, 1st list will have 1 more item
        #keepFIDs = keepFIDs[:halfway] # On first iteration
        keepFIDs = keepFIDs[halfway:] # On second   
        query = "FID IN {}".format(tuple(keepFIDs)) # redo query
        """
        
        # Filter and write the features we want to keep to new output DS:
        ## Pass ID's to a SQL query as a tuple, i.e. "(1, 2, 3, ...)"
        layer.SetAttributeFilter(query)

        dsOut = drv.CreateDataSource(outShp)
        layerOutName = os.path.basename(outShp).replace('.shp', '')
        layerOut = dsOut.CopyLayer(layer, layerOutName)
        
        if not layerOut: # If CopyLayer failed for whatever reason
            print "Could not remove NoData polygons"
            return self.filePath
        
        ds = layer = dsOut = layerOut = feature = None
        
        return outShp

    #--------------------------------------------------------------------------
    # getFidQuery()
    #  Get the SQL query from a list of FIDs. For large FID sets,
    #  return a query that avoids SQL error from "FID IN (<largeTuple>)"
    #  List of FIDs will be split into chunks separated by OR
    #-------------------------------------------------------------------------- 
    def getFidQuery(self, FIDs, maxFeatures = 4800):
        
        nFID = len(FIDs)
        print "nFID", nFID
        if nFID > maxFeatures: # Then we must combine multiple smaller queries
            
            import math
            nIter = int(math.ceil(nFID/float(maxFeatures)))

            query = 'FID IN'
            
            a = 0
            b = maxFeatures # initial bounds for first iter (0, maxFeat)
            
            for i in range(nIter):
                
                if i == nIter-1: # if in the last iteration
                    b = nFID
                    
                queryFIDs = FIDs[a:b]
                query += ' {} OR FID IN'.format(tuple(queryFIDs))
                
                a += maxFeatures # Get bounds for next iteration
                b += maxFeatures
                
            query = query.rstrip(' OR FID IN') 
            
        else:
            query = "FID IN {}".format(tuple(FIDs))    
    
        return query

    #--------------------------------------------------------------------------
    # filterAttributes()
    #--------------------------------------------------------------------------    
    def filterAttributes(self, filterStr, outShp = None):
        
        ogr.UseExceptions() # To catch possible error with filtering
        
        # Get name output shp: 
        if not outShp:
            outShp = self.filePath.replace(self.extension, '__filtered.shp')        
        
        # Get layer and filter the attributes
        drv = ogr.GetDriverByName("ESRI Shapefile")
        ds = drv.Open(self.filePath)
        layer = ds.GetLayer()
        
        try:
            layer.SetAttributeFilter(filterStr)
        except RuntimeError as e:
            print 'Could not filter based on string "{}": {}'.format(filterStr, e)
            return self.filePath
        
        # Copy filtered layer to output and save
        drv = ogr.GetDriverByName("ESRI Shapefile")        
        dsOut = drv.CreateDataSource(outShp)
        layerOutName = os.path.basename(outShp).replace('.shp', '')
        layerOut = dsOut.CopyLayer(layer, layerOutName)

        if not layerOut: # If CopyLayer failed for whatever reason
            print 'Could not filter based on string "{}"'.format(filterStr)
            return self.filePath
        
        return outShp
        
        