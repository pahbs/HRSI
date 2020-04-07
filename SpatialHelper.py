# -*- coding: utf-8 -*-
"""
Created on Fri Apr  3 16:23:55 2020
@author: mwooten3

SpatialHelper.py

Functions associated with Gdal/OGR
"""

from osgeo import osr

# NOTES (for GDAL classes later):
# from rasterstats import point_query, zonal_stats
# from shapely.geometry import Point, Polygon
# for feature in layer:
#   lon = feature.GetGeometryRef().Centroid().GetX()
#   lat = feature.GetGeometryRef().Centroid().GetY()
#   ptGeom = Point(lon, lat)
#   ptVal = point_query([ptGeom], mask)[0]

# import time
# start = time.time()
# round((time.time()-start)/60, 4)
#------------------------------------------------------------------------------
# class SpatialHelper
#------------------------------------------------------------------------------
class SpatialHelper(object):
    
    #--------------------------------------------------------------------------
    # __init__
    #--------------------------------------------------------------------------
    
    # no init for now
    
     #--------------------------------------------------------------------------
    # convertCoords()
    #--------------------------------------------------------------------------
    def convertCoords(self, coords, sourceEpsg, targetEpsg):
        
        # Coords expected = (lon, lat)
        (x, y) = coords
                
        sourceSrs = osr.SpatialReference()
        sourceSrs.ImportFromEPSG(int(sourceEpsg)) 
    
        targetSrs = osr.SpatialReference()
        targetSrs.ImportFromEPSG(int(targetEpsg))
    
        coordTrans = osr.CoordinateTransformation(sourceSrs, targetSrs)
        xOut, yOut = coordTrans.TransformPoint(x, y)[0:2]
    
        return (xOut, yOut)   