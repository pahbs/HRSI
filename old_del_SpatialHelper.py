# -*- coding: utf-8 -*-
"""
Created on Fri Apr  3 16:23:55 2020
@author: mwooten3

SpatialHelper.py

Functions associated with Gdal/OGR

Not the best organization, should definitely put these functions into classes
that actually make sense, but whatever for now

Code that uses SpatialHelper():
    - something i cant remember/multiple 3DSI zonal stats scripts
    - prepareTrainingData.py
    - others probably

"""

from osgeo import osr, ogr

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

    #--------------------------------------------------------------------------
    # determineUtmZone()
    # Determine the UTM (WGS84) zone for a polygon/point objects
    #--------------------------------------------------------------------------
    #def determineUtmZone(self):   
    
    #--------------------------------------------------------------------------
    # reprojectShape()
    # reprojects a OGR geometry object
    #--------------------------------------------------------------------------    
    def reprojectShape(self, shape, sEpsg, tEpsg):
    
        shapeType = shape.GetGeometryName()
        
        if shapeType == 'POINT':
            outShape = self.reprojectPoint(shape, sEpsg, tEpsg)
            
        elif shapeType == 'POLYGON':
            outShape = self.reprojectPolygon(shape, sEpsg, tEpsg)
            
        elif shapeType == 'MULTIPOLYGON':
            outShape = self.reprojectMultiPolygon(shape, sEpsg, tEpsg)
            
        else:
            raise RuntimeError("Input shape must be (MULTI)POLYGON or POINT")
            
        return outShape

    def reprojectPoint(self, point, sEpsg, tEpsg):
        
        # OGR point object --> projected OGR point object
        
        lon, lat = point.GetX(), point.GetY() 
        
        outLon, outLat = self.convertCoords((lon, lat), sEpsg, tEpsg)
        
        # Reconstruct OGR point geometry with projected coords:
        outPoint = ogr.Geometry(ogr.wkbPoint)
        outPoint.AddPoint(outLon, outLat)
        
        return outPoint
    
    def reprojectPolygon(self, polygon, sEpsg, tEpsg):
        
        # OGR polygon object --> projected OGR polygon object
    
        # Set up output polygon
        outRing = ogr.Geometry(ogr.wkbLinearRing)
        outPolygon = ogr.Geometry(ogr.wkbPolygon)
    
        # Iterate through points and reproject, then add to new polygon feature
        ring = polygon.GetGeometryRef(0)
        
        for pt in range(ring.GetPointCount()):
            
            lon, lat, z = ring.GetPoint(pt)
            
            # Construct OGR point to call reprojectPoint()
            point = ogr.Geometry(ogr.wkbPoint)
            point.AddPoint(lon, lat)
            
            outPoint = self.reprojectPoint(point, sEpsg, tEpsg)
            
            # Then add projected point to output polyon ring:
            outRing.AddPoint(outPoint.GetX(), outPoint.GetY())
            
        # Then add projected ring coords to output polygon    
        outPolygon.AddGeometry(outRing)
        
        return outPolygon
    
    def reprojectMultiPolygon(self, multiPolygon, sEpsg, tEpsg):
        
        # OGR multipolygon object --> projected OGR multipolygon object
    
        # Set up output multipolygon
        outMultiPolygon = ogr.Geometry(ogr.wkbMultiPolygon)
    
        # Iterate through polygons and reproject, then add to new multipolygon feature
        for part in range(multiPolygon.GetGeometryCount()):
            
            polygon = multiPolygon.GetGeometryRef(part)
            
            outPolygon = self.reprojectPolygon(polygon, sEpsg, tEpsg)
              
            outMultiPolygon.AddGeometry(outPolygon)
        
        return outMultiPolygon