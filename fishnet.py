#!/usr/bin/env python
import sys
import os
import argparse

import ogr
import osr
from math import ceil
import subprocess as subp

##https://pcjericks.github.io/py-gdalogr-cookbook/vector_layers.html#create-fishnet-grid

def getparser():
    parser = argparse.ArgumentParser(description='Create a vecotr grid within a lat/lon bounding box')
    parser.add_argument('out_fn', type=str, help='Output vector grid name)')
    parser.add_argument('xmin', type=float, default=None, help='xmin')
    parser.add_argument('xmax', type=float, default=None, help='xmax')
    parser.add_argument('ymin', type=float, default=None, help='ymin')
    parser.add_argument('ymax', type=float, default=None, help='ymax')
    parser.add_argument('gridWidth', type=float, default=1, help='Width of vector grid cell')
    parser.add_argument('gridHeight', type=float, default=1, help='Height of vector grid cell')
    return parser

def main():


    parser = getparser()
    args = parser.parse_args()

    out_fn = args.out_fn

    # convert sys.argv to float
    xmin = args.xmin
    xmax = args.xmax
    ymin = args.ymin
    ymax = args.ymax
    gridWidth = args.gridWidth
    gridHeight = args.gridHeight

    # get rows
    rows = ceil((ymax-ymin)/gridHeight)
    # get columns
    cols = ceil((xmax-xmin)/gridWidth)

    # start grid cell envelope
    ringXleftOrigin = xmin
    ringXrightOrigin = xmin + gridWidth
    ringYtopOrigin = ymax
    ringYbottomOrigin = ymax-gridHeight

    # create output file
    outDriver = ogr.GetDriverByName('ESRI Shapefile')
    if os.path.exists(out_fn):
        os.remove(out_fn)
    outDataSource = outDriver.CreateDataSource(out_fn)
    outLayer = outDataSource.CreateLayer(out_fn,geom_type=ogr.wkbPolygon )
    featureDefn = outLayer.GetLayerDefn()

    # create grid cells
    countcols = 0
    while countcols < cols:
        countcols += 1

        # reset envelope for rows
        ringYtop = ringYtopOrigin
        ringYbottom =ringYbottomOrigin
        countrows = 0

        while countrows < rows:
            countrows += 1
            ring = ogr.Geometry(ogr.wkbLinearRing)
            ring.AddPoint(ringXleftOrigin, ringYtop)
            ring.AddPoint(ringXrightOrigin, ringYtop)
            ring.AddPoint(ringXrightOrigin, ringYbottom)
            ring.AddPoint(ringXleftOrigin, ringYbottom)
            ring.AddPoint(ringXleftOrigin, ringYtop)
            poly = ogr.Geometry(ogr.wkbPolygon)
            poly.AddGeometry(ring)

            # add new geom to layer
            outFeature = ogr.Feature(featureDefn)
            outFeature.SetGeometry(poly)
            outLayer.CreateFeature(outFeature)
            outFeature = None

            # new envelope for next poly
            ringYtop = ringYtop - gridHeight
            ringYbottom = ringYbottom - gridHeight

        # new envelope for next poly
        ringXleftOrigin = ringXleftOrigin + gridWidth
        ringXrightOrigin = ringXrightOrigin + gridWidth

    # Save and close DataSources
    outDataSource = None

    # make prj
    # https://pcjericks.github.io/py-gdalogr-cookbook/projection.html
    spatialRef = osr.SpatialReference()
    spatialRef.ImportFromEPSG(4326)
    spatialRef.MorphToESRI()
    prj_file = open(out_fn.replace("shp","prj"), 'w')
    prj_file.write(spatialRef.ExportToWkt())
    prj_file.close()


if __name__ == '__main__':
    main()