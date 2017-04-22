import os, sys
import ogr
import osr
from math import ceil
import subprocess as subp

##https://pcjericks.github.io/py-gdalogr-cookbook/vector_layers.html#create-fishnet-grid

def main(outputGridfn,xmin,xmax,ymin,ymax,gridHeight,gridWidth, out_epsg):

    # convert sys.argv to float
    xmin = float(xmin)
    xmax = float(xmax)
    ymin = float(ymin)
    ymax = float(ymax)
    gridWidth = float(gridWidth)
    gridHeight = float(gridHeight)

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
    if os.path.exists(outputGridfn):
        os.remove(outputGridfn)
    outDataSource = outDriver.CreateDataSource(outputGridfn)
    outLayer = outDataSource.CreateLayer(outputGridfn,geom_type=ogr.wkbPolygon )
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
    prj_file = open(outputGridfn.replace("shp","prj"), 'w')
    prj_file.write(spatialRef.ExportToWkt())
    prj_file.close()

    # reproject
    cmdStr = "ogr2ogr -f 'ESRI Shapefile' -t_srs EPSG:{} {} {} -overwrite".format(str(out_epsg),outputGridfn.replace('.shp','_'+str(out_epsg)+'.shp'), outputGridfn)
    Cmd = subp.Popen(cmdStr.rstrip('\n'), stdout=subp.PIPE, shell=True)
    stdOut, err = Cmd.communicate()


if __name__ == "__main__":

    #
    # example run : $ python grid.py <full-path><output-shapefile-name>.shp xmin xmax ymin ymax gridHeight gridWidth
    #
    num = 9
    if len( sys.argv ) != num:
        print "[ ERROR ] you must supply %s arguments: output-shapefile-name.shp xmin xmax ymin ymax gridHeight gridWidth " %(num-1)
        sys.exit( 1 )

    main( sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8] )