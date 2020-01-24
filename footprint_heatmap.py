#!/usr/bin/env python

import os, sys, osgeo, math, datetime, time
from osgeo import ogr, gdal, osr
import subprocess as subp
import fishnet
from os.path import basename
import argparse

gdal.AllRegister()
start_time = time.time()

# Create a heatmap grid with number of observations representing overlapping data from an input footprint shapefile
# Call example: python footprint_grid_num.py /att/pubrepo/hma_data/ASTER HMA_AST_L1A_DSM_footprints_20170423.shp 0.1 66 106 25 50
#
# http://stackoverflow.com/questions/7861196/check-if-a-geopoint-with-latitude-and-longitude-is-within-a-shapefile
# http://geospatialpython.com/2010/12/dot-density-maps-with-python-and-ogr.html


def makeGrid(cell_size):
    worldGrid = {}
    for i in range(0,int(180/cell_size)):
        for j in range(0,int(360/cell_size)):
            gridCell = str(int((90/cell_size))-i)+'_'+str(j)
            worldGrid[gridCell] = [] # list to be populated by NTF polys in grid cell
    return worldGrid

# takes in lat,lon and outputs grid cell ID
def gridSort(lat,lon,cell_size):
    latCell = str(int(math.ceil(float(lat)/cell_size)))
    if lon < 0: lon = 360+lon
    lonCell = str(int(math.floor(float(lon)/cell_size)))
    if int(lonCell)*cell_size == 360: lonCell = '0'
    gridCell = latCell+'_'+lonCell #refers to UL corner of grid cell
    return gridCell

def getparser():
    parser = argparse.ArgumentParser(description="Create a heatmap grid with number of observations representing overlapping data from an input footprint shapefile")
    parser.add_argument('in_fn', default=None, type=str, help='Input footprint shapefile name')
    parser.add_argument('cell_size', type=float, default=0.25, help='Float indicating the cell size (degrees)')
    parser.add_argument('llon', default=None, type=int, help='The left longitude value (degrees)')
    parser.add_argument('rlon', default=None, type=int, help='The right longitude value (degrees)')
    parser.add_argument('blat', default=None, type=int, help='The bottom latitude value (degrees)')
    parser.add_argument('ulat', default=None, type=int, help='The upper latitude value (degrees)')
    parser.add_argument('-UID_index', default='FID', type=str, help='(Default: FID) A unique ID field from the reprojected version of the input shapefile')
    parser.add_argument('-out_fishnet_fn', default="tmp_fishnet.shp", help='Output vector grid (fishnet) of given cell size')
    return parser

def main():

    parser = getparser()
    args = parser.parse_args()

    in_fn = args.in_fn
    cell_size = args.cell_size
    fishnet_fn = args.out_fishnet_fn

    root = os.path.split(in_fn)[0]
    os.chdir(root)

    if args.llon is None:
        sys.exit("Enter in correct geographic bounds for heatmap: llon rlon blat ulat")

    outIntersect = in_fn.replace('.shp','_INTERSECT_'+args.UID_index)

    print "\t[1] CREATE: fishnet (i.e., vector grid), and reproject to a srs that matches that of the footprint shp"
    create_start_time = time.time()

    # Get EPSG of in_fn
    cmdStr = "gdalsrsinfo -o proj4 {}".format(in_fn)
    Cmd = subp.Popen(cmdStr, stdout=subp.PIPE, shell=True)
    proj4_str, err = Cmd.communicate()

    fishnet_path, fishnet_name = os.path.split(fishnet_fn)

    os.system("fishnet.py {} {} {} {} {} {} {}".format(fishnet_fn, args.llon, args.rlon, args.blat, args.ulat, cell_size, cell_size) )

    # reproject fishnet to match footprint prj
    fishnet_fn_repro = fishnet_fn.replace('.shp','_reprj.shp')
    cmdStr = "ogr2ogr {} {} -f 'ESRI Shapefile' -overwrite -t_srs {}".format(fishnet_fn_repro,fishnet_fn,proj4_str)

    Cmd = subp.Popen(cmdStr, stdout=subp.PIPE, shell=True)
    stdOut, err = Cmd.communicate()
    create_end_time = time.time()
    duration = (create_end_time-create_start_time)/60
    print("\t\tElapsed CREATE time was %g minutes." % duration)


    print "\t[2] INTERSECT: 2 shps; fishnet and footprints"

    outIntersect_fn = outIntersect+'.shp'

    try:
        if not os.path.isfile(outIntersect_fn):

            intersect_start_time = time.time()
            ## https://gis.stackexchange.com/questions/119374/intersect-shapefiles-using-shapely
            ogr.UseExceptions()
            ogr_ds = ogr.Open(root, True)  # Windows: r'C:\path\to\data'
            SQL = """\
                SELECT ST_Intersection(A.geometry, B.geometry) AS geometry, A.*, B.*
                FROM {} A, {} B
                WHERE ST_Intersects(A.geometry, B.geometry);
            """.format(basename(in_fn).split('.')[0], basename(fishnet_fn_repro).split('.')[0] )

            layer = ogr_ds.ExecuteSQL(SQL, dialect='SQLITE')
            # copy result back to datasource as a new shapefile
            layer2 = ogr_ds.CopyLayer(layer, basename(outIntersect))
            # save, close
            layer = layer2 = ogr_ds = None

            intersect_end_time = time.time()
            duration = (intersect_end_time-intersect_start_time)/60
            print("\t\tElapsed INTERSECT time was %g minutes." % duration)

        else:
            print "Intersection file already exists: %s" %(outIntersect_fn)

    except Exception,e:
        print "\n\t!!!--- Problem with the intersection: "
        print "\n\t", e

    print "\t[3] PROJECT: intersected shp to GEOG"
    proj_start_time = time.time()
    sufx = '_geog'
    cmdStr = "ogr2ogr -f 'ESRI Shapefile' -t_srs EPSG:{} {} {} -overwrite".format(str(4326),outIntersect_fn.replace('.shp',sufx+'.shp'), outIntersect_fn)
    Cmd = subp.Popen(cmdStr, stdout=subp.PIPE, shell=True)
    s, e = Cmd.communicate()
    in_intersect_fn = outIntersect+sufx+'.shp'

    proj = 4326 #http://spatialreference.org/ref/epsg/wgs-84/
    outCountField = "count" #new field for output coverage shp
    proj_end_time = time.time()
    duration = (proj_end_time-proj_start_time)/60
    print("\t\tElapsed PROJECT time was %g minutes." % duration)

    print "\t[4] POPULATE: reference grid cell dictionary with footprint info"
    pop_start_time = time.time()

    drv = ogr.GetDriverByName('ESRI Shapefile')
    shp_open = drv.Open(in_intersect_fn)
    lyr = shp_open.GetLayer(0)
    featGrid = makeGrid(cell_size)
    featDict = {}

    for feat in lyr: #each feat is of type "feature"
        #print "\tUID_index: %s" %(args.UID_index)
        ID_index = feat.GetFieldIndex(args.UID_index)
        featID = feat.GetField(ID_index)

        featGeom = feat.GetGeometryRef()
        centroid = featGeom.Centroid() #lon-lat
        centroidLon = float(str(centroid).split(' ')[1].strip('('))
        centroidLat = float(str(centroid).split(' ')[2].strip(')'))

        featDict[featID]=[centroidLat,centroidLon]#,ntfDate]
        #print "\tcent lat: %s, cent lon: %s" %(centroidLat,centroidLon)
        featCell = gridSort(centroidLat,centroidLon,cell_size)
        featGrid[featCell].append(featID)

    featCellCount=0
    for cell in featGrid:
        if featGrid[cell] != []: featCellCount+=1

    #reformat decimal degree for output if necessary
    cell_sizeStr = str(cell_size)
    if cell_sizeStr.split('.')>0:  # 0.25 degree cell size, for example
        cell_sizeStr.replace('.','-')

    print('\t\tRead '+ str(len(featDict))+' '+in_intersect_fn+' poly features into '+str(featCellCount)+'/'+str(len(featGrid)) +' '+ cell_sizeStr+'-deg cells.\n')
    pop_end_time = time.time()
    duration = (pop_end_time-pop_start_time)/60
    print("\t\tElapsed POPULATE time was %g minutes." % duration)

    # #### CREATE GRID SHP BASED ON REFERENCE GRID CELLS

    print "\t[5] GRID: shp based on vector grid cells"
    grid_start_time = time.time()
    gridShp = os.path.join(root,in_fn.replace(".shp","_heatmap_"+cell_sizeStr.replace(".","_")+"deg.shp"))

    if os.path.exists(gridShp): os.remove(gridShp)

    outGridShp = drv.CreateDataSource(gridShp)
    gridLayer = outGridShp.CreateLayer(cell_sizeStr+"_deg", geom_type=ogr.wkbPolygon)
    #gridIDField = ogr.FieldDefn("ntfIDField", ogr.OFTString)
    gridCountField = ogr.FieldDefn(outCountField, ogr.OFTInteger)
    #gridAngleField = ogr.FieldDefn(outAngleField, ogr.OFTDouble)
    #gridLayer.CreateField(gridIDField)
    gridLayer.CreateField(gridCountField)
    #gridLayer.CreateField(gridAngleField)

    #bring in cell boundary geometry and NTF footprint data
    #http://pcjericks.github.io/py-gdalogr-cookbook/geometry.html#create-a-polygon
    #http://pcjericks.github.io/py-gdalogr-cookbook/layers.html
    for cell in featGrid:
        sceneCount = len(featGrid[cell])
        if sceneCount > 0: #only make shp cells where at least one scene
            ring = ogr.Geometry(ogr.wkbLinearRing)
            cell_UL_lat = float(cell.split('_')[0])*cell_size
            cell_UL_lon = float(cell.split('_')[1])*cell_size
            if cell_UL_lon > 180.: cell_UL_lon = cell_UL_lon - 360
            ring.AddPoint(cell_UL_lon,cell_UL_lat)
            ring.AddPoint(cell_UL_lon,cell_UL_lat-cell_size)
            ring.AddPoint(cell_UL_lon+cell_size,cell_UL_lat-cell_size)
            ring.AddPoint(cell_UL_lon+cell_size,cell_UL_lat)
            ring.AddPoint(cell_UL_lon,cell_UL_lat)

            poly = ogr.Geometry(ogr.wkbPolygon)
            poly.AddGeometry(ring)

            featureDefn = gridLayer.GetLayerDefn()
            feature = ogr.Feature(featureDefn)
            feature.SetGeometry(poly)
            #feature.SetField("ID", cell)
            feature.SetField(outCountField,sceneCount)
            gridLayer.CreateFeature(feature)

    shp_open.Destroy()
    outGridShp.Destroy()

    # make prj
    # https://pcjericks.github.io/py-gdalogr-cookbook/projection.html
    spatialRef = osr.SpatialReference()
    spatialRef.ImportFromEPSG(proj)
    spatialRefTIF = spatialRef
    spatialRef.MorphToESRI()
    ##prj = open(gridShp.replace("shp","prj"),'w')
    ##prj.write(spatialRef.ExportToWkt())
    ##prj.close()

    prj_name =gridShp.replace("shp","prj")
    prj = open(prj_name, "w")
    epsg = 'GEOGCS["WGS 84",'
    epsg += 'DATUM["WGS_1984",'
    epsg += 'SPHEROID["WGS 84",6378137,298.257223563]]'
    epsg += ',PRIMEM["Greenwich",0],'
    epsg += 'UNIT["degree",0.0174532925199433]]'
    prj.write(epsg)
    prj.close()

    grid_end_time = time.time()
    duration = (grid_end_time-grid_start_time)/60
    print("\t\tElapsed GRID time was %g minutes." % duration)

    outGridShp = None
    del outGridShp


    # #### CONVERT SHP TO TIF

    print "\t[6] CONVERT: shp to GeoTiff"
    convert_start_time = time.time()

    #taken from shp-convert_shp_to_raster.py
    source_ds = ogr.Open(gridShp)
    source_layer = source_ds.GetLayer(0)
    source_srs = source_layer.GetSpatialRef()

    x_min, x_max, y_min, y_max = source_layer.GetExtent()
    col = int((x_max - x_min) / cell_size)
    row = int((y_max - y_min) / cell_size)

    #set up spatial referencing of TIFs
    coverage_ds = gdal.GetDriverByName('GTiff').Create(gridShp.replace("shp","tif"), col,
            row, 1, gdal.GDT_UInt16)
    coverage_ds.SetGeoTransform((x_min, cell_size, 0, y_max, 0, -cell_size,))
    coverage_ds.SetProjection(spatialRefTIF.ExportToWkt())
    if source_srs:
        # make the target raster have the same projection as the source
        coverage_ds.SetProjection(source_srs.ExportToWkt())
    else:
        # source has no projection (needs GDAL >= 1.7.0 to work)
        coverage_ds.SetProjection('LOCAL_CS["arbitrary"]')

    coverage_band = coverage_ds.GetRasterBand(1)
    coverage_band.SetNoDataValue(0) #noData value is established for one and all bands

    # make rasters based on shp fields
    err = gdal.RasterizeLayer(coverage_ds, [1,1,1], source_layer,
            #burn_values=(0,0,0), #need to update these for proper background values
            options=["ATTRIBUTE=%s" % outCountField])
    if err != 0: raise Exception("error rasterizing layer: %s" % err)

    coverage_ds = None

    convert_end_time = time.time()
    duration = (convert_end_time-convert_start_time)/60
    print("\t\tElapsed CONVERT time was %g minutes." % duration)

    end_time = time.time()
    duration = (end_time-start_time)/60
    print("\t\tTotal elapsed time was %g minutes." % duration)

if __name__ == "__main__":
    ##import sys
    main()



