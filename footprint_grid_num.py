#
#http://stackoverflow.com/questions/7861196/check-if-a-geopoint-with-latitude-and-longitude-is-within-a-shapefile
#http://geospatialpython.com/2010/12/dot-density-maps-with-python-and-ogr.html
import os, sys, osgeo, math, datetime, time
from osgeo import ogr, gdal, osr
import subprocess as subp
import fishnet
from os.path import basename
import argparse

gdal.AllRegister()
start_time = time.time()

#
# Call example: python footprint_grid_num.py /att/pubrepo/hma_data/ASTER HMA_AST_L1A_DSM_footprints_20170423.shp 0.1 hma_010_fishnet 66 106 25 50 FID
#

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
    parser = argparse.ArgumentParser(description="Create a grid with number of observations")
    parser.add_argument('root', default=None, help='Specify dir where input is located')
    parser.add_argument('in_fn', default=None, type=str, help='Input shapefile name')
    parser.add_argument('cell_size', type=float, default=0.25, help='Float indicating the cell size (degrees)')
    parser.add_argument('fishnet_name', default="fishnet", help='Output vector grid (fishnet) of given cell size')
    parser.add_argument('llon', default=66, type=int, help='The left longitude value (degrees)')
    parser.add_argument('rlon', default=106, type=int, help='The right longitude value (degrees)')
    parser.add_argument('blat', default=25, type=int, help='The bottom latitude value (degrees)')
    parser.add_argument('ulat', default=50, type=int, help='The upper latitude value (degrees)')
    parser.add_argument('UID_index', default='FID', type=str, help='A unique ID field from the input shapefile')
    return parser

def main():

    parser = getparser()
    args = parser.parse_args()

    root = args.root
    in_fn = args.in_fn
    cell_size = args.cell_size
    fishnet_name = args.fishnet_name

    os.chdir(root)

    #root = "/att/pubrepo/hma_data/ASTER"
    #in_fn = "HMA_AST_L1A_DSM_footprints_20170422" # BEFORE, then AFTER INTERSECT: coverage shp with fishnet at cell_size resolution
    #fishnet_name = "fishnet"
    outIntersect = in_fn.replace('.shp','_INTERSECT_'+args.UID_index)
    #cell_size = .25 #degrees

    print "\t[1] Create fishnet (i.e., vector grid), and reproject to a srs that matches that of the footprint shp"
    # Get EPSG of in_fn
    cmdStr = "gdalsrsinfo -o proj4 {}".format(os.path.join(root, in_fn))
    Cmd = subp.Popen(cmdStr, stdout=subp.PIPE, shell=True)
    proj4_str, err = Cmd.communicate()


    ##out_epsg = 3995
    fishnet_path = os.path.join(root,fishnet_name+'.shp')
    fishnet.main(fishnet_path,args.llon,args.rlon,args.blat,args.ulat, cell_size, cell_size)

    # reproject fishnet to match footprint prj
    fishnet_path_repro = fishnet_path.replace('.shp','_reprj.shp')
    cmdStr = "ogr2ogr {} {} -f 'ESRI Shapefile' -overwrite -t_srs {}".format(fishnet_path_repro,fishnet_path,proj4_str)

    Cmd = subp.Popen(cmdStr, stdout=subp.PIPE, shell=True)
    stdOut, err = Cmd.communicate()

    print "\t[2] Intersect 2 shps: fishnet and footprints"
    outIntersect_path = os.path.join(root,outIntersect+'.shp')

    try:
        if not os.path.isfile(outIntersect_path):

            intersect_start_time = time.time()
            ## https://gis.stackexchange.com/questions/119374/intersect-shapefiles-using-shapely
            ogr.UseExceptions()
            ogr_ds = ogr.Open(root, True)  # Windows: r'C:\path\to\data'
            SQL = """\
                SELECT ST_Intersection(A.geometry, B.geometry) AS geometry, A.*, B.*
                FROM {} A, {} B
                WHERE ST_Intersects(A.geometry, B.geometry);
            """.format(in_fn.split('.')[0], basename(fishnet_path_repro.strip(".shp")) )

            layer = ogr_ds.ExecuteSQL(SQL, dialect='SQLITE')
            # copy result back to datasource as a new shapefile
            layer2 = ogr_ds.CopyLayer(layer, outIntersect)
            # save, close
            layer = layer2 = ogr_ds = None

            intersect_end_time = time.time()
            duration = (intersect_end_time-intersect_start_time)/60
            print("\t\tElapsed INTERSECT time was %g minutes." % duration)

        else:
            print "Intersection file already exists: %s" %(outIntersect_path)

    except Exception,e:
        print "\n\t!!!--- Problem with the intersection: "
        print "\n\t", e

    print "\t[3] Project intersected shp to GEOG"
    sufx = '_geog'
    cmdStr = "ogr2ogr -f 'ESRI Shapefile' -t_srs EPSG:{} {} {} -overwrite".format(str(4326),outIntersect_path.replace('.shp',sufx+'.shp'), outIntersect_path)
    Cmd = subp.Popen(cmdStr, stdout=subp.PIPE, shell=True)
    s, e = Cmd.communicate()
    in_intersect = outIntersect+sufx+'.shp'

    #UID_index = "FID" # shp field name with UID
    #date_field = 'Year' # name of field with YYYY-MM-DD
    #viewing_angle_field = 'IncidAngle' # name of field in input shp with look angle
    proj = 4326 #http://spatialreference.org/ref/epsg/wgs-84/
    outCountField = "count" #new field for output coverage shp
    #outAngleField = 'max_view_angle'


    # #### POPULATE REFERENCE GRID CELL DICTIONARY WITH NTF FOOTPRINT INFO

    # In[5]:

    print "\t[4] POPULATE REFERENCE GRID CELL DICTIONARY WITH FOOTPRINT INFO"
    in_intersect_path = os.path.join(root,in_intersect)
    ##print in_intersect_path
    drv = ogr.GetDriverByName('ESRI Shapefile')
    ntfIn = drv.Open(in_intersect_path)
    ntfLyr = ntfIn.GetLayer(0)
    ntfGrid = makeGrid(cell_size)
    ntfDict = {}

    for ntf in ntfLyr: #each ntf is of type "feature"
        ID_index = ntf.GetFieldIndex(args.UID_index)
        ntfID = ntf.GetField(ID_index)

        ntfGeom = ntf.GetGeometryRef()
        ntfCentroid = ntfGeom.Centroid() #lon-lat
        ntfCentroidLon = float(str(ntfCentroid).split(' ')[1].strip('('))
        ntfCentroidLat = float(str(ntfCentroid).split(' ')[2].strip(')'))

        ntfDict[ntfID]=[ntfCentroidLat,ntfCentroidLon]#,ntfDate]
        #print "\tcent lat: %s, cent lon: %s" %(ntfCentroidLat,ntfCentroidLon)
        ntfCell = gridSort(ntfCentroidLat,ntfCentroidLon,cell_size)
        ntfGrid[ntfCell].append(ntfID)

    ntfCellCount=0
    for cell in ntfGrid:
        if ntfGrid[cell] != []: ntfCellCount+=1

    #reformat decimal degree for output if necessary
    cell_sizeStr = str(cell_size)
    if cell_sizeStr.split('.')>0:  # 0.25 degree cell size, for example
        cell_sizeStr.replace('.','-')

    print('\t\tRead '+ str(len(ntfDict))+' '+in_intersect+' poly features into '+str(ntfCellCount)+'/'+str(len(ntfGrid)) +' '+ cell_sizeStr+'-deg cells.\n')


    # #### CREATE GRID SHP BASED ON REFERENCE GRID CELLS

    # In[6]:

    print "\n\tCREATE GRID SHP BASED ON REFERENCE GRID CELLS"
    gridShp = os.path.join(root,in_fn.replace(".shp","-GRIDnum-"+args.UID_index+".shp")) #+cell_sizeStr+"deg_grid.shp"

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
    for cell in ntfGrid:
        sceneCount = len(ntfGrid[cell])
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

    ntfIn.Destroy()
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


    # In[7]:

    outGridShp = None
    del outGridShp


    # #### CONVERT SHP TO TIF

    # In[ ]:

    print "\tCONVERT SHP TO TIF"

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

    end_time = time.time()
    duration = (end_time-start_time)/60
    print("\tTotal elapsed time was %g minutes." % duration)

if __name__ == "__main__":
    ##import sys
    main()



