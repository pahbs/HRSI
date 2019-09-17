# python file to hold functions and dictionaries for:
# lidarHeight_disturbance_database.py
#import lidarHeight_disturbance_database as main
#GLAS_id_field = main.GLAS_id_field
import os
from osgeo import gdal,ogr,osr
import xml.etree.ElementTree as ET

class Parameters():
    def __init__(self):
        pass

    # Default inputs (latter 2 can be changed by CL args when running run_GLAS_zonal_database.py):
    ddir = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal'
    # METRICS
    #GLAS_csv_dir = '/att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/data/glas/tiles_5deg/n00_n70_csv'  # old location of v1 CSVs
    #GLAS_csv_dir = '/att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/data/glas/circ_boreal/N50_5dtiles/metrics_csv' # old location of v2 CSVs
#    GLAS_csv_dir = '/att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/data/glas/misc/tiles_5deg_old/csv_files' # current location of v1
    GLAS_csv_dir = '/att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/data/glas/circ_boreal' # current location of v2
    default_buffSize = 15
    default_outCsvDir = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal/zonal_outputs' # default directory for output csv's
    default_shpDir = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal/zonal_data' # where created shapefiles will go unless otherwise specified
    default_logdir = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal/logs'
    default_zstats = "median mean std nmad min max"

    default_mainDatabase = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/3DSI_GLAS_stats_database'#__{}'.format(datetime.datetime.now().strftime("%Y%m%d")) # will end in either __15m.csv or __15m.shp (depending on buffer size)

    majority_basenames = ['C2C_change_year_type_warp', 'C2C_change_type_type_warp', 'AK_NWCanada_Fire1965_2013_ras_type_warp',
                            'MCD12Q1_A2017001_LC_Type1_type_warp', 'boreal_clust_30_30_2019_9_16_warp', 'NPCA_01_091219_warp', 'NPCA_02_091219_warp', 'NPCA_03_091219_warp']

def getWKT_PRJ(epsg_code): # generate a .prj file based off epsg from input
    # as of 4.16/2019, spatialreference.org is down. Use GDAL API instead

    #wkt = urllib.urlopen("http://spatialreference.org/ref/epsg/{0}/prettywkt/".format(epsg_code))
    #remove_spaces = wkt.read().replace(" ","")
    #output = remove_spaces.replace("\n", "")

    from osgeo.osr import SpatialReference

    srs = SpatialReference()
    srs.ImportFromEPSG(epsg_code)
    outWKT = srs.ExportToWkt()
    return str(outWKT)

def get_year_laserID_from_recndx(rndx):

    if rndx > 115657054 and rndx < 139113811: lID, year = 'L2A', 2003
    elif rndx > 217602635 and rndx < 232247064: lID, year = 'L2C', 2004
    elif rndx > 277304961 and rndx < 292736041: lID, year = 'L3A', 2004
    elif rndx > 336382900 and rndx < 351518549: lID, year = 'L3B', 2005
    elif rndx > 376139426 and rndx < 390631206: lID, year = 'L3C', 2005
    elif rndx > 442783123 and rndx < 457075593: lID, year = 'L3D', 2005
    elif rndx > 496304419 and rndx < 510663620: lID, year = 'L3E', 2006
    elif rndx > 535568792 and rndx < 549832457: lID, year = 'L3F', 2006
    elif rndx > 602008610 and rndx < 616334316: lID, year = 'L3G', 2006
    elif rndx > 661425008 and rndx < 675958194: lID, year = 'L3H', 2007
    elif rndx > 749901728 and rndx < 764251809: lID, year = 'L3I', 2007
    elif rndx > 931281270 and rndx < 940762915: lID, year = 'L2D', 2008
    else:
##        print "Cannot find laser ID or campaign year for record {}".format(rndx)
        lID, year = None, None

    return lID, year

def get_proj_info(raster): # get the SRS WKT string and EPSG code from input raster

    prj = gdal.Open(raster).GetProjection()
    srs = osr.SpatialReference(wkt=prj)
    epsg = srs.GetAttrValue("AUTHORITY", 1)

    return (prj, srs, epsg) # returns PRJ str [0], SRS string [1] and EPSG code [2]

def get_gcs_extent(raster): # get GCS (decimal degrees) extent from projected raster

    # Get the extent from the geotransform
    ds=gdal.Open(raster)
    gt=ds.GetGeoTransform()
    cols = ds.RasterXSize
    rows = ds.RasterYSize

    ext=[]
    xarr=[0,cols]
    yarr=[0,rows]

    for px in xarr:
        for py in yarr:
            x=gt[0]+(px*gt[1])+(py*gt[2])
            y=gt[3]+(px*gt[4])+(py*gt[5])
            ext.append([x,y])
##            print x,y
        yarr.reverse()

    src_srs=osr.SpatialReference()
    src_srs.ImportFromWkt(ds.GetProjection())
    tgt_srs = src_srs.CloneGeogCS()

    # Reproject the coordinates to GCS
    geo_ext=[]
    transform = osr.CoordinateTransformation( src_srs, tgt_srs)
    for x,y in ext:
        x,y,z = transform.TransformPoint(x,y)
        geo_ext.append([x,y])

    # Get the gdal extent (xmin, ymin, xmax, ymax) from the GCS coords
    xmin = min(geo_ext[0][0], geo_ext[1][0])
    xmax = max(geo_ext[2][0], geo_ext[3][0])
    ymin = min(geo_ext[1][1], geo_ext[2][1])
    ymax = max(geo_ext[0][1], geo_ext[3][1])

    return (xmin, ymin, xmax, ymax)

def getSunAngle(useXml):

    tree = ET.parse(useXml)
    IMD = tree.getroot().find('IMD')

    try:
        return str(float(IMD.find('IMAGE').find('MEANSUNEL').text))
    except:
        return None

def make_GLAS_csv_list(raster, GLAS_csv_dir):

    # Create a list of csv's needed for AOI
    (xmin, ymin, xmax, ymax) = get_gcs_extent(raster)

    # get left and right, and top and bottom rounded to the nearest 5 (with extra on the bottom and left)
    left = int(5 * round(float(xmin)/5)) - 5
    bottom = int(5 * round(float(ymin)/5)) - 5
    right = int(5 * round(float(xmax)/5))
    top = int(5 * round(float(ymax)/5))

    csv_list = []
    for x in range(left, right+5, 5):
        for y in range(bottom, top+5, 5):

            xSuff = 'E' # is always East-based
            if x < 0: x = str(x + 360).zfill(3)
            else: x = str(x).zfill(3)

            y = str(y).zfill(2) # num is always y, but suffix depends on location
            if y < 0: ySuff = 'S'
            else: ySuff = 'N'

            # METRICS
            #glas_csv = os.path.join(GLAS_csv_dir, 'gla14_{}{}{}{}.csv'.format(ySuff, y, xSuff, x)) # v1
            #glas_csv = os.path.join(GLAS_csv_dir, '{}{}{}{}-data-metrices.csv'.format(ySuff, y, xSuff, x)) #* 11/27/2018 updated -- old v2 names
            glas_csv = os.path.join(GLAS_csv_dir, 'gla01-{}{}{}{}-data.csv'.format(ySuff, y, xSuff, x)) # 3/21/2019 updated
            if os.path.isfile(glas_csv):
                csv_list.append(glas_csv)
            else: print "{} does not exist".format(glas_csv)
    #return ['/att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/data/glas/circ_boreal/gla01-boreal50up-fix2-data.csv']
    return csv_list

def create_GLAS_point_shp(raster, GLAS_csv_dir, outputShpDir):
    outShp = os.path.join(outShpDir, bnameFromRaster.shp)

    # get list of GLAS csv's
    GLAS_csv_list = make_GLAS_csv_list(raster, GLAS_csv_dir)

# make sure ID field is there so we can eliminate the func below that relies on arcpy

def create_ID_field(idField, inShp): # used this for arcgis method
    import arcpy
    if idField not in [field.name for field in arcpy.ListFields(inShp)]:
        print '{} field does not exist'.format(idField)
        arcpy.AddField_management(inShp, idField, "SHORT")
        arcpy.CalculateField_management(inShp, idField, "!FID!+1", "PYTHON")
    else: print "{} field already exists in {}\n\n".format(idField, inShp)
    return None

def inputTxt_toDict(textfile): # feeds first line without # to GLASshp and the rest to layerDict
    layerDict = {}
    with open(textfile, 'r') as it:
        g = 0 # first line without # is GLAS layer
        for line in it.readlines():
            line = line.strip()
            if line.startswith('#'): continue # ignore comments
            g+=1
            if g == 1: GLASshp = line
            else: # else put layer into the dictionary
                (key, val_str) = line.split('::')
                layerDict[str(key)] = val_str.split(',')
    return GLASshp, layerDict

def update_dict_forLayer(key, values, dictName, tableName, fields): # used this for arcgis method
    import arcpy
    layerKeys = [] # list of layer keys so we can fill in those not found in zonal stats of neighbor later
    dictName[key].extend(values) # append new fields to hdr
    # now loop through lines of dbf and add stats for each GLAS_id key #* TD instead of looping through lines of dbf, loop through lines of new output
    with arcpy.da.SearchCursor(tableName,fields) as curs:
        for row in curs:
            GLAS_id = str(row[0])
            layerKeys.append(GLAS_id)

            dictName[GLAS_id].extend(list(row[1:])) # append new field vals to dict

    return dictName, layerKeys

# write final output dictionary to database
def write_dict_toDatabase(database, dictName, headerFieldName):
    with open(database, 'w') as od:
        od.write('{},{}\n'.format(headerFieldName, ", ".join(dictName[headerFieldName]).replace(', ', ','))) # first write header
        for key, stats in dictName.items():
            stats = [str(s) for s in stats] # convert items in stats to str
            if key != headerFieldName:
                od.write('{},{}\n'.format(key, ", ".join(stats).replace(', ', ','))) # if its not the header, write to csv
    return None

def determine_runZonal(layer, dbfLayerTable):
    skipLayer = False # only true under one condition (below)
    if not os.path.isfile(layer):
        print " Layer {} does not exist. Skipping zonal stats run\n".format(layer)
        runZonal = False
        if not os.path.isfile(dbfLayerTable): # however, if the dbf doesnt exist either we have to skip
            print " --The dbf doesn't exist either. Skipping layer entirely and it will not be written to output database"
            skipLayer = True

    if not os.path.isfile(dbfLayerTable):
        runZonal = True
    else:
        print " {} already exists. Skipping zonal stats but still writing to output\n".format(dbfLayerTable)
        runZonal= False

    return runZonal, skipLayer

