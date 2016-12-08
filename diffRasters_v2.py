import os, osgeo, numpy as np, numpy.ma as ma, subprocess as subp, struct
from osgeo.gdalnumeric import *
from osgeo.gdalconst import *
from osgeo import gdal, osr, ogr
from gdalconst import * # import all drivers
import scipy.ndimage.filters as filt
from distutils.util import strtobool

np.random.seed(11)
np.set_printoptions(precision=8)
driverTiff = gdal.GetDriverByName('GTiff')

def run_wait_os(cmdStr):
    """
    Initialize OS command
    Wait for results (Communicate results i.e., make python wait until process is finished to proceed with next step)
    """
    import subprocess as subp

    Cmd = subp.Popen(cmdStr.rstrip('\n'), stdout=subp.PIPE, shell=True)
    stdOut, err = Cmd.communicate()

    print ("\tInitialized: %s" %(cmdStr))
    print ("\t..Waiting for command to run...")
    print("\n\t" + str(stdOut) + str(err))
    print("\n\tEnd of printed cmd output.")

def run_os(cmdStr):
    """
    Initialize OS command
    Don't wait for results (don't communicate results i.e., python code proceeds immediately after initializing script)
    """
    import subprocess as subp

    Cmd = subp.Popen(cmdStr.rstrip('\n'), stdout=subp.PIPE, shell=True)
    stdOut, err = Cmd.communicate()

    print ("\n\tInitialized: %s" %(cmdStr))
    print ("\n\tMoving on to next step.")


def findRasterIntersect(raster1_ds,raster2_ds):
    """
    Returns the 2 arrays cut to the area of intersection betwen the
    """
    band1 = raster1_ds.GetRasterBand(1)
    band2 = raster2_ds.GetRasterBand(1)

    # trim raster arrays to overlap if necessary
    # https://gis.stackexchange.com/questions/16834/how-to-add-different-sized-rasters-in-gdal-so-the-result-is-only-in-the-intersec
    gt1 = raster1_ds.GetGeoTransform()
    gt2 = raster2_ds.GetGeoTransform()

    # r1 has left, bottom, right, top of dataset's bounds in geospatial coordinates.
    r1 = [gt1[0], gt1[3], gt1[0] + (gt1[1] * raster1_ds.RasterXSize), gt1[3] + (gt1[5] * raster1_ds.RasterYSize)]
    r2 = [gt2[0], gt2[3], gt2[0] + (gt2[1] * raster2_ds.RasterXSize), gt2[3] + (gt2[5] * raster2_ds.RasterYSize)]
    print '\t        - - bounding box: [left, top, right, bottom]'
    print '\t        - 1 bounding box: %s' % str(r1)
    print '\t          2 bounding box: %s' % str(r2)

    intersection = [    max(r1[0], r2[0]) , \
                        min(r1[1], r2[1]) , \
                        min(r1[2], r2[2]) , \
                        max(r1[3], r2[3]) ]
    if r1 != r2:
        print '\t          ** different bounding boxes **'
        # check for any overlap at all...
        if (intersection[2] < intersection[0]) or (intersection[1] < intersection[3]):
            intersection = None
            print '\t        ***no overlap***'
            return np.array([]),0,0,0,0
        else:
            print '\t          intersection:',intersection
            left1   = int(round((intersection[0] - r1[0]) / gt1[1]))          # difference divided by pixel dimension
            top1    = int(round((intersection[1] - r1[1]) / gt1[5]))
            col1    = int(round((intersection[2] - r1[0]) / gt1[1])) - left1   # difference minus offset left
            row1    = int(round((intersection[3] - r1[1]) / gt1[5])) - top1

            left2   = int(round((intersection[0] - r2[0]) / gt2[1]))          # difference divided by pixel dimension
            top2    = int(round((intersection[1] - r2[1]) / gt2[5]))
            col2    = int(round((intersection[2] - r2[0]) / gt2[1])) - left2   # difference minus new left offset
            row2    = int(round((intersection[3] - r2[1]) / gt2[5])) - top2

            #print '\t        col1:',col1,'row1:',row1,'col2:',col2,'row2:',row2
            if col1 != col2 or row1 != row2:
                print col1, col2, row1, row2
                if abs(col1-col2) <=2 and abs(row1-row2) <= 2:
                    print " *** CORRECTING FOR COL/ROW ROUNDING ERROR ***"
                    col1, col2 = min(col1,col2),min(col1,col2)
                    row1, row2 = min(row1,row2),min(row1,row2)
                else:
                    print "*** MEGA ERROR *** COLS and ROWS DO NOT MATCH ***"
                    return np.array([]),0,0,0,0

            # these arrays should have the same spatial geometry though NaNs may differ
            print '\tReading array1...'
            array1 = band1.ReadAsArray(left1,top1,col1,row1)
            print '\tReading array2...'
            array2 = band2.ReadAsArray(left2,top2,col2,row2)

    else: # same dimensions
        col1 = raster1_ds.RasterXSize # = col2
        row1 = raster1_ds.RasterYSize # = row2
        array1 = band1.ReadAsArray()
        array2 = band2.ReadAsArray()

    print ">>\n\tCheck array1 range: min,max, med, mean"
    print ">>\n\t                  :",array1.min(),array1.max(),np.ma.median(array1),array1.mean()
    print ">>\n\tCheck array2 range: min,max, med, mean"
    print ">>\n\t                  :",array2.min(),array2.max(),np.ma.median(array2),array2.mean()

    band1 = None
    band2 = None
    ##array1 = np.where()
    return array1, array2, col1, row1, intersection


def diffRasters(outDir, inRaster1, inRaster2, res, kernelSize, ht_thresh, doFilt=False, isDSM=True, overwrite=False):
    """
    * must provide full path to these files: out-strip-holes-fill-DEM.tif
    ** watch for some hard-coded stuff, such as the EPSG in the gdalwarp command

    outDir      dir for the difference raster to be output
    inraster1   first raster (The Estimate)
    inraster2   raster that is subtracted from first (The Truth)
                (Estimate - Truth)
    res         VRT output resolution used in gdalwarp
    kernelSize  kernel used for minimum filter

    """
##    # Set booleans
##    doFilt = bool(strtobool(doFilt))
##    isDSM = bool(strtobool(isDSM))

    # Get input data at same res: build a VRT at 2m for each input
    vrtExt = "_" + str(res) + "m.vrt"
    res_str = "%03d" % (res)
    print "\n\tWorking on " + res_str + "m VRT versions of input rasters..."

    # Read in UTM projection info from first raster and put into -t_srs for gdalwarp of both rasters
    ds = gdal.Open(inRaster1)
    srs = osr.SpatialReference(wkt=ds.GetProjection())
    zone = srs.GetAttrValue('projcs').split('zone ')[1][:-1]        # removes the last char, which is 'N' or 'S'

    if int(zone) < 10:
        zone = "0" + str(zone)

    for inFile in [inRaster1, inRaster2]:

        if os.path.isfile(inFile.strip('.tif') + vrtExt):
            os.remove(inFile.strip('.tif') + vrtExt)

        print(" ------- ")
        print("\n\tInput Raster: " + inFile + "----")
        print(" ------- ")

        cmdStr = "gdalwarp -srcnodata -99 -dstnodata -99 -of VRT -tr " + str(res) + " " + str(res) + " -t_srs EPSG:326" + zone + " " + inFile + " " + inFile.strip('.tif') + vrtExt
        run_wait_os(cmdStr)

    # Get the datasets from the input
    inRaster1_ds = gdal.Open(inRaster1.strip('.tif') + vrtExt)
    gt = inRaster1_ds.GetGeoTransform()
    inRaster2_ds = gdal.Open(inRaster2.strip('.tif') + vrtExt)

    # Set up Tiff output name
    path1, name1 = os.path.split(inRaster1)
    path2, name2 = os.path.split(inRaster2)

    # Get a name string using the dates of each raster from their respective file names
    if len(path1.split('/')[-1].split('_')) > 1 and len(path2.split('/')[-1].split('_')) > 1:
        outNameStr = path1.split('/')[-1].split('_')[1] + "_" + path2.split('/')[-1].split('_')[1]
    else:
        outNameStr = name1.split(".")[0] + "_" + name2.split(".")[0]
    ##outVRT = os.path.join(outDir,"stack_dsm_" + outNameStr + vrtExt)

    # Using dir name of each input to help name the diff raster
    if isDSM:
        diffFn = os.path.join(outDir,"diff_dsm_" + outNameStr + '_' + res_str + 'm.tif')
    else:
        diffFn = os.path.join(outDir,"diff_HRSI_gliht_" + outNameStr.split('_')[2] + '_' + outNameStr.split('_')[3] + '_' + res_str + 'm.tif')
        rel_diffFn = os.path.join(outDir,"diff_HRSI_gliht_" + outNameStr.split('_')[2] + '_' + outNameStr.split('_')[3] + '_' + res_str + 'm_rel.tif')

    if ( os.path.isfile(diffFn) and overwrite ) or not os.path.isfile(diffFn):
        # Find intersection of two rasters
        ras1_array, ras2_array, col, row, intersection = findRasterIntersect(inRaster1_ds, inRaster2_ds)
        xmin, ymin, xmax, ymax = intersection[0], intersection[3], intersection[2], intersection[1]

        if doFilt:
            print "\t Filter ras2_array with local min: Same as min filter in hi_res_filter_v4..."
            # This one just operates on intersection array, instead of building a huge file to output and then read back in.
            ras2_array = np.where(ras2_array != -99, ras2_array, -99)
            ras2_array = filt.minimum_filter(ras2_array,kernelSize)

        print("\n\tDifferencing arrays...")

##        if isDSM:
##            # Test 2
##            diff_array = np.where((ras1_array != 0) & (ras2_array != 0), ras1_array - ras2_array , -99)
##            ##diff_array[diff_array <= -99] = np.nan
##            # Clean up
##            print('\n\tHeight threshold = ' + str(ht_thresh))
##            diff_array[np.fabs(diff_array) >= ht_thresh] = -99       # set to nan these diff pixels -- likely not valid..slopes maybe?
##        else:
        # --- The meat...
        print("\n\t\t [1] Replace -99 with nan...")
        #ras1 = ras1_array.copy()
        #ras2 = ras2_array.copy()
        ras1_array[ras1_array == -99]=np.nan
        ras2_array[ras2_array == -99]=np.nan

        print("\n\t\t [2] Do diff...")
        diff_array = np.where((ras1_array != np.nan) & (ras2_array != np.nan), np.around(ras1_array - ras2_array, decimals=1), np.nan)
        diff_array = np.where(np.isnan(diff_array),-99, diff_array)

        print "\n\t- Making Difference GeoTiff: ", diffFn.split('/')[-1]
        diff_ds = driverTiff.Create(diffFn, col, row, 1, gdal.GDT_Float32)
        diff_ds.SetGeoTransform((intersection[0], gt[1], 0, intersection[1], 0, gt[5])) # origin_x, px_width,0,origin_y,0,px_height) # set the datum
        diff_ds.SetProjection(inRaster1_ds.GetProjection())                                     # set the projection according to inRaster1
        diff_band = diff_ds.GetRasterBand(1)
        diff_band.WriteArray(diff_array)
        diff_band.FlushCache()

        if not isDSM:
            print("\n\t\t [3] Do relative diff (diff / Truth)...")
            ## Convert Estimate to absolute val; Add 0.1 to Truth to ensure there is no divde by zeros
            rel_diff_array = np.where((diff_array != np.nan) & (ras2 != np.nan), np.around(np.absolute(diff_array) / np.add(ras2, 0.1), decimals=1), np.nan)
            rel_diff_array = np.where(np.isnan(rel_diff_array),-99, rel_diff_array)

            print "\n\t- Making Relative Difference GeoTiff: ", rel_diffFn.split('/')[-1]
            diff_ds = driverTiff.Create(rel_diffFn, col, row, 1, gdal.GDT_Float32)
            diff_ds.SetGeoTransform((intersection[0], gt[1], 0, intersection[1], 0, gt[5])) # origin_x, px_width,0,origin_y,0,px_height) # set the datum
            diff_ds.SetProjection(inRaster1_ds.GetProjection())                                     # set the projection according to inRaster1
            diff_band = diff_ds.GetRasterBand(1)
            diff_band.WriteArray(rel_diff_array)
            diff_band.FlushCache()

##    # Visual products: Hillshade and Colormap
##    cmdStr = "hillshade  " + diffFn + " -o  " + diffFn.strip('.tif') + "-hlshd-e25.tif -e 25"
##    run_wait_os(cmdStr)
##    cmdStr = "colormap  " + diffFn + " -s " + diffFn.strip('.tif') + "-hlshd-e25.tif -o " + diffFn.strip('.tif') + "-clr-shd.tif" + " --colormap-style /att/adaptfs/home/pmontesa/code/color_lut_diff.txt"
##    run_wait_os(cmdStr)
##    cmdStr ="gdaladdo -r average " + diffFn.strip('.tif') + "-clr-shd.tif 2 4 8 16"
##    run_wait_os(cmdStr)
       ##cmdStr = "gdalwarp -srcnodata -99 -dstnodata -99 -of VRT -tr " + str(res) + " " + str(res) + " -t_srs EPSG:326" + zone + " " + diffFn + " " + diffFn.split('.tif') + vrtExt
       ## run_wait_os(cmdStr)

        # Log file
        with open(diffFn.strip('.tif') + "_LOG.txt",'w') as out_txt:
            out_txt.write("Ouput Dir: %s" %(outDir) + '\n')
            out_txt.write("Input raster 1: %s" %(inRaster1) + '\n')
            out_txt.write("Input raster 2: %s" %(inRaster2) + '\n')

        del diff_ds
        diff_ds = None
        del inRaster1_ds
        inRaster1_ds = None
        del inRaster2_ds
        inRaster2_ds = None

    else:
        print("\tDiff raster exists: %s" %(diffFn))

    return diffFn

if __name__ == "__main__":
    import sys
    diffRasters(str(sys.argv[1]), str(sys.argv[2]), str(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5]), int(sys.argv[6]))