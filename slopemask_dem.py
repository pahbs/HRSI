#!/usr/bin/python
#
# Slope-mask a DEM with values greater than a max slope calc'd from a DEM coarsened by reduce_pct

import sys
import os
import re
import argparse
import subprocess

import numpy as np
import scipy.ndimage.morphology as morph

from pygeotools.lib import iolib
from pygeotools.lib import malib
from pygeotools.lib import geolib
from pygeotools.lib import warplib

def mask_slope(ds, maxslope=10, out_fn=None):
    """Generate raster mask for slope from input slope
    """
    print("Loading slope")
    b = ds.GetRasterBand(1)
    l = b.ReadAsArray()
    print("Masking pixels with >%0.1f%% slopes\n" % maxslope)
    if maxslope < 0.0 or maxslope > 90.0:
        sys.exit("Invalid slope in degrees")
    mask = (l<maxslope)
    #Write out original data
    if out_fn is not None:
        print("Writing out %s\n" % out_fn)
        iolib.writeGTiff(l, out_fn, ds)
    l = None

    return mask

def run_os(cmdStr):
    """
    Initialize OS command
    Wait for results (Communicate results i.e., make python wait until process is finished to proceed with next step)
    """
    import subprocess as subp

    Cmd = subp.Popen(cmdStr.rstrip('\n'), stdout=subp.PIPE, shell=True)
    stdOut, err = Cmd.communicate()

    print ("\n\tInitialized: %s" %(cmdStr))

def getparser():
    parser = argparse.ArgumentParser(description="Compute a masked slope dataset based on input dem and max valid slope")
    parser.add_argument('dem_fn', type=str, help='Input dem')
    parser.add_argument('-ndv', type=int, default=-99, help='Output no data value (default: %(default)s)')
    parser.add_argument('-max_slope', type=int, default=20, help='Max slope (degrees) that will be included/valid')
    parser.add_argument('-reduce_pct', type=int, default=100, help='The pct of the input dem res by which slope is initially coarsened before masking (default: %(default)s)')
    return parser

def main():

    parser = getparser()
    args = parser.parse_args()
    dem_fn = args.dem_fn
    ndv = args.ndv
    max_slope = args.max_slope
    reduce_pct = args.reduce_pct
    #slope_res = args.slope_res

    slopelim=(0.1, max_slope)

    out_base = os.path.splitext(dem_fn)[0]

    print "\n\tGetting a slopemasked dem...\n"

    if reduce_pct != 100:
        print "\tCoarsening by %s.." %(reduce_pct)
        out_fn = out_base + '_pct' + str(reduce_pct) + '.tif'
        if not os.path.exists(out_fn):
            red_pct_str = str(reduce_pct) + "% " + str(reduce_pct) + "%"
            cmdStr = " ".join(["gdal_translate", "-r", "cubic", "-outsize", red_pct_str, dem_fn, out_fn])
            print cmdStr
            #cmdStr = "gdal_translate -r cubic -outsize " + str(reduce_pct) + "% " + str(reduce_pct) + "%"
            #cmdStr += dem_fn + out_fn 
            cmd = subprocess.Popen(cmdStr, stdout=subprocess.PIPE, shell=True)
            stdOut, err = cmd.communicate()

            dem_fn = out_fn

    # Get slope as a masked array
    dem_slope_ma = geolib.gdaldem_wrapper(dem_fn, product='slope', returnma=True)

    # Get reduced dem ds
    dem_ds = iolib.fn_getds(dem_fn)
    dem_ma = iolib.ds_getma(dem_ds)

    # Get a new slope ma using max slope
    slopemask = (dem_slope_ma > max_slope)

    # Get whichever dem ma was there in the first place
    demmask = (dem_ma==ndv)
    newmask = np.logical_or(demmask, slopemask)

    # Apply mask from slope to slope
    newdem = np.ma.array(dem_ma, mask=newmask)

    # Save the new masked DEM
    dst_fn = out_base +'_slopemasked.tif'
    iolib.writeGTiff(newdem , dst_fn, dem_ds, ndv=ndv)

    return dst_fn

if __name__ == "__main__":
    main()