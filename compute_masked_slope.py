#!/usr/bin/python
#
# Compute a masked slope dataset based on input dem and max valid slope

import sys
import os
import re
import argparse

import numpy as np
import scipy.ndimage.morphology as morph

from pygeotools.lib import iolib
from pygeotools.lib import malib
from pygeotools.lib import geolib
from pygeotools.lib import warplib

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
    #parser.add_argument('-slope_res', type=int, default=10, help='The res of the dem on which slope is calculated and output')
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
    
    print "\n\tGetting masked slope of input dem..."
    
    #Get a coarsened version of DEM on which to calc slope
    dem_fn_reduced = os.path.splitext(dem_fn)[0]+'_'+ str(reduce_pct) +'pct.vrt'
    #dem_fn_reduced = os.path.splitext(dem_fn)[0]+'_'+ str(slope_res) +'m.vrt'
    
    #print "\tReducing percent by %s..." %(str(reduce_pct))
    run_os("gdal_translate -of VRT -r cubic -outsize " + str(reduce_pct) + "% " + str(reduce_pct) + "% " + dem_fn + " " + dem_fn_reduced)
    #run_os("gdal_translate -of VRT -r cubic -tr " + str(slope_res) + "% " + str(slope_res) + "% " + dem_fn + " " + dem_fn_reduced)

    # Run slope
    dem_slope_fn = geolib.gdaldem_wrapper(dem_fn_reduced, product='slope', returnma=False)

    # Get original ma and ds
    dem_slope = iolib.fn_getma(dem_slope_fn)
    dem_slope_ds = iolib.fn_getds(dem_slope_fn)

    # Apply mask from slope to slope
    dem_slope = np.ma.array(dem_slope, mask=np.ma.masked_outside(dem_slope, *slopelim).mask, keep_mask=True, fill_value=dem_slope.fill_value)

    # Save the filtered slope dataset
    dst_fn = os.path.splitext(dem_slope_fn)[0]+'_mask.tif'
    iolib.writeGTiff(dem_slope, dst_fn, dem_slope_ds, ndv=ndv)
    run_os("rm -fv " + dem_slope_fn)

    return dst_fn

if __name__ == "__main__":
    main()