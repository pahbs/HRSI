#!/usr/bin/python

# Adapted from compute_dz.py
# by
#David Shean
#dshean@gmail.com

#Utility to compute elevation change (canopy height) from two input DEMs of different sun elevation angles
# Added slope filter computed on warped version of dem1

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

def slope_fltr(dem_fn, reduce_pct, diffndv, slopelim=(0.1, 40)):
    
    #Get a coarsened version of DEM on which to calc slope
    #dem_fn_pct = os.path.splitext(dem_fn)[0]+'_'+ reduce_pct +'pct.vrt'
    #run_os("gdal_translate -of VRT -r cubic -outsize " + reduce_pct + "% " + reduce_pct + "% " + dem_fn + " " + dem_fn_pct)

    # Run slope
    #dem_slope = geolib.gdaldem_slope(dem_fn_pct)
    dem_slope = geolib.gdaldem_slope(dem_fn)
    
    # Get original DEM
    dem = iolib.fn_getma(dem_fn)
    dem_ds = iolib.fn_getds(dem_fn)

    # Set up mask at orig res
    # Binary closing on the mask of the coarsened slope to un-mask small areas captured by the slope filter
    # ---If coarsened with reduce_pct, how to go back to original res after this operation?
    #struct1=morph.generate_binary_structure(2,2)
    #dem_slope=morph.binary_dilation(dem_slope,structure=struct1,iterations=2).astype(dem_slope.dtype)

    # Apply mask from slope to original DEM
    ###out_array = np.ma.array(dem, mask=np.ma.masked_outside(dem_slope, *slopelim).mask, keep_mask=True, fill_value=dem.fill_value)
    dem_mask = np.ma.masked_outside(dem_slope, *slopelim).mask
    out_array = np.ma.array(dem, mask=dem_mask , keep_mask=True, fill_value=dem.fill_value)

    # Remove the slope raster now
    run_os("rm -f " + os.path.splitext(dem_fn)[0]+'_slope.tif')

    # Save the slope-filtered DEM to be used in the differencing
    dst_fn = os.path.splitext(dem_fn)[0]+'_slopefilt.tif'
    iolib.writeGTiff(out_array, dst_fn, dem_ds, ndv=diffndv)
    return dst_fn


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

def getparser():
    parser = argparse.ArgumentParser(description="Compute difference between two rasters")
    parser.add_argument('fn1', type=str, help='Raster filename 1')
    parser.add_argument('fn2', type=str, help='Raster filename 2')
    parser.add_argument('-max_slope', type=int, default=20, help='Max slope (degrees) that will be included')
    parser.add_argument('-slope_reduce_pct', type=int, default=20, help='The pct of the input dem res by which slope is initially coarsened before masking')
    parser.add_argument('-outdir', default=None, help='Output directory')
    return parser

def main():
    parser = getparser()
    args = parser.parse_args()

    #This is output ndv, avoid using 0 for differences
    diffndv = -99

    dem1_fn = args.fn1
    dem2_fn = args.fn2
    max_slope = args.max_slope
    slope_reduce_pct = args.slope_reduce_pct

    if dem1_fn == dem2_fn:
        sys.exit('Input filenames are identical')

    # Filter the hi sun elev (dem1) warp-trans-ref DEM to mask out steep slopes
    #Apply slope filter
    dem1_slpfilt_fn = slope_fltr(dem1_fn, slope_reduce_pct, diffndv, slopelim=(0.1, max_slope))

    #fn_list = [dem1_fn, dem2_fn]
    fn_list = [dem1_slpfilt_fn, dem2_fn, ]

    print("Warping DEMs to same res/extent/proj")
    dem1_ds, dem2_ds = warplib.memwarp_multi_fn(fn_list, extent='intersection', res='max')

    outdir = args.outdir
    if outdir is None:
        outdir = os.path.split(dem1_fn)[0]
    outprefix = os.path.splitext(os.path.split(dem1_fn)[1])[0]+'_'+os.path.splitext(os.path.split(dem2_fn)[1])[0]

    print("Loading input DEMs into masked arrays")
    dem1 = iolib.ds_getma(dem1_ds, 1)
    dem2 = iolib.ds_getma(dem2_ds, 1)

    #Extract basename
    adj = ''
    if '-adj' in dem1_fn:
        adj = '-adj'
    dem1_fn_base = re.sub(adj, '', os.path.splitext(dem1_fn)[0])
    dem2_fn_base = re.sub(adj, '', os.path.splitext(dem2_fn)[0])

    #Check to make sure inputs actually intersect
    #Masked pixels are True
    if not np.any(~dem1.mask*~dem2.mask):
        sys.exit("No valid overlap between input data")

    #Compute common mask
    print("Generating common mask")
    common_mask = malib.common_mask([dem1, dem2])

    ## Dilation to remove bad pixels adjacent to nodata
    #struct1=morph.generate_binary_structure(2,2)
    #common_mask=morph.binary_dilation(common_mask,structure=struct1,iterations=3).astype(common_mask.dtype)

    #Compute relative elevation difference with Eulerian approach
    print("Computing elevation difference with Eulerian approach")
    diff_euler = np.ma.array(dem2-dem1, mask=common_mask)

    if True:
        print("Eulerian elevation difference stats:")
        diff_euler_stats = malib.print_stats(diff_euler)
        diff_euler_med = diff_euler_stats[5]

    if True:
        print("Writing Eulerian elevation difference map")
        dst_fn = os.path.join(outdir, outprefix+'_dz_eul.tif')
        print(dst_fn)
        iolib.writeGTiff(diff_euler, dst_fn, dem1_ds, ndv=diffndv)

    if False:
        print("Writing Eulerian relative elevation difference map")
        diff_euler_rel = diff_euler - diff_euler_med
        dst_fn = os.path.join(outdir, outprefix+'_dz_eul_rel.tif')
        print(dst_fn)
        iolib.writeGTiff(diff_euler_rel, dst_fn, dem1_ds, ndv=diffndv)

    if False:
        print("Writing out DEM2 with median elevation difference removed")
        dst_fn = os.path.splitext(dem2_fn)[0]+'_med'+diff_euler_med+'.tif'
        print(dst_fn)
        iolib.writeGTiff(dem2 - diff_euler_med, dst_fn, dem1_ds, ndv=diffndv)

    if False:
        print("Writing Eulerian elevation difference percentage map")
        diff_euler_perc = 100.0*diff_euler/dem1
        dst_fn = os.path.join(outdir, outprefix+'_dz_eul_perc.tif')
        print(dst_fn)
        iolib.writeGTiff(diff_euler_perc, dst_fn, dem1_ds, ndv=diffndv)

    return dst_fn

if __name__ == "__main__":
    main()
