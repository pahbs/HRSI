#!/usr/bin/env python
# Adapted from compute_dz.py by David Shean
# paul.m.montesano@nasa.gov

#Utility to mask and difference two aligned input DEMs to estimate canopy height
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
from pygeotools.lib import filtlib

import dem_control
from demcoreg import dem_mask

def range_fltr(dem, rangelim):
    """Range filter (helper function)
    """
    print('Excluding values outside of range: {0:f} to {1:f}'.format(*rangelim))
    out = np.ma.masked_outside(dem, *rangelim)
    out.set_fill_value(np.nan)
    return out


def getparser():
    parser = argparse.ArgumentParser(description="Compute difference between two rasters")
    parser.add_argument('dem1_fn', type=str, help='DEM filename 1 that has been aligned')
    parser.add_argument('dem2_fn', type=str, help='DEM filename 2 that has been aligned')
    parser.add_argument('-tr', default='max', help='Output resolution (default: %(default)s)')
    parser.add_argument('-te', default='intersection', help='Output extent (default: %(default)s)')
    parser.add_argument('-t_srs', default='first', help='Output projection (default: %(default)s)')
    parser.add_argument('-min_toa', type=float, default=0.18, help='Min TOA that will be included')
    parser.add_argument('-min_toatri', type=float, default=0.001, help='Min TOA TRI that will be included')
    parser.add_argument('-max_slope', type=int, default=20, help='Max slope (degrees) that will be included')
    parser.add_argument('-outdir', default=None, help='Output directory')
    return parser

def main():
    parser = getparser()
    args = parser.parse_args()

    #This is output ndv, avoid using 0 for differences
    diffndv = np.nan

    dem1_fn = args.dem1_fn
    dem2_fn = args.dem2_fn

    if dem1_fn == dem2_fn:
        sys.exit('Input filenames are identical')

    fn_list = [dem1_fn, dem2_fn]

    print("Warping DEMs to same res/extent/proj")
    dem1_ds, dem2_ds = warplib.memwarp_multi_fn(fn_list, extent=args.te, res=args.tr, t_srs=args.t_srs)

    print("Loading input DEMs into masked arrays")
    dem1 = iolib.ds_getma(dem1_ds)
    dem2 = iolib.ds_getma(dem2_ds)

    outdir = args.outdir
    if outdir is None:
        outdir = os.path.split(dem1_fn)[0]
    outprefix = os.path.splitext(os.path.split(dem1_fn)[1])[0]+'_'+os.path.splitext(os.path.split(dem2_fn)[1])[0]

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

    #Compute relative elevation difference with Eulerian approach
    print("Computing elevation difference with Eulerian approach")
    diff_euler = np.ma.array(dem2-dem1, mask=common_mask)

    #Absolute range filter on the difference
    # removes differences outside of a range that likely arent related to canopy heights
    diff_euler = range_fltr(diff_euler, (-3, 30))

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
