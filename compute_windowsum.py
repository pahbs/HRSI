#-------------------------------------------------------------------------------
# Name:        compute_windowsum.py
# Purpose:
#
# Author:      pmontesa
#
# Created:     14/03/2017
# Copyright:   (c) pmontesa 2017
# Licence:     <your licence>
#-------------------------------------------------------------------------------
#! /usr/bin/env python
"""Compute a sum per pixel from a moving window on a raster and return a GTiff

"""

import os
import sys
import argparse

import numpy as np
from osgeo import ogr
# Incorporate some of dshean code
# https://github.com/dshean/pygeotools
from pygeotools.lib import iolib
from pygeotools.lib import geolib
from pygeotools.lib import filtlib

def gauss_fltr(dem, sigma=1):
    print("\tApplying gaussian smoothing filter with sigma %s" % sigma)
    #Note, ndimage doesn't properly handle ma - convert to nan
    from scipy.ndimage.filters import gaussian_filter
    dem_filt_gauss = gaussian_filter(dem.filled(np.nan), sigma)
    #Now mask all nans
    #dem = np.ma.array(dem_filt_gauss, mask=dem.mask)
    out = np.ma.fix_invalid(dem_filt_gauss, copy=False, fill_value=dem.fill_value)
    out.set_fill_value(dem.fill_value)
    return out

def moving_window(arr, func=np.mean, window_size=3):
    """moving_window(array, window_size, func=mean)
    """
    from scipy.ndimage.filters import generic_filter
    print "\tCheck array type INPUT to filter: %s" %(arr.dtype)
    return generic_filter(arr, func, size=window_size)

def getparser():
    parser = argparse.ArgumentParser(description="Compute the sum per pixel from a moving window")
    parser.add_argument('r_fn', type=str, help='Input raster filename')
    parser.add_argument('windowsize', type=int, default=3, \
                        help='Moving window size, in pixels')
    return parser

def main():
    parser = getparser()
    args = parser.parse_args()

    r_fn = args.r_fn
    if not os.path.exists(r_fn):
        sys.exit("Unable to find r_fn: %s" % r_fn)

    windowsize=args.windowsize

    # r_fn to r_ds
    r_ds = iolib.fn_getds(r_fn)
    r_arr = r_ds.GetRasterBand(1).ReadAsArray()

    # Creating data range
    r_arr = np.ma.masked_outside(r_arr,0,100)   # mask all values outside this interval
    r_arr = np.ma.masked_invalid(r_arr)       # mask all nan and inf values
    #myType=float
    #r_arr = r_arr.astype(myType)

    print "\tDoing moving window on array..."
    arr_out = moving_window(r_arr.astype(np.uint16), func=np.sum, window_size=windowsize)
    #r = filtlib.rolling_fltr(r_arr, f=np.sum, size=windowsize, circular=True)
    #r = gauss_fltr(r_arr, sigma=1)
    print "\tCheck array type OUTPUT from filter: %s" %(arr_out.dtype)
    # Deal with negatives and nan
    arr_out = np.where(arr_out < 0, -99, arr_out)
    arr_out = np.where(np.isnan(arr_out),-99, arr_out)

    #Write out
    win_str = "%02d" % (int(windowsize))
    out_fn = os.path.splitext(r_fn)[0]+'_win'+win_str+'sum.tif'
    #Note: passing r_fn here as the src_ds
    iolib.writeGTiff(arr_out, out_fn, r_fn)

if __name__ == "__main__":
    main()
