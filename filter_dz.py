#!/usr/bin/python
"""
Filter an input dz raster using a range
"""

import sys
import os
import argparse

import numpy as np
from osgeo import gdal

from pygeotools.lib import iolib
from pygeotools.lib import malib
from pygeotools.lib import filtlib

def getparser():
    parser = argparse.ArgumentParser(description="Apply a range filter to raster values")
    parser.add_argument('ras_fn', type=str, help='Raster filename')
    parser.add_argument('min', type=int, help='min value of range')
    parser.add_argument('max', type=int, help='max value of range')
    parser.add_argument('-stats', action='store_true', help='Compute and print stats before/after')
    return parser

def main():
    parser = getparser()
    args = parser.parse_args()

    ras_fn = args.ras_fn
    min = args.min
    max = args.max

    print("Loading dz raster into masked array")
    ras_ds = iolib.fn_getds(ras_fn)
    ras = iolib.ds_getma(ras_ds, 1)
    #Cast input ma as float32 so np.nan filling works
    ras = ras.astype(np.float32)
    ras_fltr = ras

    #Absolute range filter
    ras_fltr = filtlib.range_fltr(ras_fltr, (min, max))

    if args.stats:
        print("Input dz raster stats:")
        malib.print_stats(ras)
        print("Filtered dz raster stats:")
        malib.print_stats(ras_fltr)

    #Output filename will have 'filt' appended
    dst_fn = os.path.splitext(ras_fn)[0]+'_filt.tif'

    print("Writing out filtered dz raster: %s" % dst_fn)
    #Note: writeGTiff writes ras_fltr.filled()
    iolib.writeGTiff(ras_fltr, dst_fn, ras_ds)

if __name__ == '__main__':
    main()
