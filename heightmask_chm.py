#!/usr/bin/python
#
# Height-mask a CHM with values less than a min height calc'd from a CHM coarsened by reduce_pct

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

def getparser():
    parser = argparse.ArgumentParser(description="Compute a masked CHM dataset based on input CHM and min valid height")
    parser.add_argument('chm_fn', type=str, help='Input CHM')
    parser.add_argument('-outdir', type=str, default=None, help='Output dir for *_htmasked.tif')
    parser.add_argument('-ndv', type=int, default=-99, help='Output no data value (default: %(default)s)')
    parser.add_argument('-min_height', type=float, default=1.37, help='Min valid height (m) (default: %(default)s)')
    parser.add_argument('-max_height', type=float, default=40.0, help='Max valid height (m) (default: %(default)s)')
    return parser

def main():

    parser = getparser()
    args = parser.parse_args()
    chm_fn = args.chm_fn
    outdir = args.outdir
    ndv = args.ndv
    min_height = args.min_height
    max_height = args.max_height

    if not iolib.fn_check(chm_fn):
        sys.exit("Unable to find chm_fn: %s" % chm_fn)

    out_base = os.path.splitext(chm_fn)[0]
    if outdir is not None:
        inputdir, chmname = os.path.split(chm_fn)
        out_base = os.path.join(outdir, os.path.splitext(chmname)[0])

    # Get chm ds and ma
    chm_ds = iolib.fn_getds(chm_fn)
    chm_ma = iolib.ds_getma(chm_ds)

    # Get a new chm ma using max height
    heightmasklo = (chm_ma < min_height)
    heightmaskhi = (chm_ma > max_height)

    # Get whichever dem ma was there in the first place
    chmmask = (chm_ma==ndv)
    # https://stackoverflow.com/questions/20528328/numpy-logical-or-for-more-than-two-arguments
    newmask = np.logical_or.reduce((chmmask , heightmasklo, heightmaskhi) ) #np.logical_or(heightmasklo, heightmaskhi)

    # Apply mask
    newchm = np.ma.array(chm_ma, mask=newmask)

    # Save the new masked CHM
    dst_fn = out_base +'_htmasked.tif'
    iolib.writeGTiff(newchm , dst_fn, chm_ds, ndv=ndv)

    print(dst_fn)
    return dst_fn

if __name__ == "__main__":
    main()