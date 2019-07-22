#!/usr/bin/env python
#
# Utility to plot a histogram of a raster

import sys
import os

import argparse

import numpy as np

from pygeotools.lib import iolib
from pygeotools.lib import malib
from pygeotools.lib import geolib
from pygeotools.lib import filtlib
from pygeotools.lib import warplib

from dem_control import sample_ma

import matplotlib
##https://stackoverflow.com/questions/37604289/tkinter-tclerror-no-display-name-and-no-display-environment-variable
matplotlib.use('Agg')
import matplotlib.pyplot, matplotlib.mlab, math
import scipy.stats

def getparser():
    parser = argparse.ArgumentParser(description="Utility to get histogram from a raster")
    parser.add_argument('ras_fn', type=str, help='Raster filename')
    parser.add_argument('-min_val', type=float, default=None, help='Min value that will be included')
    parser.add_argument('-max_val', type=float, default=None, help='Max value that will be included')
    parser.add_argument('-sample_step', type=int, default=50, help='Sampling step value')
    parser.add_argument('-axis_lab_x', type=str, default="X", help='X-axis label')
    return parser

def main():

    parser = getparser()
    args = parser.parse_args()

    ras_fn = args.ras_fn
    min_val = args.min_val
    max_val = args.max_val
    sample_step = args.sample_step

    # Get ma
    ma = iolib.fn_getma(ras_fn)

    # Sample ma
    if min_val is not None:
        ma = np.ma.masked_less(ma, min_val)
    if max_val is not None:
        ma = np.ma.masked_greater(ma, max_val)
    
    ma = sample_ma(ma, sample_step)

    if ma is None:
        print "No histogram. Array is None."
        fig_name = ""
    
    else:        

        sample_step_str = "%03d" % (sample_step)

        histo = matplotlib.pyplot.hist(ma.compressed(), 300, normed=True, color='gray', alpha = 0.5)
        matplotlib.pyplot.xticks(np.arange(min_val, max_val, 1.0))
        matplotlib.pyplot.xlabel(args.axis_lab_x, fontsize=12)

        #Write histogram
        fig_name = ras_fn.split('/')[-1].strip('.tif') + '_hist.png'

        matplotlib.pyplot.savefig(os.path.join(os.path.dirname(ras_fn),fig_name))
        matplotlib.pyplot.clf()

        print "Saved histogram fig:"
        print os.path.join(os.path.dirname(ras_fn),fig_name)

    return fig_name  

if __name__ == "__main__":
    main()