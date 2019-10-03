#!/usr/bin/env python
#
# Utility to refine the HRSI CHMs with filters applied according to masks
# 
#
# 

import sys
import os
import subprocess
import glob
import argparse
import shutil

import numpy as np

from pygeotools.lib import iolib
from pygeotools.lib import malib
from pygeotools.lib import geolib
from pygeotools.lib import filtlib
from pygeotools.lib import warplib

import matplotlib
##https://stackoverflow.com/questions/37604289/tkinter-tclerror-no-display-name-and-no-display-environment-variable
matplotlib.use('Agg')
import matplotlib.pyplot, matplotlib.mlab, math
import scipy.stats

#TOA reflectance mask (formerly get_toa_mask)
def get_dark_mask(toa_ds, min_toa):
    print("\nApplying TOA filter to remove dark areas (water and shadows) using pan TOA (masking values < %0.4f)" % min_toa)
    toa = iolib.ds_getma(toa_ds)
    dark_mask = np.ma.masked_less(toa, min_toa)
    #This should be 1 for valid surfaces, nan for removed surfaces
    dark_mask = ~(np.ma.getmaskarray(dark_mask))
    return dark_mask

#DEM slope mask
def get_hi_slope_mask(dem_ds, max_slope):
    print("\nApplying DEM slope filter (masking values > %0.1f)" % max_slope)
    #dem = iolib.ds_getma(dem_ds)
    slope = geolib.gdaldem_mem_ds(dem_ds, 'slope', returnma=True)
    hi_slope_mask = np.ma.masked_greater(slope, max_slope)
    #This should be 1 for valid surfaces, nan for removed surfaces
    hi_slope_mask = ~(np.ma.getmaskarray(hi_slope_mask))
    return hi_slope_mask

#DEM or TOA smooth mask : note this seems coarser than TRI
def get_lo_rough_mask(dem_ds, min_rough):
    # Roughness is the largest inter-cell difference of a central pixel and its surrounding cell, as defined in Wilson et al (2007, Marine Geodesy 30:3-35).
    print("\nApplying roughness filter (masking low roughness values < %0.4f)" % min_rough)
    #dem = iolib.ds_getma(dem_ds)
    rough = geolib.gdaldem_mem_ds(dem_ds, 'Roughness', returnma=True)
    lo_rough_mask = np.ma.masked_greater(rough, min_rough)
    #This should be 1 for valid surfaces, nan for removed surfaces
    lo_rough_mask = ~(np.ma.getmaskarray(lo_rough_mask))
    return lo_rough_mask

#DEM or TOA Terrain Ruggedness index mask
def get_lo_tri_mask(dem_ds, min_tri):
    # TRI is the mean difference between a central pixel and its surrounding cells (see Wilson et al 2007, Marine Geodesy 30:3-35).
    print("\nApplying TRI filter (masking low TRI values < %0.4f)" % min_tri)
    #dem = iolib.ds_getma(dem_ds)
    tri = geolib.gdaldem_mem_ds(dem_ds, 'TRI', returnma=True)
    lo_tri_mask = np.ma.masked_less(tri, min_tri)
    #This should be 1 for valid surfaces, nan for removed surfaces
    lo_tri_mask = ~(np.ma.getmaskarray(lo_tri_mask))
    return lo_tri_mask

def main():
    pass

"""
First, merge 2 versions of dem_control

Then, write a chm_filt.py

import dem_control

# Basic logic
Divide HRSI CHM into forest and non-forest;
to estimate max canopy height, within the forest mask run a 'max'filter (filtlib)
to remove spurious 'heights' in the non-forest using a 'min' filter (filtlib)


(1) use roughmask (as is now in dem_control) to get 'non-forest' valid pixels
    -run a 'min' filter,
    -maybe a small (3 pix) window filter

(2) invert roughmask to get 'forest'
    -run a 'max' filter,

(3) then mask the result with the toamask (this removes water and other dark (shadow) areas
    -remove dark and smooth (water)
    -smooth is non-veg land
    -dark and rough is woody veg land

(4) for later: then mask with the slopemask, toatrimask


* mask outputs should all consistently show the 'masked out' area as NaN (eg, the slopemask)
"""
if __name__ == '__main__':
    main()
