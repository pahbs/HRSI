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

# NOTE: let's use the term 'mask' to refer to areas of interest for which valid pixels are returned. 
# eg., hi_slope_mask will return valid '1' values for high slopes (above max_slope), and NaN elsewhere
def get_min_gaus(ras_fn, sample_step=50, ncomp=3):
    # Get ma
    masked_array = iolib.fn_getma(ras_fn)
    # Sample ma
    masked_array= sample_ma(masked_array, sample_step)

    if masked_array is None:
        mean_min = 0
        stdev = 0
        print "No shift will be done. Masked array is None. Setting mean and stdv to 0."
    else:

        # Do gaussian fitting
        means, vars, weights = fit_gaus(masked_array, ncomp)

        sample_step_str = "%03d" % (sample_step)
        histo = matplotlib.pyplot.hist(masked_array.compressed(), 300, normed=True, color='gray', alpha = 0.5)
        #Write histogram
        fig_name = ras_fn.split('/')[-1].strip('.tif') + "_" + str(ncomp) + "_" + sample_step_str + '.png'
        i = 0

        out_means = []
        out_stdevs = []
        for w, m, c in zip(weights, means, vars):
            i += 1
            matplotlib.pyplot.plot(histo[1], w*scipy.stats.norm.pdf( histo[1], m, np.sqrt(c) ), linewidth=3)
            #matplotlib.pyplot.axis([min(masked_array.compressed()),max(masked_array.compressed()),0,1])
            gauss_num = 'Gaussian peak #%s' %(i)

            print 'Gaussian peak #%s (mean, stdv):  %s, %s' %(i, round(m,3), round(np.sqrt(c),3))

            out_means.append(m)
            out_stdevs.append(np.sqrt(c))

        matplotlib.pyplot.savefig(os.path.join(os.path.dirname(ras_fn),fig_name))
        matplotlib.pyplot.clf()
        print "Saved histogram fig:"
        print os.path.join(os.path.dirname(ras_fn),fig_name)

        # Find min
        mean_min = min(out_means)
        stdev = np.sqrt(vars[out_means.index(mean_min)])

    return mean_min, stdev

def get_toa_fn(dem_fn):
    toa_fn = None
    dem_dir_list = os.path.split(os.path.abspath(dem_fn))[0].split(os.sep)
    import re
    #Get index of the top level pair directory containing toa (WV02_20140514_1030010031114100_1030010030896000)
    r_idx = [i for i, item in enumerate(dem_dir_list) if re.search('(_10)*(_10)*00$', item)]
    if r_idx:
        r_idx = r_idx[0]
        #Reconstruct dir
        dem_dir = (os.sep).join(dem_dir_list[0:r_idx+1])
        #Find toa.tif in top-level dir
        toa_fn = glob.glob(os.path.join(dem_dir, '*toa.tif'))
        # Check for *r100.xml here; if not exists, then break
        if not toa_fn:
            # My own version, with an edit to recognize ortho.tif, then use the 4m version of the ortho
            cmd = ['toa_calc.sh', dem_dir]
            print(cmd)
            subprocess.call(cmd)
            toa_fn = glob.glob(os.path.join(dem_dir, '*toa.tif'))
        toa_fn = toa_fn[0]
    return toa_fn

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

# inputs: 
#	sr05*tif (chm)
#	ortho_toa.tif
#	out-DEM_1m.tif

def getparser():
    parser = argparse.ArgumentParser(description="Gets the number of valid height and age pixels from a 3DSI stacks")
    parser.add_argument('-max_slope', type=int, default=20, help='Max slope (degrees) that will be included (default: %(default)s)')
    parser.add_argument('-min_rough', type=float, default=0.75, help='Min roughness (diff *input units* from adjacent) to be included (default: %(default)s)')
    parser.add_argument('-min_tri', type=float, default=0.001, help='Min TRI (the smoothest surfaces) to be included (default: %(default)s)')
    parser.add_argument('-min_toa', type=float, default=0.15, help='Min TOA to be included (default: %(default)s)')
    parser.add_argument('-outdir', type=str, help='Directory to output the text file with pixel count')
    parser.add_argument('--no-auto_min_toa', dest='auto_min_toa', action='store_false', help='Turn off auto-compute min TOA using gaussian mixture model')
    parser.add_argument('pairname', type=str, help='Pairname to run the count on')
    return parser

def main():

    """
    # chm_refine.py
    # Main goals: 
    # (1)fix non-forest "heights"
    # (2)fix dense interior forest height estimates
    # (3)remove water
    # Basic logic
    #Divide HRSI CHM into forest and non-forest;
    #to estimate max canopy height, within the forest mask run a 'max'filter (filtlib)
    #to remove spurious 'heights' in the non-forest using a 'min' filter (filtlib)
    #
    #(1) invert roughmask to get 'forest'
    #    -run a 'max' filter,
    #(2) use roughmask (get_lo_rough_mask) to get 'non-forest' pixels
    #    -run a 'min' filter,
    #    -maybe a small (3 pix) window filter
    #(3) then mask the result with the toamask (this removes water and other dark (shadow) areas
    #    -remove dark and smooth (water)
    #    -smooth is non-veg land
    #    -dark and rough is woody veg land
    #
    #(4) for later: then mask with the slopemask, toatrimask
    
    # step 1
    #  get_dark_mask from ortho_toa --> remove the areas that are TOA dark (water,shadow) from the chm
    # step 2
    #	get_hi_slope_mask from DEM --> remove areas of high slopes from chm
    # step 3
    #	get_lo_rough_mask from DEM --> remove areas that are NOT rough (aka remove non-forest)
    #	run a max filter on remaining pixels
    
    * mask outputs should all consistently show the 'masked' area as valid
    """
    parser=getparser()
    args=parser.parse_args()
    
    outdir=args.outdir
    pairname=args.pairname
    
    if not os.path.exists(outdir):
        os.mkdir(outdir)
        
    outfolder=os.path.join(outdir,pairname)
    if not os.path.exists(outfolder):
        os.mkdir(outfolder)
    
    auto_min_toa=args.auto_min_toa
    
    #Symlink files to working directory.
    #symlinks=['out-DEM_1m.tif','{}_ortho.tif'.format(pairname)]
    print("\nSymlinking Files to Working Directory\n")
    cmd="ln -sf /att/pubrepo/DEM/hrsi_dsm/v2/{}/*ortho*tif {}".format(pairname,outfolder)
    subprocess.call(cmd,shell=True)
    
    cmd="ln -sf /att/pubrepo/DEM/hrsi_dsm/v2/{}/out-DEM*m.tif {}".format(pairname,outfolder)
    subprocess.call(cmd,shell=True)
    
    cmd="xml_fn_list=$(ls /att/pubrepo/DEM/hrsi_dsm/v2/{}/*.xml);ln -sf $xml_fn_list {}".format(pairname,outfolder)
    subprocess.call(cmd,shell=True)
    
    #dsm_maindir='/att/pubrepo/DEM/hrsi_dsm/v2/'
    #dsm_dir=os.path.join(dsm_maindir,pairname)
    
    chm_dir='/att/gpfsfs/briskfs01/ppl/pmontesa/chm_work/hrsi_chm_sgm_filt/chm'
    chm_name='{}_sr05_4m-sr05-min_1m-sr05-max_dz_eul.tif'.format(pairname)
    chm_fn=os.path.join(chm_dir,chm_name)

    
    print("[1]\nLoading Input CHM into masked array\n")
    chm_ds = iolib.fn_getds(chm_fn)
    
    print("[2]\nGetting Dark Mask from Ortho TOA\n")
    #May need to include get_toa_fn from DEM_control.py
    print("\n\t-Compute TOA from Ortho\n")
    dem_fn=os.path.join(outfolder,'out-DEM_1m.tif')
    toa_fn = get_toa_fn(dem_fn)
    
    print("\nt-Warp TOA to CHM...\n")
    toa_ds = warplib.memwarp_multi_fn([toa_fn,], res=chm_ds, extent=chm_ds, t_srs=chm_ds)[0]
    
    #Determine from inputs or calculate lowest acceptable TOA valuesfor masking
    if auto_min_toa:
        # Compute a good min TOA value
        m,s = get_min_gaus(toa_fn, 50, 4)
        min_toa = m + s
        min_toa = m
    else:
        min_toa = args.min_toa
    
    #Write TOA Mins for reference
    with open(os.path.join(os.path.split(toa_fn)[0], "min_toa_" + pairname + ".txt"), "w") as text_file:
        text_file.write(os.path.basename(__file__))
        text_file.write("\nMinimum TOA used for mask:\n{0}".format(min_toa))

    # Should mask dark areas and dilate
    dark_mask = get_dark_mask(toa_ds, min_toa)
    
    print("\n\t-Completed Calculating Dark Mask\n")
    
    print("[3]\nGetting High Slope Mask from DEM\n")
    
    max_slope=args.max_slope
    dem_ds = iolib.fn_getds(dem_fn)
    slope_mask=get_hi_slope_mask(dem_ds, max_slope)
    print("\n\t-Completed Sploe Masking\n")
    
    print("[4]\nGetting Roughness for Forest/Non-Forest Classification\n")
    
    #NOTE: Not sure which mask we want to use. Will write up both
    min_rough=args.min_rough
    
    lo_rough_mask=get_lo_rough_mask(dem_ds, min_rough)
    #Areas less than min_rough is masked#
    min_tri=args.min_tri
    lo_tri_mask=get_lo_tri_mask(dem_ds, min_tri)
    
    #Valid areas are forest
    forest_mask=np.logical_or(lo_rough_mask,log_tri_mask)
    
    ground_mask = ~forest_mask
        

if __name__ == '__main__':
    main()
