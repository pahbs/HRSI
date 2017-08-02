#!/usr/bin/python

"""
Correct a CHM (dz raster) using gaussian peak estimation on a sampled version of the image histogram
"""
import sys
import os
import argparse

import numpy as np
from osgeo import gdal
import csv
import shutil

import numpy as np
from sklearn import mixture
import matplotlib
##https://stackoverflow.com/questions/37604289/tkinter-tclerror-no-display-name-and-no-display-environment-variable
matplotlib.use('Agg')

import matplotlib.pyplot, matplotlib.mlab, math

from pygeotools.lib import iolib
from pygeotools.lib import filtlib
from pygeotools.lib import geolib


def slope_fltr_chm(chm_array, hi_sun_dem_fn, slopelim=(0.1, 30)):
    """Apply a filter to a chm array based on a slope mask calc'd from the associated hi-sun-elev (ground) DSM
    """
    #dem_slope = np.gradient(dem)
    dem_slope = geolib.gdaldem_slope(hi_sun_dem_fn)
    dem = iolib.fn_getma(hi_sun_dem_fn)
    ##out = np.ma.array(chm_array, mask=np.ma.masked_outside(dem_slope, *slopelim).mask, keep_mask=True, fill_value=-9999)
    ##https://stackoverflow.com/questions/35435015/extending-numpy-mask
    out = np.ma.array(*np.broadcast(chm_array, np.ma.masked_outside(dem_slope, *slopelim).mask), keep_mask=True, fill_value=-9999)
    shutil.rm(os.path.splitext(hi_sun_dem_fn)[0]+'_slope.tif')
    return out


def fit_gaus(masked_array, ras_fn, ncomp, sampleStep):

    # http://stackoverflow.com/questions/10143905/python-two-curve-gaussian-fitting-with-non-linear-least-squares/19182915#19182915
    X_compress = masked_array.compressed()
    X_reshape = np.reshape(X_compress,(masked_array.compressed().size,1))

    clf = mixture.GaussianMixture(n_components=ncomp, covariance_type='full')
    clf.fit(X_reshape)

    ml = clf.means_
    wl = clf.weights_
    cl = clf.covariances_
    ms = [m[0] for m in ml]
    cs = [np.sqrt(c[0][0]) for c in cl]
    ws = [w for w in wl]
    i = 0

    sampleStep_str = "%03d" % (sampleStep)

    histo = matplotlib.pyplot.hist(masked_array.compressed(), 300, normed=True, color='gray', alpha = 0.5)
    fig_name = ras_fn.split('/')[-1].strip('.tif') + "_" + str(ncomp) + "_" + sampleStep_str + '.png' ##'_pks' + str(ncomp) + '_' + 'hist' + str(sampleStep_str) +'.png'

    # Delete out_peaksCSV if exists
    out_dir = os.path.split(ras_fn)[0]
    out_peaksCSV = os.path.join(out_dir,fig_name.strip('.png') +'.csv')

    if os.path.isfile(out_peaksCSV):
        os.remove(out_peaksCSV)

    print"\tOutput gaussian peaks csv: %s" %(out_peaksCSV)

    with open(out_peaksCSV,'w') as outpk:

        # Write hdr if new
        outpk.write('ras_fn,gaus1_mean,gaus1_sd,gaus2_mean,gaus2_sd,gaus3_mean,gaus3_sd\n')
        i = 0
        gauss_num = ''
        outpk.write(ras_fn)                               # Start writing the line
        for w, m, c in zip(ws, ms, cs):
            i += 1
            matplotlib.pyplot.plot(histo[1],w*matplotlib.mlab.normpdf(histo[1],m,np.sqrt(c)), linewidth=3)
            matplotlib.pyplot.axis([-5,15,0,1])

            gauss_num = 'Gaussian peak #%s' %(i)
            print '\t' + gauss_num + ' mean: ', m , ' std dev:',c

            outpk.write(',' + str(m) + ',' + str(c))        # Finish writing the line
            if i == ncomp:
                outpk.write('\n')

        matplotlib.pyplot.savefig(os.path.join(out_dir,fig_name))
        matplotlib.pyplot.clf()

        return(out_peaksCSV)


def get_hist_n(array, ras_fn, ncomp, sampleStep):
    """
    Get a histogram of image by regularly sampling a 'pct' of the input image's pixels
        Provides an even sample from across the entire image without having to analyze the entire array
    Call 'fit_gaus' Fit 3 gaussian peaks to the histogram
    Return and Write out data to out_peaksCSV
    """

    ### Creating data range
    masked_array = np.ma.masked_less_equal(array,-99)    # mask all values inside this interval
    masked_array = np.ma.masked_invalid(masked_array)              # mask all nan and inf values

    # Numpy slicing to sample image for histogram generation
    # Get size
    nrow,ncol = masked_array.shape
    print '\n\tRaster histogram: sampling & estimating gaussian peaks'
    print '\tArray dims: ' + str(nrow) + " , " + str(ncol)

    # [start:stop:step]
    print '\tSampling the rows, cols with sample step: %s' %(sampleStep)
    masked_array = masked_array[0::sampleStep,0::sampleStep]
    sz = masked_array.size
    print '\tNum. elements in NEW sampled array: %s' %(sz)

    print "\t: min, max, med, mean, std"
    print "\t:",masked_array.min(),masked_array.max(),np.ma.median(masked_array),masked_array.mean(),masked_array.std()

    if masked_array.compressed().size > 1:

        print '\n\tFitting gaussian peaks...'
        ## https://stackoverflow.com/questions/10143905/python-two-curve-gaussian-fitting-with-non-linear-least-squares
        outpeaksCSV = fit_gaus(masked_array, ras_fn, ncomp, sampleStep)

        return(outpeaksCSV)

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
    parser = argparse.ArgumentParser(description='Correct CHM pixel values according to correction based on guassian peak analysis of minimum (ground) peak in CHM image histogram')
    parser.add_argument('ras_fn', type=str, help='Raster filename')
    parser.add_argument('-out_name', type=str, default=None, help='Output raster filename')
    parser.add_argument('-pre_min', type=int, default=-15, help='min value (m) of pre-corrected range')
    parser.add_argument('-pre_max', type=int, default=30, help='max value (m) of pre-corrected range')
    parser.add_argument('-n_gaus', type=int, default=3, help='histogram sampling: num of gaussian peaks to fit to sampled histogram')
    parser.add_argument('-shift', type=int, default=0, help='num of std devs subtracted from the min (ground) gaussian peak to identify a CHM of 0')
    parser.add_argument('-sample_step', type=int, default=50, help='histogram sampling: sample every nth pixel of the input image to create the histogram to which the gaussians are fit')
    return parser

def main():
    parser = getparser()
    args = parser.parse_args()

    ras_fn = args.ras_fn
    out_name = args.out_name
    pre_min = args.pre_min
    pre_max = args.pre_max
    n_gaus = args.n_gaus
    sample_step = args.sample_step
    ##ht_thresh = args.ht_thresh
    stddev_shift = args.shift

    driverTiff = gdal.GetDriverByName('GTiff')
    print '\n\tCHM Correction'
    print '\tRaster name: %s' %ras_fn

    # [4] Read in raster as a masked array
    array = iolib.fn_getma(ras_fn, bnum=1)
    array = array.astype(np.float32)

    # TODO: fix Slope filter
    # Get hi-sun elev warp-trans-ref-DEM
    #tail_str = "-DEM_warp-trans_reference-DEM"
    #chm_dir, chm_pairname = os.path.split(ras_fn)       # eg chm_pairname WV02_20130804_1030010024808A00_1030010025118000-DEM_warp-trans_reference-DEM_WV01_20150726_1020010043A37200_1020010040698700-DEM_warp-trans_reference-DEM_dz_eul.tif
    #main_dir = os.path.split(chm_dir)[0]
    #diff_pairs = chm_pairname.replace(tail_str,"").replace("_dz_eul.tif","")
    #hi_sun_dem_fn = os.path.join(main_dir,diff_pairs,chm_pairname.split(tail_str)[0] + "-DEM_warp_align",chm_pairname.split(tail_str)[0] + tail_str + ".tif")

    #array = slope_fltr_chm(array, hi_sun_dem_fn)

    # TODO: incidence angle correction of heights

    #Absolute range filter
    # returns a masked array...
    array = filtlib.range_fltr(array, (pre_min, pre_max))

    # Get gaussian peaks
    out_gaus_csv = get_hist_n(array, ras_fn, n_gaus, sample_step)

    with open(out_gaus_csv,'r') as peaksCSV:
        """Create a canopy height model
            Shift the values of the dz raster based on the ground peak identified in the histogram
                Read in CSV of gaussian peaks computed from the dz raster
                Apply a shift based on the minimum peak and the stddev_shift
                Returns a tif of canopy heights.
        """
        hdr = peaksCSV.readline()
        line = peaksCSV.readline()

    # Get raster diff dsm name
    ras_fn    = line.split(',')[0]

    # Get the min of the means: represents the the offset value that will be subtracted from each pixel of the corresonding diff_dsm
    gmeans  = map(float, line.split(',')[1::2])

    # Find the min of the gaussian peak means
    gmin    = min(gmeans)

    # Get corresponding sd
    idx     = line.split(',').index(str(gmin)) + 1
    gsd     = float(line.split(',')[idx])

    ##array = np.where(array <= -99, np.nan, array)
    gsd_str = "%04d" % (round(gsd,2)*100)
    print '\n\tApply CHM gaussian correction:'
    print '\tHeight of the gound peak (m) (gaussian min):  %s' % gmin
    print '\tEstimated height uncertainty (m) (gaussian std dev):  %s' % gsd
    print '\tNumber of std devs used in calculating shift:  %s' % stddev_shift

    shift_val = float(np.subtract(gmin, (stddev_shift * gsd) ) )

    print "\t: Final CHM correction value (shift) (m) %s" % shift_val
    array = np.subtract(array, shift_val)

    print '\n\tApply masking...'

    print '\t\tConvert values below 0'
    # Better handling of negative values?
    #   1. take abs value of all negative values?
    #   2. take abs value of all negative values within 1 stddev of ground peak; all the rest convert to 0
    array = np.ma.where(array < (0 - 6 * gsd) , 0, abs(array))

##            print "\t:TEST--"
##            print "\t: min, max, med, mean, std"
##            print "\t:",array.min(),array.max(),np.median(array),array.mean(),array.std()
    fn_tail = '_chm_'+gsd_str+'.tif'
    if out_name is not None:
        chm_fn = os.path.join(os.path.split(ras_fn)[0], out_name + fn_tail)
    else:
        chm_fn = os.path.join(ras_fn.split('.tif')[0] + fn_tail)

    # Write array to dataset
    print "\n\t----------------------"
    print "\n\tMaking CHM GeoTiff: ", chm_fn

    iolib.writeGTiff(array, chm_fn, iolib.fn_getds(ras_fn), ndv=-99)

    cmdStr ="gdaladdo -ro -r average " + chm_fn + " 2 4 8 16 32 64"
    run_os(cmdStr)

    # Append to a dir level CSV file that holds the uncertainty info for each CHM (gmin, gsd, stddev_shift)
    out_dir = os.path.split(ras_fn)[0]
    out_stats_csv = out_dir + '_stats.csv'

    if not os.path.exists(out_stats_csv):
        writetype = 'wb'    # write file if not yet existing
    else:
        writetype = 'ab'    # append line if exists

    with open(out_stats_csv, writetype) as out_stats:
        wr = csv.writer(out_stats, delimiter =",")
        if writetype == 'wb':
            wr.writerow(["chm_name", "ground_peak_mean_m", "ground_peak_stdev_m", "num_stdevs_shift", "final_chm_peak_shift_m"])     # if new file, write header
        wr.writerow([os.path.split(chm_fn)[1] , str(round(gmin,2)) , str(round(gsd,2)) , str(round(stddev_shift,2)) , str(round(shift_val,2))])

if __name__ == '__main__':
    main()
