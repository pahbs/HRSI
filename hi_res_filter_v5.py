#-------------------------------------------------------------------------------
# Name:        hi_res_filter_v5
# Purpose:
#
# Author:      pmontesa
#
# Created:     03/09/2015
# Copyright:   (c) pmontesa 2015
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import os, osgeo, Image, matplotlib, matplotlib.pyplot as plt, numpy as np
from osgeo import gdal, osr
from osgeo.gdalnumeric import *
from osgeo.gdalconst import *
import scipy.ndimage.filters as filt
import diffRasters_v2 as dif
import workflow_functions as wf
import numpy as np
from scipy.signal import fftconvolve

def gaussian_blur(in_array, size):
    ## https://gis.stackexchange.com/questions/9431/what-raster-smoothing-generalization-tools-are-available
    # http://gis.stackexchange.com/a/10467
    # expand in_array to fit edge of kernel
    padded_array = np.pad(in_array, size, 'symmetric')

    # build kernel
    x, y = np.mgrid[-size:size + 1, -size:size + 1]
    g = np.exp(-(x**2 / float(size) + y**2 / float(size)))
    g = (g / g.sum()).astype(in_array.dtype)

    # do the Gaussian blur
    return fftconvolve(padded_array, g, mode='valid')

def main(hiResDSM, kernelSize, rFact, readDSM=False, ht=False, mask_less_than_val=-15):
    """
    readDSM=False when you dont want the DSM filtered..you just want simple slope and aspect from gdaldem

    ; Input datasets:
    ; 	DSM
    ;
    ; Output:
    ; 	DTM --> Digital Terrain Model     :: ground surface elevation (m), vegetation removed
    ;	SR  --> Surface Roughness         :: heights (m) of surface features (ability to get top of veg canopy depends on how well these features are resolved in the DSM); useful for for sparse forest / vegetation; uses DSM.
    ; 	COV --> Cover of Surface Features :: binary showing cover of surface features above (1) and below (0) a height threshold (2m) applied to the SR layer (note: values < 0m and > 20m are set to 0)
    ;
    ; This output should be coupled with multi-spectral derived vegetation extent info (NDVI)
    ; The CSR layer may serve as a proxy for vegetation density in sparse forests (designed for taig-tundra ecotone)
    ;
    ; Summary:
    ; 	Estimate ground surface elevation to produce a digital terrain model (DTM).
    ; 	Rescale (coarsen) data by a factor of rFact to get a more managable layer used to get the DTM
    ; 	Apply a low-pass filter on this rescaled layer to smooth the elevation.
    ;	  (Improve this script by using a custom low pass filter: example here: http://www.exelisvis.com/docs/LowPassFilter.html)
    ; 	The filtering removes hi-frequency noise (trees and other landscape objects)
    ; 	First the input data is RESIZED, removing a few lines and sample to create an array that is a multiple of the resize factor.
    ; 	The filtering is done on a resized data set that is essentially a lower res sample of the original data.
    ; 	This was done:
    ; 		a. to reduce processing
    ; 		b. b/c ground surface is only visible in some of the image anyway.
    ; 	This provides an estimate of ground (a DTM).
    ; 	Subtract DTM from the DSM to get the height above this DTM of surface features that are resolved in the hi-res DSM
    ;
    ;	Result:
    ;	*_DTM.tif
    ;	*_SR.tif
    ;	*_COV.tif
    ;
    ;
    ; Help
    ; http://www.exelisvis.com/docs/CONV_DOIT.html
    """

    path, fname = os.path.split(hiResDSM)
    fname_stem = fname.split('.')[0]

    # - Set up the output file name stems
    out_fname_stem  = os.path.join(path,fname_stem + '_' + str(kernelSize) + '_' + str(rFact))
    outNameSlope    = os.path.join(out_fname_stem + '_SLP.tif')		# SLOPE now output via GDAL os cmd instead of a numpy array operation
    outNameAspect   = os.path.join(out_fname_stem + '_ASP.tif')
    outNameDTM      = os.path.join(out_fname_stem + '_DTM.tif')          	# Output DTM (m)
    outName         = os.path.join(out_fname_stem + '_SR.tif')            # Output Surface Roughness (m)
    outNameCov      = os.path.join(out_fname_stem + '_COV.tif')    	    # Output Cover of surface features (binary)

    print('\n\t[1] Reading in hi-res DSM file...: %s' %(hiResDSM))
    # Original input dsm: Need pixel size
    orig_ds = gdal.Open(hiResDSM)
    orig_gt = orig_ds.GetGeoTransform()
    ps = orig_gt[1]                         # pixel size
    print"\n\tInput pixel size: %s" %(ps)

    print('\n\t[2] Re-scale (gdalwarp uses nearest neighbor by default) to a lower res VRT')
    # Resample to lower res: This reduces teh size of the array that is read into memory
    res = int(round(ps * rFact,0))
    print"\n\tre-sized pixel size: %s" %(res)
    vrtExt = "_" + str(res) + "m.vrt"

    # Read in UTM projection info from ds and put into -t_srs below
    srs = osr.SpatialReference(wkt=orig_ds.GetProjection())
    zone = srs.GetAttrValue('projcs').split('zone ')[1][:-1]        # removes the last char, which is 'N' or 'S'
    cmdStr = "gdalwarp -of VRT -tr " + str(res) + " " + str(res) + " -t_srs EPSG:326" + zone + " " + hiResDSM + " " + hiResDSM.strip('.tif') + vrtExt
    dif.run_wait_os(cmdStr)

    print '\n\tNow read in re-sized VRT on which script will operate'
    if readDSM:
        in_ds = gdal.Open(hiResDSM.strip('.tif') + vrtExt)
        gt = in_ds.GetGeoTransform()
        bnd_arr = in_ds.GetRasterBand(1).ReadAsArray()

        # Assign NaN values to areas with holes
        # (these 'holes' are might be assigned a variety of negative values)
        dsm = np.ma.masked_less(bnd_arr,mask_less_than_val) # mask all values less than -15m
        dims = dsm.shape      # this is a numpy array, with dimensions [row, col]
        input_dims = dims
        print ('----> Original dims:')
        print ('----> input_dims (row,col) = ' + str(input_dims[0]) + " , " + str(input_dims[1]))
        ns = input_dims[1]      # num samples means num cols
        nl = input_dims[0]

        #+++++++++++++DTM++++++++++++++++

        print("[3] Get DTM: Find a local min in window")
        ##----- G.Sun modification starts
        print("[3.1.1] From DSM, find minimum in window kernelSize x kernelSize")
        filt_dtm = filt.minimum_filter(dsm,kernelSize)

    print("[3.1.2] From DSM, get slope and aspect at ~ 10m res")

    # The Numpy approach...
    ## http://geoexamples.blogspot.com/2014/03/shaded-relief-images-using-gdal-python.html
    #x, y = np.gradient(dsm)
    #deg2rad = 3.141592653589793 / 180.0
    #rad2deg = 180.0 / 3.141592653589793
    #slope = np.arctan(sqrt(x*x + y*y)) * rad2deg


    # The GDAL approach...
    # first coarsen to ~10m
    cmdStr = "gdal_translate -of VRT -outsize 5% 5% " + hiResDSM + ' ' + hiResDSM.split('.')[0] + '_10m.vrt'
    wf.run_wait_os(cmdStr)

    if not os.path.isfile(outNameSlope):
        cmdStr = "gdaldem slope " + hiResDSM.split('.')[0] + '_10m.vrt' + ' ' + outNameSlope
        wf.run_wait_os(cmdStr)
    if not os.path.isfile(outNameAspect):
        cmdStr = "gdaldem aspect -zero_for_flat " + hiResDSM.split('.')[0] + '_10m.vrt' + ' ' + outNameAspect
        wf.run_wait_os(cmdStr)

    ##print"[3.2] Re-sample (bilinear interpolation) the smaller minimum dsm back to the dims of the input..."
    ##filt_dtm = CONGRID(dsmmin, nsn, nln, /INTERP)
    ##----- G.Sun modification ends
    if ht:
        #+++++++++CSR+++++++++++++++++++++++
        print '[4] Compute surface roughness with feature height'

        # TTE specific
        # Do Band Math to subtract filtered image from original to get 'surface feature height'
        height_sr = 10 * (dsm - filt_dtm)	#scale by a factor of 10

        # TTE specific
        # Less than 0 or greater than 200 (20m), recode to 0
        ##height_sr = height_sr[np.where((height_sr < 0) or (height_sr > 20), 0, height_sr)]
        height_sr = np.where(np.logical_or((height_sr < 0) , (height_sr > 20)), 0, height_sr)

        # TTE specific
        # Cast to an integer value
        ##height_sr = FIX(height_sr)

        #+++++++++COVER+++++++++++++++++++++++
        # based on surface feature height above a threshold

        # TTE specific
        thresh = 2.0   #Threshold in m for determining pixels representing feature cover of the ground surface

        print '[5] Create COVER binary mask based on height threshold(m): %s' %(thresh)
        cover = height_sr
        cover = np.where(height_sr > thresh, 1, 0)

    #+++++++++++OUTPUT+++++++++++++++++++++
    # Write out Tifs
    if readDSM:
        if ht:
            outputs = [outNameDTM, outName, outNameCov]
            outarrs = [filt_dtm, height_sr, cover]
        else:
            outputs = [outNameDTM]
            outarrs = [filt_dtm]

        for n,output in enumerate(outputs):
            print '[6.0] Writing output'
            print '\n\tPixel-size: %s' %(res)
            print "\n\t- Making GeoTiff: ", output
            ## Output code here:
            driverTiff = gdal.GetDriverByName('GTiff')
            ds = driverTiff.Create(output, in_ds.GetRasterBand(1).XSize, in_ds.GetRasterBand(1).YSize, 1, gdal.GDT_Float32)
            ds.SetGeoTransform(in_ds.GetGeoTransform())
            ds.SetProjection(in_ds.GetProjection())
            band = ds.GetRasterBand(1)
            band.WriteArray(outarrs[n])
            ## Cleanup code here:
            band.FlushCache()
            del filt_dtm
            filt_dtm = None
            #del slope
            #slope = None

            if ht:
                del height_sr
                height_sr = None
                del cover
                cover = None

    return outNameDTM, outNameSlope, outNameAspect

if __name__ == '__main__':
    import sys
    main(str(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]))
