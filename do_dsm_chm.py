#-------------------------------------------------------------------------------
# Name:        do_dsm_chm
# Purpose:
#
#               Uses overlapping (2) DSMs
#                   Performs gaussian correction to both input canopy and ground DSMs
#                   Get CHMs by diff'ing overlapping HRSI DSMs in the TTE
#
#               Analyze histogram of CHM and apply correction based on the minimum gaussian peak
#
# Author:      pmontesa
#
# Created:     29/09/2015
# Copyright:   (c) pmontesa 2015
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import os, osgeo, numpy as np
from osgeo.gdalnumeric import *
from osgeo.gdalconst import *
from osgeo import gdal, osr, ogr
## My scripts
import diffRasters_v2 as dif
import hist_subsets
import demcoreg
from distutils.util import strtobool
"""
FUNCTION
"""
def gaus_correct(inRaster, corVal, res, overwrite):
    """
    Apply a gaussian-based histogram correction to each DSM
    Return --> pathname of cor DEM file
    """
    overwrite = bool(strtobool(overwrite))
    driverTiff = gdal.GetDriverByName('GTiff')

    # Read in raster
    gdal.SetConfigOption('NITF_OPEN_UNDERLYING_DS', 'NO')
    ds = gdal.Open(inRaster)
    band = ds.GetRasterBand(1)
    #array = band.ReadAsArray()

    block_sizes = band.GetBlockSize()
    x_block_size = block_sizes[0]
    y_block_size = block_sizes[1]

    xsize = band.XSize
    ysize = band.YSize

    dsm_cor = inRaster.replace('DEM.tif','DEM_cor.tif')

    if not os.path.isfile(dsm_cor) or (os.path.isfile(dsm_cor) and overwrite):


        cor_ds = driverTiff.Create(dsm_cor, ds.RasterXSize, ds.RasterYSize, 1, band.DataType)       ##gdal.GDT_Int32
        cor_ds.SetGeoTransform(ds.GetGeoTransform())
        cor_ds.SetProjection(ds.GetProjection())

        print "\n\tLooping over blocks, applying DSM correction..."
        for i in range(0, ysize, y_block_size):

            if i + y_block_size < ysize:
                rows = y_block_size
            else:
                rows = ysize - i
            for j in range(0, xsize, x_block_size):
                if j + x_block_size < xsize:
                    cols = x_block_size
                else:
                    cols = xsize - j

                # Numpy Math
                array = band.ReadAsArray(j, i, cols, rows)
                array[array == -99]=np.nan

                array_cor = np.where((array != np.nan), np.subtract(array, corVal), np.nan)
                array = None

                # If you want to sen nan back to -99
                ##array_cor = np.where(np.isnan(array_cor),-99, array_cor)

                cor_ds.GetRasterBand(1).WriteArray(array_cor,j,i)

        # Write array to dataset
        print "\n\t----------------------"
        print "\n\t- Wrote GeoTiff of corrected DSM: ", dsm_cor

    return(dsm_cor)

    array_cor = None
    band = None
    del cor_ds
    cor_ds = None
    del ds
    ds = None

def chm_correct(outDir, out_peaksCSV, res, tail, ht_thresh):
    driverTiff = gdal.GetDriverByName('GTiff')

    with open(os.path.join(outDir, out_peaksCSV),'r') as peaksCSV:
        """Create a canopy height model
            Shift the values of the diff raster based on the ground peak identified in the histogram
                Read in CSV of gaussian peaks computed from the differenced raster
                Apply a shift based on the minimum peak and 2x its standard deviation
                Returns a GeoTiff of canopy heights.
        """
        hdr = peaksCSV.readline()
        lines = peaksCSV.readlines()

        for line in lines:

            # Get raster diff dsm name
            name    = line.split(',')[0]

            # Get the min of the means: represents the the offset value that will be subtracted from each pixel of the corresonding diff_dsm
            gmeans  = map(float, line.split(',')[1::2])

            # Find the min of the gaussian peak means
            gmin    = min(gmeans)

            # Get corresponding sd
            idx     = line.split(',').index(str(gmin)) + 1
            gsd     = float(line.split(',')[idx])

            # [4] Read in raster
            inRaster  = os.path.join(outDir, name)
            gdal.SetConfigOption('NITF_OPEN_UNDERLYING_DS', 'NO')
            ds = gdal.Open(inRaster)
            band = ds.GetRasterBand(1)
            array = band.ReadAsArray()

            # Creating data range
            masked_array = np.ma.masked_outside(array,-20,40)       # mask all values outside this interval
            masked_array = np.ma.masked_invalid(masked_array)       # mask all nan and inf values

            # Apply CHM gaussian correction
            masked_array_offset = np.subtract(masked_array, float(np.subtract(gmin, (2 * gsd) ) ) )

            # Apply nan to areas beyond threshold
            # This makes the data range -20.0 - 20.0 meters
            masked_array_offset[np.fabs(masked_array_offset) >= ht_thresh] = np.nan
            res_str = "%03d" % (res)
            ##chm_Fn = os.path.join(outDir, name.split('.tif')[0] + "_" + res_str + 'm_chm.tif')
            chm_Fn = os.path.join(outDir, name.split('.tif')[0] + tail + '_chm.tif')

            # Write array to dataset
            print "\n\t----------------------"
            print "\n\t- Making Offset (CHM) GeoTiff: ", chm_Fn
            chm_ds = driverTiff.Create(chm_Fn, ds.RasterXSize, ds.RasterYSize, 1, band.DataType)
            chm_ds.SetGeoTransform(ds.GetGeoTransform())
            chm_ds.SetProjection(ds.GetProjection())                # set the projection according to inRaster
            chm_band = chm_ds.GetRasterBand(1)
            chm_band.WriteArray(masked_array_offset)
            chm_band.FlushCache()

            cmdStr ="gdaladdo -r average " + chm_Fn + " 2 4 8 16"
            dif.run_os(cmdStr)
            ##cmdStr ="gdalbuildvrt -r average " + chm_Fn + " 2 4 8 16"
            ##dif.run_os(cmdStr)

            band = None
            del chm_ds
            chm_ds = None
            del ds
            ds = None

def main(outDir, cRasList, gRasList, corFile, cfField, applyCor, applyDif):

    """
    Canopy Height Model

    Args:
        outDir                  full path to the CHM output directory
        cRasList, gRasList      corresponding lists of DEMs to be used as ground (gRas) and canopy (cRas) surface elev estimates; eg, ['/full/path/out-DEM.tif']
        corFile                 the csv file with a field indicating the pairname (eg, WV01_20130708_1020010022283300_10200100239A2100) and a field holding the correction factor
        cfField                 the correction factor fieldname in the corFile csv
        applyCor                boolean; apply DSM correction
        applyDif                boolean; apply DSM difference
    """

    # Processing particulars
    res = 2                                             ## target res of diff_dsm raster in meters
    ncomp = 3                                           ## num of gaussian peaks to fit to sampled histogram
    sampleStep = 50                                     ## histogram sampling: every nth pixel of the image will create the histogram to before fitting gaussians
    rFact = 1                                           ## reduce res of input ground DSM by this fact to filter and get DTM; larger = faster, coarser, greater ground peak SD
    kernelSize = 25
    ht_thresh = 20                                      ## perform local min filter on gRas with this size

    res_str = "%03d" % (res)
    sampleStep_str = "%03d" % (sampleStep)
    rFact_str = "%02d" % (rFact)

    applyCor=bool(strtobool(applyCor))
    applyDif=bool(strtobool(applyDif))

    # Set up 'tail' string of output _CHM.tif showing 'Processing particulars'
    tail = '_pks' + str(ncomp) + '_' + 'hist' + sampleStep_str + '_rFact' + rFact_str

    # Loop through list
    for num, cRas in enumerate(cRasList):

        # Get corresponding ground elevation raster
        gRas = gRasList[num]

        if applyCor:
            # [1] Output from R: gaussian peaks and st devs
            # Get canopy and ground DSM names as they appear in output R CSV file
            outASPdir, DSMc_name = os.path.split(os.path.split(cRas)[0])
            outASPdir, DSMg_name = os.path.split(os.path.split(gRas)[0])
            print 'DSMc name: %s' %(DSMc_name)
            print 'DSMg name: %s' %(DSMg_name)

            # Get cor values of cor_dif_gaus for each DSM to be applied to array
            with open(corFile,'r') as corFyle:
                hdr = corFyle.readline().rstrip().split(',')

                idx_cf = hdr.index(cfField)

                lines = corFyle.readlines()
                for line in lines:
                    line = line.strip('\r\n')

                    # Get DSM name and correction
                    if str(DSMc_name) in line:
                        cor_DSMc = float(line.split(',')[idx_cf])
                        print "Cor value for DSMc: %s" %(cor_DSMc)
                    if str(DSMg_name) in line:
                        cor_DSMg = float(line.split(',')[idx_cf])
                        print "Cor value for DSMg: %s" %(cor_DSMg)

            # [2] Apply gaussian correction to both canopy and ground DSMs
            cRas_cor = gaus_correct(cRas, cor_DSMc, res, 'true')
            gRas_cor = gaus_correct(gRas, cor_DSMg, res, 'true')
        else:
            cRas_cor = cRas
            gRas_cor = gRas

        if applyDif:
            # [2] Call diffRasters
            diff_dsm_name = dif.diffRasters(outDir, cRas_cor, gRas_cor, res, kernelSize, ht_thresh, doFilt=False, isDSM=True, overwrite=True)
        else:
            # call compute_dz.py
            ##diff_dsm_name = demcoreg.compute_dz(gRas_cor, cRas_cor)
            diff_dsm_name = "/att/gpfsfs/briskfs01/ppl/pmontesa/test/test-out-DEM_10pct_warp-trans_reference-DEM_dz_eul.tif"

        # [3] From diff DSM, compute gaussians from histograms
        ##name = os.path.basename(diff_dsm_name)
        outPeakCSV = hist_subsets.get_hist_n(outDir, diff_dsm_name, ncomp, sampleStep, tail)
        ##rhs.run_hist_subsets(outDir, name, ncomp, sampleStep, tail)

        # [4] Do chm correction : look in CSV file to get raster & its min guassian peak mean
        #       then apply to raster to output a *_chm.tif
        chm_correct(outDir, outPeakCSV, res, tail, ht_thresh)


if __name__ == '__main__':
    import sys
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7])
