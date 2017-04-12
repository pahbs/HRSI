import os, osgeo, matplotlib, matplotlib.pyplot, matplotlib.mlab, numpy as np, subprocess as subp, math
from osgeo import gdal
from sklearn import mixture
import pylab as P

def sub_VHR(in_VHR_fn,out_VHR_fn,sub_radius,out_file):
    '''# reformat VHR data
    temp_VHR_tif_fn = in_VHR_fn.strip('ntf').strip('NTF').strip('TIF').strip('tif')+"tif"
    if not os.path.exists(temp_VHR_tif_fn):
        GDAL_VHR_1 = "gdalwarp -of GTiff "+in_VHR_fn+" "+temp_VHR_tif_fn
        result = os.system(GDAL_VHR_1)
        if result != 0:
            print '*** error --> ',GDAL_VHR_1
    '''

    # find VHR centroid
    #VHR_ds = gdal.Open(temp_VHR_tif_fn)
    gdal.SetConfigOption('NITF_OPEN_UNDERLYING_DS', 'NO')
    VHR_ds = gdal.Open(in_VHR_fn)
    VHR_gt = VHR_ds.GetGeoTransform()
    x_centroid_UTM = VHR_gt[0] + 0.5 * VHR_gt[1] * VHR_ds.RasterXSize
    y_centroid_UTM = VHR_gt[3] + 0.5 * VHR_gt[5] * VHR_ds.RasterYSize
    del VHR_ds
    VHR_ds = None

    # subset image at VHR centroid
    if not os.path.exists(out_VHR_fn):
        GDAL_cmd = "gdalwarp -te " +  str(float(x_centroid_UTM) - sub_radius) + " " + \
                                        str(float(y_centroid_UTM) - sub_radius) + " " + \
                                        str(float(x_centroid_UTM) + sub_radius) + " " + \
                                        str(float(y_centroid_UTM) + sub_radius) + " " + \
                                        in_VHR_fn + " " + out_VHR_fn
        cmd = subp.Popen(GDAL_cmd.rstrip('\n'), stdout=subp.PIPE, shell=True)
        stdOut, err = cmd.communicate()
        print(str(stdOut) + str(err))

    #os.remove(temp_VHR_tif_fn)
    out_file.write(in_VHR_fn + ',' + out_VHR_fn + ',' + GDAL_cmd+'\n')

def fit_GMM(data_array, plotdir, inRaster, ncomp, sampleStep, tail):
    # http://stackoverflow.com/questions/10143905/python-two-curve-gaussian-fitting-with-non-linear-least-squares/19182915#19182915
    X_compress = data_array.compressed()
    X_reshape = np.reshape(X_compress,(data_array.compressed().size,1))

    clf = mixture.GMM(n_components=ncomp, covariance_type='full')
    clf.fit(X_reshape)

    ml = clf.means_
    wl = clf.weights_
    cl = clf.covars_
    ms = [m[0] for m in ml]
    cs = [np.sqrt(c[0][0]) for c in cl]
    ws = [w for w in wl]
    i = 0

    sampleStep_str = "%03d" % (sampleStep)

    histo = matplotlib.pyplot.hist(data_array.compressed(), 300, normed=True, color='gray', alpha = 0.5)
    fig_name = inRaster.split('/')[-1].strip('.tif') + tail + '.png' ##'_pks' + str(ncomp) + '_' + 'hist' + str(sampleStep_str) +'.png'

    # Delete out_peaksCSV if exists
    out_peaksCSV = os.path.join(plotdir,fig_name.strip('.png') +'.csv')

    if os.path.isfile(out_peaksCSV):
        os.remove(out_peaksCSV)

    print"\n\tOutput gaussian peaks csv: %s" %(out_peaksCSV)

    with open(out_peaksCSV,'w') as outpk:

        # Write hdr if new
        outpk.write('file,gaus1_mean,gaus1_sd,gaus2_mean,gaus2_sd,gaus3_mean,gaus3_sd\n')
        i = 0
        guass_num = ''
        outpk.write(inRaster)                               # Start writing the line
        for w, m, c in zip(ws, ms, cs):
            i += 1
            matplotlib.pyplot.plot(histo[1],w*matplotlib.mlab.normpdf(histo[1],m,np.sqrt(c)), linewidth=3)
            matplotlib.pyplot.axis([-5,15,0,1])
            guass_num = 'guass %s' %(i)
            print '\n\t' + guass_num + ' mean: ', m , ' std dev:',c
            ##outpk.write(fig_name + ',' + guass_num + ',' + str(m) + ',' + str(c) +'\n')
            outpk.write(',' + str(m) + ',' + str(c))        # Finish writing the line
            if i == ncomp:
                outpk.write('\n')

        ##fig_name = inRaster.split('/')[-1] + '_peaks' + str(ncomp) + '_hist.png'
        matplotlib.pyplot.savefig(os.path.join(plotdir,fig_name))
        matplotlib.pyplot.clf()

        return(out_peaksCSV)


def get_hist_n(plotdir, inRaster, ncomp, sampleStep, tail):
    """
    Get a histogram of image by regularly sampling a 'pct' of the input image's pixels
        Provides an even sample from across the entire image without having to analyze the entire array
    Call 'fit_GMM' Fit 3 gaussian peaks to the histogram
    Return and Write out data to out_peaksCSV
    """
    # Sample original image at every nth pixel to create a histogram
    gdal.SetConfigOption('NITF_OPEN_UNDERLYING_DS', 'NO')
    sub_ds = gdal.Open(inRaster)
    sub_band = sub_ds.GetRasterBand(1)
    sub_array = sub_band.ReadAsArray()

    # Creating data range
    masked_array = np.ma.masked_outside(sub_array,-20,40)   # mask all values outside this interval
    masked_array = np.ma.masked_invalid(masked_array)       # mask all nan and inf values

    # Numpy slicing to sample image for histogram generation
    # Get size
    nrow,ncol = masked_array.shape
    print '\n\tArray dims: ' + str(nrow) + " , " + str(ncol)

    # [start:stop:step]
    print '\n\tSampling the rows, cols with sample step: %s' %(sampleStep)
    masked_array = masked_array[0::sampleStep,0::sampleStep]
    sz = masked_array.size
    print '\n\tNum. elements in NEW sampled array: %s' %(sz)
    ##print (masked_array[1000:1004,1000:1004])

    print inRaster
    print ">>\n\t: min, max, med, mean, std"
    print ">>\n\t:",masked_array.min(),masked_array.max(),np.ma.median(masked_array),masked_array.mean(),masked_array.std()

    if masked_array.compressed().size > 1:

        # Fit gaussian peaks
        ## https://stackoverflow.com/questions/10143905/python-two-curve-gaussian-fitting-with-non-linear-least-squares
        outpeaksCSV = fit_GMM(masked_array, plotdir, inRaster, ncomp, sampleStep, tail)

        return(outpeaksCSV)

def main(image_dir,searchStr, ncomp, sampleStep, tail, doRasSub=False):      ##WV01_20100819_102001000E7B0000_102001000F95F800/VHRhist_in/'
    """
    Get raster subset and histogram subset:
        Search for images in a top 'image_dir' with 'searchStr' and convert to VRT
        If doRasSub: create a subset at the image center that is 'sub_radius' X 'sub_radius'
        Call 'get_hist_n' to generate a subset histogram based on every nth (sampleStep) row and col
    """
    print ("\n\tStarting rhs...")
    ##sub_radius= 2000 # meters

    for root, dirs, files in os.walk(image_dir):
        ##if root == image_dir: # constrain search to image_dir
            for each in files:
                ##print each
                # Looking for the tifs dropped here from diffRasters
                if (searchStr in each and each.endswith(('.ntf','.tif')) and not '_sub' in each ):

                    print os.path.join(root,each),">>"

                    # Setup out dirs and filenames
                    ##out_dir = os.path.join(image_dir,'do_dsm_chm')                              ## dir
                    out_csv = os.path.join(image_dir,'sub_hist_list.csv')                        ## csv of input and output


                    if not os.path.isdir(image_dir):
                        os.mkdir(image_dir)

                    # Convert to VRT: put in out dir
                    cmdStr = "gdal_translate -of VRT " + os.path.join(root,each) + " " + os.path.join(image_dir,each).split('.')[0] + ".vrt"
                    cmd = subp.Popen(cmdStr.strip('\n'), stdout=subp.PIPE, shell=True)
                    stdOut, err = cmd.communicate()
                    print(str(stdOut) + str(err))

                    # Set the subset name
                    sub_fn = os.path.join(image_dir,each.strip('.ntf').strip('.NTF').strip('.TIF').strip('.tif').strip('.vrt')+'_sub.tif')

                    # Do the raster subset
                    if doRasSub:
                        if os.path.exists(sub_fn):
                            print "\n\t",each[:-4],"already made"
                            ##continue
                        else:
                            with open(out_csv,'w') as out_file:

                                #write out a header
                                out_file.write('in_data,out_data,GDAL_cmd\n')

                                print("\n\tCreating subset raster...")
                                sub_VHR(os.path.join(image_dir, each.split('.')[0] + '.vrt'), sub_fn, sub_radius, out_file)
                                print "\n\t<<",sub_fn,"created"

                    # Do the histogram subset
                    print("\n\tMaking histogram...")
                    ##get_hist(image_dir, sub_fn)
                    # Sample every_nth pixel of original input
                    get_hist_n(image_dir, os.path.join(root,each),ncomp,sampleStep, tail)

if __name__ == "__main__":
    import sys
    main(str(sys.argv[1]),str(sys.argv[2]), int(sys.argv[3]),float(sys.argv[4]), str(sys.argv[5]))