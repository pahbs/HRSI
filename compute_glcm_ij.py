import os
import skimage.feature
try:
    import skimage.filter
except Exception, e:
    import skimage.filters

from skimage.feature import greycomatrix, greycoprops
from skimage import img_as_ubyte
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
from sklearn import mixture
import numpy as np
from pygeotools.lib import iolib
import argparse
from timeit import default_timer as timer
from time import gmtime, strftime

## GLCM Code Reference:
## https://geoinformaticstutorial.blogspot.com/2016/02/creating-texture-image-with-glcm-co.html
## http://scikit-image.org/docs/dev/api/skimage.feature.html#skimage.feature.greycoprops

## GLCM Tutorial
## http://www.fp.ucalgary.ca/mhallbey/tutorial.htm

## Forest structure with GLCM refs
## Kayitakire et al. 2006   http://www.sciencedirect.com/science/article/pii/S0034425706000988
## Ozdemnir et al. 2011     http://www.sciencedirect.com/science/article/pii/S0303243411000638
## Wood et al. 2012         http://www.sciencedirect.com/science/article/pii/S0034425712000156

## GLCM Texture Properties Calcs from skimage
## http://scikit-image.org/docs/dev/api/skimage.feature.html#greycoprops

def find_elapsed_time(start, end): # take two timer() objects and find elapsed time between them
    elapsed_min = (end-start)/60
    return float(elapsed_min)

def fit_GMM(data_array, plotdir, inRaster, ncomp, tail):
    # http://stackoverflow.com/questions/10143905/python-two-curve-gaussian-fitting-with-non-linear-least-squares/19182915#19182915
    arr_flat = data_array.flatten()
    arr_reshape = np.reshape(arr_flat,(arr_flat.size,1))

    clf = mixture.GMM(n_components=ncomp, covariance_type='full')
    clf.fit(arr_reshape)

    ml = clf.means_
    wl = clf.weights_
    cl = clf.covars_
    ms = [m[0] for m in ml]
    cs = [np.sqrt(c[0][0]) for c in cl]
    ws = [w for w in wl]

    num_bins = 50
    fig, ax = plt.subplots()

    ##histo = matplotlib.pyplot.hist(arr_flat, 300, normed=True, color='gray', alpha = 0.5)
    ## https://stackoverflow.com/questions/9767241/setting-a-relative-frequency-in-a-matplotlib-histogram
    #histo = plt.hist(arr_flat, num_bins, normed=True, weights=np.zeros_like(arr_flat) + 1. / arr_flat.size, color='gray', alpha = 0.5)
    histo = ax.hist(arr_flat, num_bins, weights=np.zeros_like(arr_flat) + 1. / arr_flat.size, color='gray', alpha = 0.5)
    ax.set_xlabel('DN Value')
    ax.set_ylabel('Probability density')
    ax.set_title(r'Histogram of HRSI Pan DN Values')
    fig_name = inRaster.split('/')[-1].strip('.tif') + tail + '.png'

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
            ##matplotlib.pyplot.plot(histo[1],w*matplotlib.mlab.normpdf(histo[1],m,np.sqrt(c)), linewidth=3)
            plt.plot(histo[1],w*mlab.normpdf(histo[1],m,np.sqrt(c)), linewidth=3)
            ##matplotlib.pyplot.axis([arr_flat.min(),arr_flat.max(),0,.25])
            plt.axis([arr_flat.min(),arr_flat.max(),0,.25])

            guass_num = 'guass %s' %(i)
            print '\t' + guass_num + ' mean: ', m , ' std dev:',c
            outpk.write(',' + str(m) + ',' + str(c))        # Finish writing the line
            if i == ncomp:
                outpk.write('\n')
        fig.tight_layout()
        plt.savefig(os.path.join(plotdir,fig_name))
        plt.clf()

        return(out_peaksCSV)

def getparser():
    parser = argparse.ArgumentParser(description="Do Texture Filtering")
    parser.add_argument('r_fn', default=None, help='full path of input raster')
    parser.add_argument('metric_list', default=None, type=str, help='Input list of GLCM metrics')
    parser.add_argument('win_size_list', default=None, type=str, help='Input list of window sizes')
    parser.add_argument('distance_list', default=None, type=str, help='Input list of GLCM distances')
    return parser

def main():
    start_time = timer()
    parser = getparser()
    args = parser.parse_args()

    r_fn = args.r_fn
    metric_list = args.metric_list.split(" ")
    win_size_list = args.win_size_list.split(" ")
    distance_list = args.distance_list.split(" ")

    print "\tGLCM metrics to be processed: %s" %metric_list

    if not os.path.exists(r_fn):
        sys.exit("Unable to find r_fn: %s" % r_fn)

    # r_fn to r_ds
    r_ds = iolib.fn_getds(r_fn)

    # ...to array
    r_arr = r_ds.GetRasterBand(1).ReadAsArray()

    # Forget about masked arrays...just use np.where to put 0 for all invalid vals
    r_arr = np.where((r_arr > 0.0) & (r_arr <= 1.0), r_arr, np.nan)

    # A way of checking the histograms of the orig image, and the scaled image
    # scale and set to to byte
    ##fit_GMM(r_arr, os.path.split(r_fn)[0], os.path.split(r_fn)[1], 5, 'float')
    ##r_arr = img_as_ubyte(r_arr)
    ##r_arr = img_as_uint(r_arr)
    ##fit_GMM(r_arr, os.path.split(r_fn)[0], os.path.split(r_fn)[1], 5, 'byte')
    end_readdata = timer()
    print "\tData: %s" %(os.path.basename(r_fn))
    print "\n\tTime to read in data: {} minutes\n".format(round(find_elapsed_time(start_time, end_readdata),3))
    print "\tWindow sizes to be processed: %s" %win_size_list
    print "\tDistances to be processed: %s" %distance_list
    for win_size in win_size_list:
        print "\n\tWindow size: %s" %win_size
        win_size = int(win_size)

        for distance in distance_list:
            print "\tDistance: %s" %distance
            start_win_dist = timer()
            distance = int(distance)

            # Set up arrays to hold GLCM output
            #       these need to be float

            con = np.copy(r_arr).astype(np.float32)
            con[:] = 0
            dis = np.copy(r_arr).astype(np.float32)
            dis[:] = 0
            cor = np.copy(r_arr).astype(np.float32)
            cor[:] = 0
            asm = np.copy(r_arr).astype(np.float32)
            asm[:] = 0

            start_glcm = timer()
            print "\tCalculating GLCM and its properties..."
            # Loop over pixel windows (row and col, by win_size
            for i in range(con.shape[0] ):
                print i,
                for j in range(con.shape[1] ):

                    #windows needs to fit completely in image
                    if i <((win_size - 1)/2) or j <((win_size - 1)/2):
                        continue
                    if i > (con.shape[0] - ((win_size + 1)/2)) or j > (con.shape[0] - ((win_size + 1)/2)):
                        continue

                    #Calculate GLCM on a window
                    #
                    # converting to byte array right before GLCM processing
                    #       output arrays still with input precision
                    in_arr = img_as_ubyte(r_arr)
                    in_glcm_window_arr = in_arr[(i-((win_size - 1)/2)) : (i+((win_size + 1)/2)), (j-((win_size - 1)/2)) : (j+((win_size + 1)/2))]
                    del in_arr
                    out_glcm_window_arr = greycomatrix(in_glcm_window_arr, \
                                                        distances=[distance],\
                                                        #angles=[0, np.pi/4, np.pi/2, 3*np.pi/4],\
                                                        angles=[0],\
                                                        levels=256,  symmetric=True, normed=True )

                    con[i,j], dis[i,j], cor[i,j], asm[i,j] = [greycoprops(out_glcm_window_arr, metric) for metric in metric_list]

                    del out_glcm_window_arr

            end_glcm = timer()
            print "\n\tTime to compute this GLCM and its properties: {} minutes\n".format(round(find_elapsed_time(start_glcm, end_glcm),3))

            out_glcm_list = [con, dis, cor, asm]

            for num, metric in enumerate(metric_list):
                out_fn = os.path.splitext(r_fn)[0]+'_TEXTij_win'+str(win_size)+'_'+'dist'+str(distance)+'_'+metric+'.tif'
                print '\tWriting: %s' %(out_fn)
                iolib.writeGTiff(out_glcm_list[num], out_fn, r_fn)
            end_win_dist = timer()
            print "\tTotal compute time for GLCM on this window & distance: {} minutes\n".format(round(find_elapsed_time(start_win_dist, end_win_dist),3))

if __name__ == '__main__':
    main()