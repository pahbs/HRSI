
import os
##import pyradar
##import pyradar.core
##import pyradar.core.sar
##import pyradar.filters
##import pyradar.filters.lee_enhanced
import skimage.feature
import skimage.filter
from skimage.feature import greycomatrix, greycoprops
from skimage import img_as_ubyte
import matplotlib.pyplot as plt
import matplotlib.mlab as mlab
from sklearn import mixture
import numpy as np
from pygeotools.lib import iolib
import argparse

# Tutorial
# https://pyradar-tools.readthedocs.io/en/latest/tutorial.html

##r_fn='/att/gpfsfs/briskfs01/ppl/pmontesa/outASP/WV01_20120807_102001001C09B200_102001001C89C700/out-strip-holes-fill-DRG_sub3.tif'

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
##    parser.add_argument('winsize', type=int, default=5, help='Size of filter window (odd number)')
##    parser.add_argument('P', type=int, default = 1, help='Number of circularly symmetric neighbour set points (quantization of the angular space)')
##    parser.add_argument('R', type=float, default=10, help='...')
##    parser.add_argument('meth', default='ror', type=str, help='...')
    return parser

def main():

    parser = getparser()
    args = parser.parse_args()

    r_fn = args.r_fn
    if not os.path.exists(r_fn):
        sys.exit("Unable to find r_fn: %s" % r_fn)

    # r_fn to r_ds
    r_ds = iolib.fn_getds(r_fn)
    # ...to array
    r_arr = r_ds.GetRasterBand(1).ReadAsArray()

    #r_arr[r_arr < 0]=0
    #r_arr = np.ma.masked_outside(ras_arr,0,1)
    #r_arr = np.ma.masked_invalid(r_arr)
    # Forget about masked arrays...just use np.where to put 0 for all invalid vals
    r_arr = np.where((r_arr > 0.0) & (r_arr <= 1.0), r_arr, 0.0)

    # scale and set to unsigned 16 bit
    #r_arr = r_arr * 1000.0
    #r_arr = r_arr.astype('uint16')
    fit_GMM(r_arr, os.path.split(r_fn)[0], os.path.split(r_fn)[1], 5, 'float')

    r_arr = img_as_ubyte(r_arr)
    ##print r_arr[5,5:14]    # at row, print ten elements
    fit_GMM(r_arr, os.path.split(r_fn)[0], os.path.split(r_fn)[1], 5, 'byte')

    ## http://scikit-image.org/docs/dev/api/skimage.feature.html#skimage.feature.greycoprops
    ## Structure with GLCM refs
    ## Kayitakire et al. 2006   http://www.sciencedirect.com/science/article/pii/S0034425706000988
    ## Ozdemnir et al. 2011     http://www.sciencedirect.com/science/article/pii/S0303243411000638
    ## Wood et al. 2012         http://www.sciencedirect.com/science/article/pii/S0034425712000156

    # This is just returning a value for the final pixl, i think.Check link below.
    ## https://stackoverflow.com/questions/35551249/implementing-glcm-texture-feature-with-scikit-image-and-python
    ## angles 0, 90, 180, 270
    #glcm = greycomatrix(r_arr, distances = [15], angles = [0, np.pi/4, np.pi/2, 3*np.pi/4], levels = 256,  symmetric = True, normed = True )
    glcm = greycomatrix(r_arr, distances = [5], angles = [0], levels = 256,  symmetric = True, normed = True )
    glcmprop_list = ["contrast", "dissimilarity", "correlation", "ASM"]
    contrast, dissim, corr, asm = [greycoprops(glcm, glcmprop) for glcmprop in glcmprop_list]

##    with ThreadPoolExecutor(max_workers=threads) as executor:
##        for n, glcmprop in enumerate(glcmprop_list):
##            #print('%i of %i tiles: %i' % (n+1, len(out_tile_list), tile))
##            cmd = greycoprops(glcm, glcmprop)
##            executor.submit(subprocess.call, cmd, stdout=outf, stderr=subprocess.STDOUT)
##            #executor.submit(subprocess.Popen, cmd, stdout=outf, stderr=subprocess.STDOUT)
##            tile_fn = '%s-tile-%03i.tif' % (o, tile)
##            if stat is not None:
##                tile_fn = os.path.splitext(tile_fn)[0]+'-%s.tif' % stat
##            tile_fn_list.append(tile_fn)
##            time.sleep(delay)

    #r_arr_filt=pyradar.filters.lee_enhanced.lee_enhanced_filter(r_arr,win_size=winsize, k=k_val, cu=cu_val, cmax=cmax_val)
    #r_arr_filt = skimage.feature.local_binary_pattern(r_arr, P, R, method=meth)
    #r_arr_filt = skimage.filter.canny(r_arr, sigma=3.0)
    ##out_fn = os.path.splitext(r_fn)[0]+'_TEXT_'+str(P)+'_'+str(R)+'_'+str(meth)+'.tif'
    out_glcm_list = [contrast, dissim, corr, asm]
    for num, glcmprop in enumerate(glcmprop_list):
        out_fn = os.path.splitext(r_fn)[0]+'_TEXT_'+glcmprop+'.tif'
        print 'Writing: %s' %(out_fn)
        iolib.writeGTiff(out_glcm_list[num], out_fn, r_fn)

if __name__ == '__main__':
    main()