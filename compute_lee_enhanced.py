import os
import pyradar
import pyradar.core
import pyradar.core.sar
import pyradar.filters
import pyradar.filters.lee_enhanced
import numpy as np
from pygeotools.lib import iolib
import argparse

# Tutorial
# https://pyradar-tools.readthedocs.io/en/latest/tutorial.html

##r_fn='/att/gpfsfs/briskfs01/ppl/pmontesa/outASP/WV01_20120807_102001001C09B200_102001001C89C700/out-strip-holes-fill-DRG_sub3.tif'

def getparser():
    parser = argparse.ArgumentParser(description="Do Lee Sigma Filtering")
    parser.add_argument('r_fn', default=None, help='full path of input raster')
    parser.add_argument('winsize', type=int, default=5, help='Size of filter window (odd number)')
    parser.add_argument('k_val', type=float, default = 1.0, help='Float of dumping factor')
    parser.add_argument('cu_val', type=float, default=0.25, help='Float of the noise coefficient of variation')
    parser.add_argument('c_max', default=106, type=float, help='Float of the max image coefficient of variation')
    return parser

def main():

    parser = getparser()
    args = parser.parse_args()

    r_fn = args.r_fn
    winsize = args.winsize
    k_val = args.k_val
    cu_val = args.cu_val
    cmax_val = args.cmax_val

    ds=pyradar.core.sar.create_dataset_from_path(r_fn)
    bd=pyradar.core.sar.get_band_from_dataset(ds)
    ras_arr=pyradar.core.sar.read_image_from_band(bd)

    ras_arr[ras_arr < 0]=np.nan
    #r_arr = np.ma.masked_outside(ras_arr,0,1)
    #r_arr = np.ma.masked_invalid(r_arr)

    r_arr_filt=pyradar.filters.lee_enhanced.lee_enhanced_filter(r_arr,win_size=winsize, k=k_val, cu=cu_val, cmax=cmax_val)

    out_fn = os.path.splitext(r_fn)[0]+'_lee_enh_'+str(winsize)+'_'+str(k_val)+'_'+str(cu_val)+'_'+str(cmax_val)+'.tif'
    print 'Writing: %s' %(out_fn)
    iolib.writeGTiff(r_arr_filt, out_fn, r_fn)

if __name__ == '__main__':
    main()