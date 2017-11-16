#! /usr/bin/env python
"""Scales float values by an input factor and converts to an output type

"""
import os
import argparse
import numpy as np
from pygeotools.lib import iolib

def fmt_choices(param, choices):
    outstr = 'Valid choices for %s: {%s}\n' % (param, ','.join(choices))
    return outstr

def getparser():
    ##https://docs.scipy.org/doc/numpy/user/basics.types.html
    out_type_choices = ['int8', 'int16', 'int32', 'int64', 'uint8', 'uint16', 'uint32', 'uint64', 'float16', 'float32', 'float64']
    epilog = fmt_choices('-out_type', out_type_choices)

    parser = argparse.ArgumentParser(description="Scales float values by an input factor and converts to an output type",\
        epilog=epilog)
    parser.add_argument('r_fn', type=str, help='Input raster filename')
    parser.add_argument('-scale_factor', type=int, default=100, help='Factor by which array vals are multiplied(default: %(default)s)')
    parser.add_argument('-out_type', type=str, default='int16', help='Output data type (default: %(default)s)')
    parser.add_argument('--nodata_val', type=int, default=-99, help='Output no data value (default: %(default)s)')
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

    r_arr = r_arr * args.scale_factor
    arr_out = r_arr.astype(getattr(np,args.out_type))       # this works like r_arr.astype(np.unit16)

    print "\tOutput array type: %s" %(arr_out.dtype)
    print "\tOutput nodata value: %s" %(args.nodata_val)
    # Set No Data Value: Deal with negatives and nan
    arr_out = np.where(arr_out < 0, args.nodata_val, arr_out)
    arr_out = np.where(np.isnan(arr_out),args.nodata_val, arr_out)

    #Write out
    out_fn = os.path.splitext(r_fn)[0]+'_'+args.out_type+'.tif'
    print "\tOutput file: %s" %(out_fn)
    #Note: passing r_fn here as the src_ds
    iolib.writeGTiff(arr_out, out_fn, r_fn)

if __name__ == "__main__":
    main()
