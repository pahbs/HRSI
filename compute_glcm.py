import os
import gdal, osr
import numpy as np
import subprocess as subp
from scipy.interpolate import RectBivariateSpline
from numpy.lib.stride_tricks import as_strided as ast
import dask.array as da
from joblib import Parallel, delayed, cpu_count
from skimage.feature import greycomatrix, greycoprops
##from pygeotools.lib import iolib
import argparse

##https://stackoverflow.com/questions/35551249/implementing-glcm-texture-feature-with-scikit-image-and-python

def im_resize(im,Nx,Ny):
    '''
    resize array by bivariate spline interpolation
    '''
    ny, nx = np.shape(im)
    xx = np.linspace(0,nx,Nx)
    yy = np.linspace(0,ny,Ny)

    try:
        im = da.from_array(im, chunks=1000)   #dask implementation
    except:
        pass

    newKernel = RectBivariateSpline(np.r_[:ny],np.r_[:nx],im)
    return newKernel(yy,xx)

def p_me(Z, win):
    '''
    loop to calculate greycoprops
    '''
    try:
        glcm = greycomatrix(Z, [5], [0], 256, symmetric=True, normed=True)
        cont = greycoprops(glcm, 'contrast')
        diss = greycoprops(glcm, 'dissimilarity')
        homo = greycoprops(glcm, 'homogeneity')
        eng = greycoprops(glcm, 'energy')
        corr = greycoprops(glcm, 'correlation')
        ASM = greycoprops(glcm, 'ASM')
        return (cont, diss, homo, eng, corr, ASM)
    except:
        return (0,0,0,0,0,0)


def read_raster(in_raster):
    in_raster=in_raster
    ds = gdal.Open(in_raster)
    data = ds.GetRasterBand(1).ReadAsArray()
    data[data<=0] = np.nan
    gt = ds.GetGeoTransform()
    xres = gt[1]
    yres = gt[5]

    # get the edge coordinates and add half the resolution
    # to go to center coordinates
    xmin = gt[0] + xres * 0.5
    xmax = gt[0] + (xres * ds.RasterXSize) - xres * 0.5
    ymin = gt[3] + (yres * ds.RasterYSize) + yres * 0.5
    ymax = gt[3] - yres * 0.5
    del ds
    # create a grid of xy coordinates in the original projection
    xx, yy = np.mgrid[xmin:xmax+xres:xres, ymax+yres:ymin:yres]
    return data, xx, yy, gt

def norm_shape(shap):
   '''
   Normalize numpy array shapes so they're always expressed as a tuple,
   even for one-dimensional shapes.
   '''
   try:
      i = int(shap)
      return (i,)
   except TypeError:
      # shape was not a number
      pass

   try:
      t = tuple(shap)
      return t
   except TypeError:
      # shape was not iterable
      pass

   raise TypeError('shape must be an int, or a tuple of ints')

def sliding_window(a, ws, ss = None, flatten = True):
    '''
    Source: http://www.johnvinyard.com/blog/?p=268#more-268
    Parameters:
        a  - an n-dimensional numpy array
        ws - an int (a is 1D) or tuple (a is 2D or greater) representing the size
             of each dimension of the window
        ss - an int (a is 1D) or tuple (a is 2D or greater) representing the
             amount to slide the window in each dimension. If not specified, it
             defaults to ws.
        flatten - if True, all slices are flattened, otherwise, there is an
                  extra dimension for each dimension of the input.

    Returns
        an array containing each n-dimensional window from a
    '''
    if None is ss:
        # ss was not provided. the windows will not overlap in any direction.
        ss = ws
    ws = norm_shape(ws)
    ss = norm_shape(ss)
    # convert ws, ss, and a.shape to numpy arrays
    ws = np.array(ws)
    ss = np.array(ss)
    shap = np.array(a.shape)
    # ensure that ws, ss, and a.shape all have the same number of dimensions
    ls = [len(shap),len(ws),len(ss)]
    if 1 != len(set(ls)):
        raise ValueError(\
        'a.shape, ws and ss must all have the same length. They were %s' % str(ls))

    # ensure that ws is smaller than a in every dimension
    if np.any(ws > shap):
        raise ValueError(\
        'ws cannot be larger than a in any dimension.\
     a.shape was %s and ws was %s' % (str(a.shape),str(ws)))

    # how many slices will there be in each dimension?
    newshape = norm_shape(((shap - ws) // ss) + 1)


    # the shape of the strided array will be the number of slices in each dimension
    # plus the shape of the window (tuple addition)
    newshape += norm_shape(ws)


    # the strides tuple will be the array's strides multiplied by step size, plus
    # the array's strides (tuple addition)
    newstrides = norm_shape(np.array(a.strides) * ss) + a.strides
    a = ast(a,shape = newshape,strides = newstrides)
    if not flatten:
        return a
    # Collapse strided so that it has one more dimension than the window.  I.e.,
    # the new array is a flat list of slices.
    meat = len(ws) if ws.shape else 0
    firstdim = (np.product(newshape[:-meat]),) if ws.shape else ()
    dim = firstdim + (newshape[-meat:])
    # remove any dimensions with size 1
    dim = filter(lambda i : i != 1,dim)

    return a.reshape(dim), newshape

def CreateRaster(xx,yy,std,gt,proj,driverName,outFile):
    '''
    Exports data to GTiff Raster
    '''
    std = np.squeeze(std)
    std[np.isinf(std)] = -99
    driver = gdal.GetDriverByName(driverName)
    rows,cols = np.shape(std)
    ds = driver.Create( outFile, cols, rows, 1, gdal.GDT_Float32)
    if proj is not None:
        ds.SetProjection(proj.ExportToWkt())
    ds.SetGeoTransform(gt)
    ss_band = ds.GetRasterBand(1)
    ss_band.WriteArray(std)
    ss_band.SetNoDataValue(-99)
    ss_band.FlushCache()
    ss_band.ComputeStatistics(False)
    del ds

def getparser():

    parser = argparse.ArgumentParser(description="Do Texture Filtering")
    parser.add_argument('r_fn', default=None, help='full path of input raster')
    parser.add_argument('epsg_code', default=None, type=str, help='EPSG code')
    return parser

def main():

    parser = getparser()
    args = parser.parse_args()

    r_fn = args.r_fn

    glcmprop_list = ["contrast", "dissimilarity", "homogeneity", "energy", "correlation", "ASM"]

    win_sizes = [7]

    for win_size in win_sizes[:]:

        #r_fn = #Path to input raster
        win = win_size
        meter = str(win/4)

        #Define output file names
        #contFile    = os.path.splitext(r_fn)[0]+'_TEXT_'+'cont'+'.tif'
        #dissFile    = os.path.splitext(r_fn)[0]+'_TEXT_'+'dissim'+'.tif'
        #homoFile    = os.path.splitext(r_fn)[0]+'_TEXT_'+'homog'+'.tif'
        #energyFile  = os.path.splitext(r_fn)[0]+'_TEXT_'+'energy'+'.tif'
        #corrFile    = os.path.splitext(r_fn)[0]+'_TEXT_'+'corr'+'.tif'
        #ASMFile     = os.path.splitext(r_fn)[0]+'_TEXT_'+'asm'+'.tif'

        merge, xx, yy, gt = read_raster(r_fn)

        merge[np.isnan(merge)] = 0

        Z,ind = sliding_window(merge,(win,win),(win,win))

        Ny, Nx = np.shape(merge)

        w = Parallel(n_jobs = cpu_count(), verbose=0)(delayed(p_me)(Z[k]) for k in xrange(len(Z)))

        cont = [a[0] for a in w]
        diss = [a[1] for a in w]
        homo = [a[2] for a in w]
        eng  = [a[3] for a in w]
        corr = [a[4] for a in w]
        ASM  = [a[5] for a in w]


        #Reshape to match number of windows
        plt_cont    = np.reshape(cont , ( ind[0], ind[1] ) )
        plt_diss    = np.reshape(diss , ( ind[0], ind[1] ) )
        plt_homo    = np.reshape(homo , ( ind[0], ind[1] ) )
        plt_eng     = np.reshape(eng  , ( ind[0], ind[1] ) )
        plt_corr    = np.reshape(corr , ( ind[0], ind[1] ) )
        plt_ASM     =  np.reshape(ASM , ( ind[0], ind[1] ) )
        del cont, diss, homo, eng, corr, ASM

        #Resize Images to receive texture and define filenames
        contrast = im_resize(plt_cont,Nx,Ny)
        contrast[merge==0]=np.nan
        dissimilarity = im_resize(plt_diss,Nx,Ny)
        dissimilarity[merge==0]=np.nan
        homogeneity = im_resize(plt_homo,Nx,Ny)
        homogeneity[merge==0]=np.nan
        energy = im_resize(plt_eng,Nx,Ny)
        energy[merge==0]=np.nan
        correlation = im_resize(plt_corr,Nx,Ny)
        correlation[merge==0]=np.nan
        ASM = im_resize(plt_ASM,Nx,Ny)
        ASM[merge==0]=np.nan

        del plt_cont, plt_diss, plt_homo, plt_eng, plt_corr, plt_ASM
        del w, ind,Ny,Nx

        out_glcm_list = [contrast, dissimilarity, homogeneity, energy, correlation, ASM]

        driverName= 'GTiff'
        epsg_code=args.epsg_code
        proj = osr.SpatialReference()
        proj.ImportFromEPSG(epsg_code)

        for num, glcmprop in enumerate(glcmprop_list):
            out_fn = os.path.splitext(r_fn)[0]+'_TEXT_'+glcmprop+'.tif'
            print ("Writing: %s" %(out_fn))
            #iolib.writeGTiff(out_glcm_list[num], out_fn, r_fn)

            CreateRaster(xx, yy, glcmprop, gt, proj,driverName,out_fn)
            del glcmprop


        del contrast, merge, xx, yy, gt, meter

if __name__ == '__main__':
    main()