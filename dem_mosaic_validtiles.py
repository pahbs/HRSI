#! /usr/bin/env python
"""
Run dem_mosaic in parallel for valid tiles only
"""
#res=32
#res=8
#mkdir conus_${res}m_tile
#lfs setstripe conus_${res}m_tile --count 64
#~/src/Tools/dem_mosaic_validtiles.py --tr $res --t_projwin 'union' --t_srs '+proj=aea +lat_1=36 +lat_2=49 +lat_0=43 +lon_0=-115 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs ' --georef_tile_size 100000 -o conus_${res}m_tile/conus_${res}m *00/*/*DEM_${res}m.tif

#res=8
#mkdir hma_${res}m_tile
#lfs setstripe hma_${res}m_tile --count 64
#~/src/Tools/dem_mosaic_validtiles.py --tr $res --t_projwin 'union' --t_srs '+proj=aea +lat_1=25 +lat_2=47 +lat_0=36 +lon_0=85 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs ' --georef_tile_size 100000 -o hma_${res}m_tile/hma_${res}m */*00/*/*DEM_${res}m.tif

import os
import glob
import argparse
import math
import time
import subprocess
import tarfile
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor

from osgeo import gdal, ogr

from pygeotools.lib import geolib, warplib, iolib

def getparser():
    stat_choices = ['first', 'last', 'min', 'max', 'stddev', 'count', 'median']
    parser = argparse.ArgumentParser(description='Wrapper for dem_mosaic that will only write valid tiles')
    parser.add_argument('--tr', default='min', help='Output resolution (default: %(default)s)')
    parser.add_argument('--t_projwin', default='union', help='Output extent (default: %(default)s)')
    parser.add_argument('--t_srs', default='first', help='Output projection (default: %(default)s)')
    parser.add_argument('--georef_tile_size', type=float, default=100000., help='Output tile width (meters)')
    parser.add_argument('--threads', type=int, default=iolib.cpu_count(), help='Number of simultaneous jobs to run')
    parser.add_argument('--stat', type=str, default=None, choices=stat_choices, help='Statistic to use (default: weighted mean)')
    parser.add_argument('-o', type=str, default=None, help='Output mosaic prefix')
    parser.add_argument('src_fn_list', type=str, nargs='+', help='Input filenames (img1.tif img2.tif ...)')
    return parser

def main():
    parser = getparser()
    args = parser.parse_args()

    #Input filelist
    fn_list = args.src_fn_list
    #Might hit OS open file limit here
    print("Loading all datasets")
    ds_list = [gdal.Open(fn) for fn in fn_list]

    #Mosaic t_srs
    print("Parsing t_srs")
    t_srs = warplib.parse_t_srs(args.t_srs, ds_list)
    print(t_srs)

    #Mosaic res
    print("Parsing tr")
    tr = warplib.parse_res(args.tr, ds_list, t_srs=t_srs)
    print(tr)

    #Mosaic extent
    #xmin, ymin, xmax, ymax
    print("Parsing t_projwin")
    t_projwin = warplib.parse_extent(args.t_projwin, ds_list, t_srs=t_srs)
    print(t_projwin)
    #This could trim off some fraction of a pixel around margins
    t_projwin = geolib.extent_round(t_projwin, tr)
    mos_xmin, mos_ymin, mos_xmax, mos_ymax = t_projwin

    stat = args.stat
    if stat is not None:
        print("Mosaic type: %s" % stat)
    else:
        print("Mosaic type: Weighted average")

    #Tile dimensions in output projected units (meters)
    #Assume square
    tile_width = args.georef_tile_size
    tile_height = tile_width

    #This is number of simultaneous processes, each with one thread
    threads = args.threads

    o = args.o
    if o is None:
        o = 'mos_%im/mos' % tr
    odir = os.path.dirname(o)
    #If dirname is empty, use prefix for new directory
    if not odir:
        odir = o
        o = os.path.join(odir, o)
    if not os.path.exists(odir):
        os.makedirs(odir)
        cmd = ['lfs', 'setstripe', odir, '--count', str(threads)]
        #PAUL subprocess.call(cmd)
        cmd_subp = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        stout, sterr = cmd_subp.communicate()

    #Compute extent geom for all input datsets
    print("Computing extent geom for all input datasets")
    input_geom_dict = OrderedDict()
    for ds in ds_list:
        ds_geom = geolib.ds_geom(ds, t_srs)
        #Could use filename as key here
        input_geom_dict[ds] = ds_geom

    #Mosaic tile size
    #Should have float extent and tile dim here
    ntiles_w = int(math.ceil((mos_xmax - mos_xmin)/tile_width))
    ntiles_h = int(math.ceil((mos_ymax - mos_ymin)/tile_height))
    ntiles = ntiles_w * ntiles_h
    print("%i (%i cols x %i rows) tiles required for full mosaic" % (ntiles, ntiles_w, ntiles_h))
    #Use this for zero-padding of tile number
    ntiles_digits = len(str(ntiles))

    print("Computing extent geom for all output tiles")
    tile_dict = OrderedDict()
    for i in range(ntiles_w):
        for j in range(ntiles_h):
            tilenum = j*ntiles_w + i
            tile_xmin = mos_xmin + i*tile_width
            tile_xmax = mos_xmin + (i+1)*tile_width
            tile_ymax = mos_ymax - j*tile_height
            tile_ymin = mos_ymax - (j+1)*tile_height
            #Corner coord needed for geom
            x = [tile_xmin, tile_xmax, tile_xmax, tile_xmin, tile_xmin]
            y = [tile_ymax, tile_ymax, tile_ymin, tile_ymin, tile_ymax]
            tile_geom_wkt = 'POLYGON(({0}))'.format(', '.join(['{0} {1}'.format(*a) for a in zip(x,y)]))
            tile_geom = ogr.CreateGeometryFromWkt(tile_geom_wkt)
            tile_geom.AssignSpatialReference(t_srs)
            tile_dict[tilenum] = tile_geom

    out_tile_list = []
    print("Computing valid intersections between input dataset geom and tile geom")
    for tilenum, tile_geom in tile_dict.iteritems():
        for ds, ds_geom in input_geom_dict.iteritems():
            if tile_geom.Intersects(ds_geom):
                out_tile_list.append(tilenum)
                #Write out shp for debugging
                #geolib.geom2shp(tile_geom, 'tile_%03i.shp' % tilenum)
                break

    #Could also preserve list of input files that intersect tile
    #Then only process those files for given tile bounds
    #Avoid loading all files for each dem_mosaic call

    print("%i valid output tiles" % len(out_tile_list))
    out_tile_list.sort()
    out_tile_list_str = ' '.join(map(str, out_tile_list))
    print(out_tile_list_str)

    out_fn = o+'_tilenum_list.txt'
    with open(out_fn, 'w') as f:
        f.write(out_tile_list_str)

    print("Running dem_mosaic in parallel")
    dem_mosaic_args = (fn_list, o, tr, t_srs, t_projwin, tile_width, 1)
    processes = []
    log = False
    delay = 0.1
    outf = open(os.devnull, 'w')
    #outf = open('%s-log-dem_mosaic-tile-%i.log' % (o, tile), 'w')

    tile_fn_list = []
    with ThreadPoolExecutor(max_workers=threads) as executor:
        for n, tile in enumerate(out_tile_list):
            #print('%i of %i tiles: %i' % (n+1, len(out_tile_list), tile))
            cmd = geolib.get_dem_mosaic_cmd(*dem_mosaic_args, tile=tile, stat=stat)
            executor.submit(subprocess.call, cmd, stdout=outf, stderr=subprocess.STDOUT)
            #executor.submit(subprocess.Popen, cmd, stdout=outf, stderr=subprocess.STDOUT)
            tile_fn = '%s-tile-%03i.tif' % (o, tile)
            if stat is not None:
                tile_fn = os.path.splitext(tile_fn)[0]+'-%s.tif' % stat
            tile_fn_list.append(tile_fn)
            time.sleep(delay)

    outf = None

    print("Creating vrt of valid tiles:")
    tile_fn_list_str = ' '.join(str(e) for e in tile_fn_list)
    #tile_fn_list = glob.glob(o+'-tile-*.tif')
    vrt_fn = o+'.vrt'
    if stat is not None:
        vrt_fn = os.path.splitext(vrt_fn)[0]+'_%s.vrt' % stat
    cmd = 'gdalbuildvrt {} {}'.format(vrt_fn,tile_fn_list_str)
    #cmd.extend(tile_fn_list)
    print(cmd)
    #PAUL subprocess.call(cmd)
    cmd_subp = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
    stout, sterr = cmd_subp.communicate()

    #This cleans up all of the log txt files (potentially 1000s of files)
    #Want to preserve these, as they contain list of DEMs that went into each tile
    log_fn_list = glob.glob(o+'-log-dem_mosaic-*.txt')
    print("Cleaning up %i dem_mosaic log files" % len(log_fn_list))
    if stat is not None:
        tar_fn = o+'_%s_dem_mosaic_log.tar.gz' % stat
    else:
        tar_fn = o+'_dem_mosaic_log.tar.gz'
    with tarfile.open(tar_fn, "w:gz") as tar:
        for log_fn in log_fn_list:
            tar.add(log_fn)
    for log_fn in log_fn_list:
        os.remove(log_fn)

if __name__ == "__main__":
    main()