#! /usr/bin/env python

#Library containing various functions for working with DigitalGlobe imagery

import os
import glob
from datetime import datetime, timedelta

import numpy as np
from osgeo import gdal, ogr, osr

from pygeotools.lib import geolib
from pygeotools.lib import timelib

wgs_srs = geolib.wgs_srs 

#Creat class for id_dict and pair_dict with appropriate parameters and functions distributed

#Create spacecraft class
#Contain orbit height, sensor dimensions, etc
#CCD boundaries, offsets

#Extract height from Ephemeris?
wv1_alt = 496.0
wv2_alt = 770.0
wv3_alt = 617.0

#Spectral Irradiance for sensor/band combinations
#band-averaged solar spectral irradiance (W/m^2*um) at the average Earth-Sun distance
#Table 4 in https://www.digitalglobe.com/sites/default/files/Radiometric_Use_of_WorldView-2_Imagery%20%281%29.pdf
EsunDict = { 
'QB02_BAND_P':1381.79,
'QB02_BAND_B':1924.59,
'QB02_BAND_G':1843.08,
'QB02_BAND_R':1574.77,
'QB02_BAND_N':1113.71,
'WV01_BAND_P':1487.54715,
'WV02_BAND_P':1580.8140,
'WV02_BAND_C':1758.2229,
'WV02_BAND_B':1974.2416,
'WV02_BAND_G':1856.4104,
'WV02_BAND_Y':1738.4791,
'WV02_BAND_R':1559.4555,
'WV02_BAND_RE':1342.0695,
'WV02_BAND_N':1069.7302,
'WV02_BAND_N2':861.2866,
'WV03_BAND_P':1616.4508,
'WV03_BAND_C':1544.5748,
'WV03_BAND_B':1971.4957,
'WV03_BAND_G':1821.7494,
'WV03_BAND_Y':1779.2849,
'WV03_BAND_R':1586.8104,
'WV03_BAND_RE':1320.2137,
'WV03_BAND_N':1088.7935,
'WV03_BAND_N2':777.5231,
'GE01_BAND_P':1617,
'GE01_BAND_B':1960,
'GE01_BAND_G':1853,
'GE01_BAND_R':1505,
'GE01_BAND_N':1039,
'IK01_BAND_P':1375.8,
'IK01_BAND_B':1930.9,
'IK01_BAND_G':1854.8,
'IK01_BAND_R':1556.5,
'IK01_BAND_N':1156.9
}

#WV2 xml band order
#BAND_C 1
#BAND_B 2
#BAND_G 3
#BAND_Y 4
#BAND_R 5
#BAND_RE 6
#BAND_N 7 
#BAND_N2 8 

wv_ms_bands={1:'C',2:'B',3:'G',4:'Y',5:'R',6:'RE',7:'N',8:'N2'} 

#Catalog az is targetaz (azimuth from sat to target) while xml az is sataz (azimuth from target to sat)
def az_correct(az):
    az = az - 180.0
    if az < 0:
        az += 360.0
    return az

def shp_dt_list(shp_fn, dt_f_name=None, geom=False):
    d_list = geolib.shp_dict(shp_fn, fields=dt_f_name)
    if dt_f_name is None:
        f_list = d_list[0].keys()
        for f in f_list:
            if 'date' in f:
                dt_f_name = f
                break
    dt_list = [i[dt_f_name] for i in d_list]
    dt_list_sort = sorted(dt_list)
    return dt_list_sort

def parse_pgc_catalog_stereo(shp_fn):
    return None

def timelist_dict(timelist):
    d = {}
    with open(timelist) as f:
        for line in f:
           (key, val) = line.split()
           d[key] = val
    return d

def parse_pgc_catalog_mono(shp_fn, timelist=None):
    if timelist is not None:
        timedict = timelist_dict(timelist)
    ds = ogr.Open(shp_fn)
    lyr = ds.GetLayer()
    nfeat = lyr.GetFeatureCount()
    print '%i input features\n' % nfeat
    d_list = []
    for n,feat in enumerate(lyr):
        d = {}
        #catalogid = feat.GetField("catalogid")
        #platform = feat.GetField("platform")
        catalogid = feat.GetField("CATALOGID")
        platform = feat.GetField("PLATFORM")

        #This throws out GE and QB data
        #Could probably do GE and WV pairs without issue
        #if not (('WV' in platform) or ('GE' in platform)):
        #    print "Excluding non-WV platform: "+catalogid
        #    continue

        #acqdate = feat.GetField("acqdate")
        #avoffnadir = feat.GetField("avoffnadir")
        #avtargetaz = feat.GetField("avtargetaz")
        #cloudcover = feat.GetField("cloudcover")
        #stereopair = feat.GetField("stereopair")
        acqdate = feat.GetField("ACQDATE")
        avoffnadir = feat.GetField("AVOFFNADIR")
        avtargetaz = feat.GetField("AVTARGETAZ")
        cloudcover = feat.GetField("CLOUDCOVER")
        stereopair = feat.GetField("STEREOPAIR")
        if stereopair == "NONE":
            stereopair = None
        geom = feat.GetGeometryRef()
        d['id'] = catalogid 
        d['sensor'] = platform
        d['date'] = datetime.strptime(acqdate,"%Y-%m-%d")
        if timelist is not None:
            if catalogid in timedict:
                mydt = timedict[catalogid]
                d['date'] = datetime.strptime(mydt, "%Y%m%d%H%M%S")
        d['alt'] = get_alt(catalogid)
        #May need to do 180 deg from this, as az is usually SATAZ
        #d['az'] = float('%0.2f' % avtargetaz)
        d['az'] = float('%0.2f' % az_correct(avtargetaz))
        d['el'] = float('%0.2f' % (90 - avoffnadir))
        d['offnadir'] = float('%0.2f' % avoffnadir)
        d['cloudcover'] = float(cloudcover)
        d['stereopair'] = stereopair
        d['geom'] = geolib.geom_dup(geom)
        d_list.append(d)
    d_list_sort = sorted(d_list, key=lambda k: k['date']) 
    print
    print '%i features will be considered' % len(d_list_sort)
    ds = None
    return d_list_sort

#pair is a list of two dicts

#Need to add support for coincident WV1/WV2 pairs, as they have different altitudes
def get_pair_conv(p):
    from numpy import deg2rad as d2r
    from numpy import rad2deg as r2d
    id1 = p['id1_dict']['id']
    az1 = p['id1_dict']['az']
    el1 = p['id1_dict']['el']
    alt1 = p['id1_dict']['alt']
    id2 = p['id2_dict']['id']
    az2 = p['id2_dict']['az']
    el2 = p['id2_dict']['el']
    alt2 = p['id2_dict']['alt']
    #print "%s, %0.2f, %0.2f" % (id1, az1, el1)
    #print "%s, %0.2f, %0.2f" % (id2, az2, el2)
    conv_ang = r2d(np.arccos(np.sin(d2r(el1))*np.sin(d2r(el2)) + \
            np.cos(d2r(el1))*np.cos(d2r(el2))*np.cos(d2r(az1 - az2))))
    #base = 2 * alt * np.tan(d2r(conv_ang/2))
    #This may not actually be correct, as actual lengths are not altitudes, but longer b/c arbitrary
    base = np.sqrt(alt1**2 + alt2**2 - 2*alt1*alt2*np.cos(d2r(conv_ang)))
    bh = base/np.mean([alt1, alt2])
    #print "%0.2f, %0.2f" % (conv_ang, bh)
    p['conv_ang'] = float('%0.2f' % conv_ang)
    #return float('%0.2f' % conv_ang)

def get_pair_dt(p):
    dt1 = p['id1_dict']['date']
    dt2 = p['id2_dict']['date']
    dt = abs(dt1 - dt2)
    p['dt'] = dt

def get_pair_intersection(p):
    geom1 = p['id1_dict']['geom']
    geom2 = p['id2_dict']['geom']
    intersection = geolib.geom_intersection([geom1, geom2])
    p['intersection'] = intersection
    #This recomputes for local orthographic - important for width/height calculations
    intersection_local = geolib.geom2localortho(intersection)
    if intersection is not None:
        #Area calc shouldn't matter too much
        intersection_area = intersection_local.GetArea()
        int_w, int_h = geolib.geom_wh(intersection_local)
        #Comput width/height, scale to km
        p['int_w'] = float('%0.2f' % (int_w/1000.))
        p['int_h'] = float('%0.2f' % (int_h/1000.))
        p['intersection_area'] = float("{0:.2f}".format(intersection_area/1E6))
        perc = (100*intersection_area/geom1.GetArea(), 100*intersection_area/geom2.GetArea()) 
        perc = (float("{0:.2f}".format(perc[0])), float("{0:.2f}".format(perc[1])))
        p['intersection_area_perc'] = perc
    else:
        p['intersection_area'] = None 
        p['intersection_area_perc'] = None 

def intrack_check(p):
    if p['id1_dict']['stereopair'] == p['id2_dict']['id'] or p['id2_dict']['stereopair'] == p['id1_dict']['id']:
        #The DG catalog only has one of these populated
        #Should set both
        p['id1_dict']['stereopair'] = p['id2_dict']['id']
        p['id2_dict']['stereopair'] = p['id1_dict']['id']
        p['pairtype'] = 'intrack'
    else:
        p['pairtype'] = 'coincident' 

#This is much faster, uses dt_diff and intersection to filter large list of inputs 
def get_candidates_dt(d_list, max_dt=7.0):
    #Input list should already be sorted by date
    dt_list = np.array([d['date'] for d in d_list])
    #This is output list of candidates
    candidates = []
    for n,d in enumerate(d_list):
        c_list = []
        dt1 = d['date']
        print n, dt1
        #Find anything forward in time
        dt_idx = n+1 + timelib.get_closest_dt_padded_idx(dt1, dt_list[n+1:], pad=timedelta(days=max_dt))
        if len(dt_idx) > 0:
            geom1 = d['geom']
            #print "Checking %i options" % len(dt_idx)
            for i in dt_idx:
                geom2 = d_list[i]['geom']
                intersection = geolib.geom_intersection([geom1, geom2])
                if intersection is not None:
                    c_list.append((d,d_list[i]))
        if len(c_list) > 0:
            for c in c_list:
                p = pair_dict(c[0], c[1])
                candidates.append(p)
            #print '%i pair candidates\n' % len(c_list)
    print '%i total pair candidates\n' % len(candidates)
    return candidates

def get_candidates(d_list):
    import itertools
    candidates = []
    for c in itertools.combinations(d_list, 2):
        p = pair_dict(c[0], c[1])
        if p['intersection'] is not None:
            candidates.append(p)
    print '%i pair candidates\n' % len(candidates)
    return candidates

#This can be used to determine the max time interval for a given location
#Turns out that for Jak or other fast-flowing locations, 0.1 px displacement in only a few minutes
def max_dt(vm, px_tol=0.1, res=0.5):
    return px_tol/((1/res)*(vm/365.25))

#Return expected disparities based on an existing set of x and y velocities (m/yr)
#Subtract from ASP -F.tif
#Shift actual pixels in mosaiaced ortho
#Should clip to extent of stereo intersection 
#vx = iolib.fn_getma(vx_fn)
#vy = iolib.fn_getma(vy_fn)
def exp_disp(vx, vy, dt, res=0.5):
    vx_s = vx/365.25/24/60/60
    vy_s = vy/365.25/24/60/60
    dt_s = dt.total_seconds()
    x_m = dt_s*vx_s
    y_m = dt_s*vy_s
    m = np.sqrt(x_m**2+y_m**2)
    x_px = x_m/res
    y_px = y_m/res
    return x_px, y_px

#min_area is in square km
def get_validpairs(candidates, min_conv=5, max_conv=70, max_dt_days=1.0, min_area=500, min_area_perc=20, min_w=10, min_h=10, max_cc=75, include_intrack=False, same_platform=False):
    max_dt = timedelta(days=max_dt_days)
    #candidates = [p for p in itertools.combinations(d_list, 2)]
    good = []
    for p in candidates:
        #Much faster to evaluate each step by step than precomputing for all possible pairs
        #area = get_pair_intersection_area(p)
        #area_perc = get_pair_intersection_area_perc(p)
        #print area, area_perc
        if p['intersection'] is None:
            continue
        if p['intersection_area'] < min_area:
            continue
        if p['int_h'] < min_h or p['int_w'] < min_w:
            continue
        if np.any(np.array(p['intersection_area_perc']) < min_area_perc):
            continue
        #dt = get_pair_dt(p)
        #print dt
        if p['dt'] > max_dt:
            continue
        #conv = get_pair_conv(p)
        #print conv
        if p['conv_ang'] < min_conv or p['conv_ang'] > max_conv:
            continue
        #if p['pairtype'] is 'intrack':
        #    continue
        if p['id1_dict']['cloudcover'] >= max_cc or p['id2_dict']['cloudcover'] >= max_cc:
            continue
        if not include_intrack:
            if p['id1_dict']['stereopair'] is not None or p['id2_dict']['stereopair'] is not None:
                continue
        if same_platform:
            if p['id1_dict']['sensor'] != p['id2_dict']['sensor']:
                continue
        #Exclude QB + WV pairs
        if np.array((p['id1_dict']['sensor'] == 'QB02', p['id2_dict']['sensor'] == 'QB02')).nonzero()[0].size == 1:
            continue
        print p['pairtype'], p['id1_dict']['id'], p['id2_dict']['id'], p['cdate'], p['dt'], p['conv_ang'], p['int_w'], p['int_h'], p['intersection_area'], p['intersection_area_perc'], p['id1_dict']['cloudcover'], p['id2_dict']['cloudcover']
        good.append(p)
    print
    print '%i valid pairs' % len(good)
    return good

def unique_ids(p_list, out_fn=None):
    id_list = []
    for i in p_list:
        id_list.extend([i['id1_dict']['id'], i['id2_dict']['id']])
    outlist = list(set(id_list))
    outlist.sort()
    print len(id_list)
    print len(outlist)
    if out_fn is None:
        out_fn = 'validpairs_uniqid.txt'
    f = open(out_fn, 'w')
    f.write('\n'.join(outlist))
    return outlist

def valid_txt(p_list, out_fn=None):
    import csv
    if out_fn is None:
        out_fn='validpairs.csv'
    with open(out_fn, 'wb') as f:
        writer = csv.writer(f)
        for p in p_list:
            line = p['pairtype'], p['id1_dict']['id'], p['id2_dict']['id'], p['cdate'], '%0.3f' % (p['dt'].total_seconds()/3600.), p['conv_ang'], p['intersection_area'], p['intersection_area_perc'][0], p['intersection_area_perc'][1], p['id1_dict']['cloudcover'], p['id2_dict']['cloudcover']
            writer.writerow(line)

#Write out a shapefile of valid intersections
def valid_shp(p_list, out_fn=None):
    driverName = "ESRI Shapefile"
    drv = ogr.GetDriverByName(driverName)
    if out_fn is None:
        out_fn='validpairs.shp'
    if os.path.exists(out_fn):
        drv.DeleteDataSource(out_fn)
    out_ds = drv.CreateDataSource(out_fn)
    out_lyrname = os.path.splitext(os.path.split(out_fn)[1])[0]
    geom = p_list[0]['intersection']
    geom_srs = geom.GetSpatialReference()
    geom_type = geom.GetGeometryType()
    out_lyr = out_ds.CreateLayer(out_lyrname, geom_srs, geom_type)
    
    field_defn = ogr.FieldDefn("pairname", ogr.OFTString)
    field_defn.SetWidth(128)
    out_lyr.CreateField(field_defn)
    field_defn = ogr.FieldDefn("pairtype", ogr.OFTString)
    field_defn.SetWidth(32)
    out_lyr.CreateField(field_defn)
    #field_defn = ogr.FieldDefn("date", ogr.OFTInteger)
    field_defn = ogr.FieldDefn("date", ogr.OFTString)
    field_defn.SetWidth(32)
    out_lyr.CreateField(field_defn)
    #field_defn = ogr.FieldDefn("dt", ogr.OFTString)
    field_defn = ogr.FieldDefn("dt", ogr.OFTReal)
    field_defn.SetPrecision(3)
    out_lyr.CreateField(field_defn)
    field_defn = ogr.FieldDefn("conv_ang", ogr.OFTReal)
    field_defn.SetPrecision(1)
    out_lyr.CreateField(field_defn)
    field_defn = ogr.FieldDefn("int_area", ogr.OFTReal)
    field_defn.SetPrecision(1)
    out_lyr.CreateField(field_defn)
    field_defn = ogr.FieldDefn("cc1", ogr.OFTReal)
    field_defn.SetPrecision(1)
    out_lyr.CreateField(field_defn)
    field_defn = ogr.FieldDefn("cc2", ogr.OFTReal)
    field_defn.SetPrecision(1)
    out_lyr.CreateField(field_defn)

    for p in p_list:
        pairname = p['pairname']
        pairtype = p['pairtype']
        #date = int(datetime.strftime(p['cdate'], '%Y%m%d'))
        date = str(datetime.strftime(p['cdate'], '%Y%m%d%H%M%S'))
        #dt = str(p['dt'])
        #This is now output in hours
        dt = '%0.3f' % (p['dt'].total_seconds()/3600.)
        conv_ang = p['conv_ang']
        intersection_area = p['intersection_area']
        geom = p['intersection']
        cc1 = p['id1_dict']['cloudcover']
        cc2 = p['id2_dict']['cloudcover']
        out_feat = ogr.Feature(out_lyr.GetLayerDefn())
        out_feat.SetGeometry(geom)
        out_feat.SetField("pairname", pairname)
        out_feat.SetField("pairtype", pairtype)
        out_feat.SetField("date", date)
        out_feat.SetField("dt", dt)
        out_feat.SetField("conv_ang", conv_ang)
        out_feat.SetField("int_area", intersection_area)
        out_feat.SetField("cc1", cc1)
        out_feat.SetField("cc2", cc2)
        out_lyr.CreateFeature(out_feat)
    out_ds = None
    
#Can also use this ID-based approach to extract sensor
def get_alt(id):
    #if getTag(xml, 'SATID') == 'WV01': 
    id = str(id)
    if id.startswith('102'):
        alt = wv1_alt
    elif id.startswith('103'):
        alt = wv2_alt
    elif id.startswith('105'):
        alt = wv3_alt
    else:
        #This should be easily recognizable 
        alt = -99999.0
    return alt

#Note: this computes mean from all ntf for an ID with equal weighting - doesn't account for short ntf
#Should be a class
def get_id_dict(dir, id):
    d = {}
    ntflist = get_ntflist(dir, id) 
    #xmllist = [getxml(ntf) for ntf in ntflist]
    xmllist = get_xmllist(dir, id) 
    azlist = []
    ellist = []
    offnadirlist = []
    cclist = []
    geomlist = []
    for xml in xmllist:
        azlist.append(float(getTag(xml, 'MEANSATAZ')))
        ellist.append(float(getTag(xml, 'MEANSATEL')))
        offnadirlist.append(float(getTag(xml, 'MEANOFFNADIRVIEWANGLE')))
        cclist.append(float(getTag(xml, 'CLOUDCOVER')))
        geomlist.append(xml2geom(xml))
    ugeom = geolib.geom_union(geomlist)
    #Populate dictionary
    d['id'] = str(id)
    d['sensor'] = getTag(xml, 'SATID')
    d['date'] = xml_dt(xmllist[0]) 
    d['alt'] = get_alt(id)
    d['az'] = float('%0.2f' % np.mean(azlist))
    d['el'] = float('%0.2f' % np.mean(ellist))
    d['offnadir'] = float('%0.2f' % np.mean(offnadirlist))
    d['cloudcover'] = float('%0.2f' % np.mean(cclist))
    d['geom'] = ugeom
    d['stereopair'] = None
    return d

#This creates a list of all ids within a directory, for arbitrary number of ids
def get_pair_dictlist(dir):
    d_list = []
    ids = dir_ids(dir)
    for id in ids:
        d_list.append(get_id_dict(dir, id))
    return d_list

#This should be a class
#Be more careful about ordering
def pair_dict(id1_dict, id2_dict, pairname=None):
    p = {} 
    p['id1_dict'] = id1_dict 
    p['id2_dict'] = id2_dict 
    get_pair_intersection(p)
    if p['intersection'] is not None:
        cdate = timelib.center_date(p['id1_dict']['date'], p['id2_dict']['date'])
        intrack_check(p)
        p['cdate'] = cdate
        if pairname is None:
            #if p['id1_dict']['sensor'] != p['id2_dict']['sensor']:
            if p['pairtype'] == 'intrack':
                sensor = p['id1_dict']['sensor']
            else:
                sensor = p['id1_dict']['sensor']+p['id2_dict']['sensor']
            cdate_str = datetime.strftime(p['cdate'], '%Y%m%d')
            pairname = '%s_%s_%s_%s' % (sensor, cdate_str, p['id1_dict']['id'], p['id2_dict']['id'])
        p['pairname'] = pairname 
        get_pair_dt(p)
        get_pair_conv(p)
    return p

#This creates a dictionary for a true pair with only two ids
def dir_pair_dict(dir):
    p = {} 
    ids = dir_ids(dir)
    id1_dict = (get_id_dict(dir, ids[0]))
    id2_dict = (get_id_dict(dir, ids[1]))
    pairname = os.path.split(dir)[-1]
    p = pair_dict(id1_dict, id2_dict, pairname)
    return p 

def pairname_ids(dir):
    return os.path.split(dir)[-1].split('_')[2:4]

def dir_ids(dir, panonly=False):
    imglist = []
    if panonly:
        #This won't work with 103001003A146500_r100.tif
        imglist.extend(glob.glob(os.path.join(dir,'*P1BS*.ntf')))
        imglist.extend(glob.glob(os.path.join(dir,'*P1BS*.tif')))
        imglist.extend(glob.glob(os.path.join(dir,'*P1BS*[0-9].xml')))
    else:
        #This will pull in M1BS IDs as well
        imglist.extend(glob.glob(os.path.join(dir,'*.ntf')))
        imglist.extend(glob.glob(os.path.join(dir,'*.tif')))
        imglist.extend(glob.glob(os.path.join(dir,'*[0-9].xml')))
    idlist = [get_id(f) for f in imglist] 
    #Remove any None, return a single list of unique strings
    idlist = list(set([item for sublist in filter(None, idlist) for item in sublist]))
    return idlist

#This should only return a single ID, not a list
def get_id(s):
    import re
    idlist = re.findall('10[123456][0-9a-fA-F]+00', s)
    idlist2 = []
    if idlist:
        #Check to make sure we have correct string length - should do this with re, but quickfix
        for id in idlist:
            if len(id) is 16:
                idlist2.append(id)
    return idlist2 

def get_ntflist(dir, id):
    imglist = glob.glob(os.path.join(dir,'*%s*.ntf' % id))
    return imglist

def get_xmllist(dir, id):
    imglist = glob.glob(os.path.join(dir,'*%s*.xml' % id))
    return imglist

def getxml(fn):
    xml_fn = os.path.splitext(fn)[0]+'.xml'
    try:
        with open(xml_fn) as f: pass
    except IOError as e:
        print 'Unable to open xml file associated with %s' % fn
        xml_fn = None 
    return xml_fn

def xml_dt(xml_fn):
    t = getTag(xml_fn, 'FIRSTLINETIME')
    dt = datetime.strptime(t,"%Y-%m-%dT%H:%M:%S.%fZ")
    return dt

def xml_cdt(xml_fn):
    flt = xml_dt(xml_fn)
    nr = float(getTag(xml_fn, 'NUMROWS'))
    #This is lines/second
    alr = float(getTag(xml_fn, 'AVGLINERATE'))
    cdt = flt + timedelta(seconds=nr/alr)
    return cdt

#Use embedded GCPs to construct geometry
#Should be similar to xml2geom, but use gdal metadata handling
#def ntf2geom(ntf_fn):
    #ds = gdal.Open(ntf_fn)
    #metadata

def xml2geom(xml_fn):
    import xml.etree.ElementTree as ET
    tree = ET.parse(xml_fn)
    #There's probably a cleaner way to do this with a single array instead of zipping
    taglon = ['ULLON', 'URLON', 'LRLON', 'LLLON']
    taglat = ['ULLAT', 'URLAT', 'LRLAT', 'LLLAT']
    #dg_mosaic.py doesn't preserve the BAND_P xml tags 
    #However, these are preserved in the STEREO_PAIR xml tags
    #taglon = ['ULLON', 'LRLON', 'LRLON', 'ULLON']
    #taglat = ['ULLAT', 'ULLAT', 'LRLAT', 'LRLAT']
    x = []
    y = []
    for tag in taglon:
        elem = tree.find('.//%s' % tag)
        #NOTE: need to check to make sure that xml has these tags (dg_mosaic doesn't preserve)
        x.append(elem.text)
    for tag in taglat:
        elem = tree.find('.//%s' % tag)
        y.append(elem.text)
    #Want to complete the polygon by returning to first point
    x.append(x[0])
    y.append(y[0])
    geom_wkt = 'POLYGON(({0}))'.format(', '.join(['{0} {1}'.format(*a) for a in zip(x,y)]))
    geom = ogr.CreateGeometryFromWkt(geom_wkt)
    geom.AssignSpatialReference(wgs_srs)
    return geom

def getTag(xml_fn, tag):
    import xml.etree.ElementTree as ET 
    tree = ET.parse(xml_fn)
    #Want to check to make sure tree contains tag
    elem = tree.find('.//%s' % tag)
    if elem is not None:
        return elem.text

def getAllTag(xml_fn, tag):
    import xml.etree.ElementTree as ET 
    tree = ET.parse(xml_fn)
    #Want to check to make sure tree contains tag
    elem = tree.findall('.//%s' % tag)
    return [i.text for i in elem]

#Return the stereo intersection from two xml files
def stereo_intersection_xml_fn(xml1, xml2, bbox=False):
    geom1 = xml2geom(xml1)
    geom2 = xml2geom(xml2)
    igeom = geolib.geom_intersection([geom1, geom2])
    if bbox:
        return igeom.GetEnvelope()
    return igeom 

def stereo_intersection_fn(fn1, fn2, bbox=False):
    ds1 = gdal.Open(fn1)
    ds2 = gdal.Open(fn2)
    return stereo_intersection_ds(ds1, ds2, bbox)

def stereo_intersection_ds(ds1, ds2, bbox=False):
    geom1 = geolib.ds_geom(ds1)
    geom2 = geolib.ds_geom(ds2)
    igeom = geolib.geom_intersection([geom1, geom2])
    if bbox:
        return igeom.GetEnvelope()
    return igeom

#Return a numpy array containing ephemeris table
def getEphem(xml_fn):
    #Get list of strings
    e = getAllTag(xml_fn, 'EPHEMLIST')
    #Could get fancy with structured array here
    #point_num, Xpos, Ypos, Zpos, Xvel, Yvel, Zvel, covariance matrix (6 elements)
    #dtype=[('point', 'i4'), ('Xpos', 'f8'), ('Ypos', 'f8'), ('Zpos', 'f8'), ('Xvel', 'f8') ...]
    #All coordinates are ECF, meters, meters/sec, m^2
    return np.array([i.split() for i in e], dtype=np.float64)

#Return a numpy array containing attitude table 
def getAtt(xml_fn):
    #Get list of strings
    e = getAllTag(xml_fn, 'ATTLIST')
    #point_num, q1, q2, q3, q4, covariance matrix (10 elements)
    return np.array([i.split() for i in e], dtype=np.float64)

#RPC extraction

#RPC forward transform

#RPC inverse transform

#Compute parallax error for a given orbit altidue (H), topographic error (dH) and along-track CCD center separation (L)
#Estimates for WV-2: parallax_err(770000, 100, 0.01)
#Sub-pixel resolution is ~0.1 px
#WV-2 pixel pitch is ~8 microns
#Max topographic error is ~200 m
def parallax_err(H, dH, L):
   import math
   #This is the angular distance in radians
   #return 2*(math.atan2((L/2),H) - math.atan2((L/2),H))
   #This is focal plane distance in m
   #These two are very similar for dH << H
   #For dH toward sensor
   return (L/2.)*dH/H
   #For dH away from sensor
   #return (L/2.)*dH/(H+dH)

#Compute expected vertical error for a give horizontal displacement error
#dx is horiz err in meters
#Could be ortho error, or disparity due to true surface displacement between images
def vert_error(dx, offnadir):
    dz = dx*np.tan(np.radians(offnadir))
    return dz

#Compute dtheta associated with refaction of water to air
def refraction_ang(ema):
    import math
    #Air
    n2 = 1.000293
    #Water at 20 deg C
    n1 = 1.3330
    dtheta = math.radians(ema) - math.asin(math.sin(math.radians(ema))*n1/n2)
    return math.degrees(dtheta)

def toa_rad(xml_fn):
    """Calculate scaling factor for top-of-atmosphere radiance 
    """
    abscal = np.array((getAllTag(xml_fn, 'ABSCALFACTOR')), dtype=float)
    effbw = np.array((getAllTag(xml_fn, 'EFFECTIVEBANDWIDTH')), dtype=float)
    #Multiply L1B DN by this to obatin top-of-atmosphere spectral radiance image pixels 
    toa_rad_coeff = abscal/effbw
    return toa_rad_coeff

def toa_refl(xml_fn, band=None):
    """Calculate scaling factor for top-of-atmosphere reflectance
    """
    #These need to be pulled out by individual band
    sat = getTag(xml_fn, 'SATID')
    if band is None:
        band = getTag(xml_fn, 'BANDID')
    band = band.upper()
    key = '%s_BAND_%s' % (sat, band)
    Esun = EsunDict[key] 
    print key, Esun
    msunel = float(getTag(xml_fn, 'MEANSUNEL'))
    sunang = 90.0 - msunel
    dt = xml_dt(xml_fn) 
    esd = calcEarthSunDist(dt)
    toa_rad_coeff = toa_rad(xml_fn)
    #Unscattered surface-reflected radiation, assuming Lambertian reflecting target, ignoring atmospheric effects
    toa_refl_coeff = toa_rad_coeff * (esd**2 * np.pi) / (Esun * np.cos(np.radians(sunang)))
    return toa_refl_coeff

def calcEarthSunDist(dt):
    """Calculate Earth-Sun distance
    """
    #Astronomical Units (AU), should have a value between 0.983 and 1.017
    year = dt.year
    month = dt.month
    day = dt.day
    hr = dt.hour
    minute = dt.minute
    sec = dt.second
    ut = hr + (minute/60.) + (sec/3600.)
    #print ut
    if month <= 2:
        year = year - 1
        month = month + 12
    a = int(year/100.)
    b = 2 - a + int(a/4.)
    #jd = timelib.dt2jd(dt)
    jd = int(365.25*(year+4716)) + int(30.6001*(month+1)) + day + (ut/24) + b - 1524.5
    g = 357.529 + 0.98560028 * (jd-2451545.0)
    d = 1.00014 - 0.01671 * np.cos(np.radians(g)) - 0.00014 * np.cos(np.radians(2*g))
    return d

#import glob
#for xml_fn in glob.glob('*P001.xml'):
#    c = toa_refl(xml_fn)
#image_calc -c 'var_0*0.00039114' 10400100120C2C00_ortho_0.35m.tif -o 10400100120C2C00_ortho_0.35m_r.tif -d float32

