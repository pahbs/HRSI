#!/usr/bin/env python
# Import and function definitions
import os, sys, osgeo, shutil, csv, subprocess as subp
from osgeo import ogr, osr, gdal
import workflow_functions as wf
import get_stereopairs_v3 as g
import argparse

def runVRT(pairname, rootDir):

    print "\n\n\t Build a VRT of the CLR, DRG and DEM tifs\n\n"

    srcList = ["-DEM-clr-shd.tif", "-DRG.tif", "-DEM.tif"]
    srcFull = ""

    for src in srcList:

        # Find the file using the src string
        for root, dirs, files in os.walk(os.path.join(rootDir,pairname)):
            for fyle in files:
                if fyle.endswith(src):
                    srcFull = os.path.join(rootDir,pairname,fyle)
                    break

        #srcFull = outStereoPre + src

        vrtDir = os.path.join(rootDir, "dem")
        vrtTail = "-DEM.vrt"
        do_kmz = False

        if "clr" in src:
            vrtDir = os.path.join(rootDir, "clr")
            vrtTail = "-CLR.vrt"
            do_kmz = False

        if "DRG" in src:
            vrtDir = os.path.join(rootDir, "drg")
            vrtTail = "-DRG.vrt"
            do_kmz = False

        os.system('mkdir -p %s' % vrtDir)

        dst = os.path.join(vrtDir, pairname + vrtTail)

        if os.path.isfile(dst):
            os.remove(dst)

        if os.path.isfile(srcFull):
            cmdStr = "gdal_translate -of VRT {} {}".format(srcFull, dst)
            cmd = subp.Popen(cmdStr, stdout=subp.PIPE, shell=True)
            print("\tWriting VRT " + dst)

            if do_kmz:
                make_kmz(dst)


def db_query(catIDlist):
    """
    returns a list of lists. Each list is a list of images associated with each catID of input list.
    """

    import psycopg2

    # Establish the database connection
    with psycopg2.connect(database="ngadb01", user="anon", host="ngadb01", port="5432") as dbConnect:

        cur = dbConnect.cursor() # setup the cursor
        sel_list=[]

        """
        Search 1 catID at a time
        """
        # setup and execute the query on both catids of the stereopair indicated with the current line of the input CSV
        for num, catID in enumerate(catIDlist):

            selquery =  "SELECT s_filepath, sensor, acq_time, cent_lat, cent_long FROM nga_inventory WHERE catalog_id = '%s'" %(catID)
            ##preLogText.append( "\n\t Now executing database query on catID '%s' ..."%catID)
            cur.execute(selquery)
            """
            'selected' will be a list of all raw scene matching the catid and their associated attributes that you asked for above
            """
            selected=cur.fetchall()
            sel_list.append(selected)

    return(sel_list)
def copy_over_symlink(file_path, rootDir, subdir):

    from shutil import copyfile

    new_file_path = os.path.join(rootDir,subdir,os.path.split(file_path)[1])
    try:
        copyfile(file_path, new_file_path)
    except Exception, e:
        if os.path.islink(new_file_path):
            os.unlink(new_file_path)
        if not os.path.exists(os.path.join(rootDir,subdir)):
            os.makedirs(os.path.join(rootDir,subdir))
        copyfile(file_path, new_file_path)

    print "\tCopied xml to %s" %(os.path.join(rootDir,subdir,os.path.split(file_path)[1]))
def make_kmz(fn):
    print("\tGenerating kmz")
    kmz_fn = os.path.splitext(fn)[0]+'.kmz'
    #Run in background..
    cmdStr = "gdal_translate -of KMLSUPEROVERLAY -co FORMAT=PNG {} {}".format(fn, kmz_fn)
##    print(' '.join(cmd))
##    subp.call(cmd, shell=False)
    cmd = subp.Popen(cmdStr, stdout=subp.PIPE, shell=True)
    stdOut, err = cmd.communicate()
    print "\tInitialized: %s" %(cmdStr)
    print "\t..Waiting for command to run..."
    print "\t" + str(stdOut) + str(err)
    print "\tEnd of command."

def make_kml(fn):
    print("\tGenerating kml")
    kml_fn = os.path.splitext(fn)[0]+'.kml'

    cmd = ['ogr2ogr', '-of', 'KML', fn, kml_fn]
##    print(' '.join(cmd))
##    subp.call(cmd, shell=False)
    cmd = subp.Popen(cmd, stdout=subp.PIPE, shell=True)
def runVALPIX(root, pairname, src, newFieldsList, newAttribsList, outSHP):

        # -- Update Valid Pixels Shapefile
        # Updates a merged SHP of all valid pixels from individual DSM strips

        print "\tRunning valid pixel footprints...\n"

        pairnameDir = os.path.join(root,pairname)

##        if"AST_" in pairname:
##            src = os.path.join(pairnameDir,'outASP',src)
##        else:
##            src = os.path.join(pairnameDir,src)

        #print("\t Source file: " + src + "\n")
        outValTif_TMP = os.path.join(pairnameDir, "VALIDtmp.tif")
        outValShp_TMP = os.path.join(pairnameDir, "VALIDtmp.shp")
        outValShp     = os.path.join(pairnameDir, "VALID.shp")
        outValShp_prj = os.path.join(pairnameDir, "VALID_WGS84.shp")
        outValShp_aggtmp = os.path.join(pairnameDir, "VALID_aggtmp.shp")
        outValShp_agg = os.path.join(pairnameDir, "VALID_agg.shp")

        if not os.path.isfile(src):
            print "\tWill not footprint: %s does not exist." %src
        else:
            #print " [1] Create *VALID.shp files for each pairname"
            #print "     [.a] Coarsen %s" %src
            if 'clr-shd' in src:
                #Band 4 is the alpha channel
                cmdStr = "gdal_translate -outsize 1% 1% -co compress=lzw -b 4 -ot byte -scale 1 1 {} {}".format(src, outValTif_TMP)
            else:
                cmdStr = "gdal_translate -outsize 1% 1% -co compress=lzw -b 1 -ot byte -scale 1 1 {} {}".format(src, outValTif_TMP)
            wf.run_wait_os(cmdStr,print_stdOut=False)
            #print "     [.b] POLYGONIZE %s" %outValTif_TMP
            cmdStr = "gdal_polygonize.py {} -f 'ESRI Shapefile' {}".format(outValTif_TMP, outValShp_TMP)
            wf.run_wait_os(cmdStr,print_stdOut=False)

            #print "     [.c] REMOVE NODATA HOLES: %s" %outValShp
            cmdStr = "ogr2ogr {} {} -where 'DN>0' -overwrite".format(outValShp, outValShp_TMP)
            wf.run_wait_os(cmdStr,print_stdOut=False)

            #print " [2] Reproject to WGS and merge"
            cmdStr = "ogr2ogr -f 'ESRI Shapefile' -t_srs EPSG:3995 {} {} -overwrite".format(outValShp_prj, outValShp)
            wf.run_wait_os(cmdStr,print_stdOut=False)

            #print " [3] Dissolve/Aggregate Polygons into 1 feature"
            input_basename = os.path.split(outValShp_prj)[1].replace(".shp","")
            cmdStr = "ogr2ogr {} {} -dialect sqlite -sql 'SELECT GUnion(geometry), DN FROM {} GROUP BY DN'".format(outValShp_aggtmp, outValShp_prj, input_basename)
            wf.run_wait_os(cmdStr,print_stdOut=False)
            #print " [4] Simplify"
            cmdStr = "ogr2ogr {} {} -simplify .001".format(outValShp_agg, outValShp_aggtmp)
            wf.run_wait_os(cmdStr,print_stdOut=False)

            # Check to see if the pairname exists in the main shp
            update = True
            mainVALID = os.path.join(root,outSHP)
            if os.path.isfile(mainVALID):
                # Open a Shapefile, and get field names
                shp = ogr.Open(mainVALID, 1)
                layer = shp.GetLayer()
                layer_defn = layer.GetLayerDefn()
                field_names = [layer_defn.GetFieldDefn(i).GetName() for i in range(layer_defn.GetFieldCount())]

                for feature in layer:
                    if feature.GetField('pairname') == pairname:
                        update = False
                        print "\n\n\t\t !! Don't update; pairname exists in shapefile: %s" %pairname
                        break
                    pass
                shp, layer, layer_defn, field_names, feature = (None for i in range(5))

            if update and os.path.isfile(outValShp_agg):
                # Add fields to the pairname's shp (outValShp_agg)
                ##https://gis.stackexchange.com/questions/3623/how-to-add-custom-feature-attributes-to-shapefile-using-python
                # Open a Shapefile, and get field names
                shp = ogr.Open(outValShp_agg, 1)
                layer = shp.GetLayer()

                for newfieldStr in newFieldsList:
                    if 'pair' in newfieldStr or 'left' in newfieldStr or 'right' in newfieldStr:
                        fieldType = ogr.OFTString
                    elif 'year' or 'month' or 'day' in newfieldStr:
                        fieldType = ogr.OFTInteger
                    else:
                        fieldType = ogr.OFTReal
                    # Add a new field
                    new_field = ogr.FieldDefn(newfieldStr, fieldType)
                    layer.CreateField(new_field)

                # Update fields based on attributes
                ## http://www.digital-geography.com/create-and-edit-shapefiles-with-python-only/#.V8hlLfkrLRY
                layer_defn = layer.GetLayerDefn()
                field_names = [layer_defn.GetFieldDefn(i).GetName() for i in range(layer_defn.GetFieldCount())]
                feature = layer.GetFeature(0)   # Gets first, and only, feature

                for num, f in enumerate(field_names):
                    i = feature.GetFieldIndex(f)
                    if num > 0:
                        feature.SetField(i, newAttribsList[num-1])
                        layer.SetFeature(feature)
                shp = None

                # Append pairname shp to main shp
                if os.path.isfile(mainVALID):
                    print "\n\t Updating footprint shp %s with current %s" %(mainVALID,pairname)
                    cmdStr = "ogr2ogr -f 'ESRI Shapefile' -update -append " + mainVALID + " " + outValShp_agg  #-nln merge
                    wf.run_wait_os(cmdStr,print_stdOut=False)
                else:
                    print "\n\t Creating footprint shp: %s" %mainVALID
                    cmdStr = "ogr2ogr -f 'ESRI Shapefile' " + mainVALID + " " + outValShp_agg
                    wf.run_wait_os(cmdStr,print_stdOut=False)

                # Clean up tmp files
                ##cmd = "find %s -type f -name VALID* -exec rm -rf {} \;" %pairnameDir
                files = os.listdir(pairnameDir)
                for f in files:
                    if "VALID" in os.path.join(pairnameDir,f):
            	       os.remove(os.path.join(pairnameDir,f))
                       ##print("\t Removed: "+ f)

def getparser():
    parser = argparse.ArgumentParser(description="Create footprints of DSMs with sun-surface-target geometry attributes from XMLs")
    parser.add_argument('-out_root', default=None, help='Specify HRSI DSM root dir')
    parser.add_argument('-out_shp', default='hrsi_dsm_footprints', help='Output shp of footprints')
    parser.add_argument('--str_fp', type=str, default='', help='String indicating the file to be footprinted')
    parser.add_argument('-kml', action='store_true', help='Output kml of footprints for Google Earth')
    return parser

def main(outRoot, outShp, str_fp):
    """
    outRoot     eg, outASP dir
    outShp      the name (not full path) of output DSM footprint shapefile
    str_fp      a string indicating the file to be footprinted, otherwise, footprint the clr-shd

    Find all DSM in subdirs of an outRoot dir.
    Create a single footprint shapefile, KML
    Create individual VRTs for each pairname (DEM, CLR, DRG)
    """
    from os import listdir
    import get_stereopairs_v3 as g

    parser = getparser()
    args = parser.parse_args()

    outRoot = args.out_root
    kml = args.kml
    outShp = args.out_shp
    str_fp = args.str_fp

    DSMincomplete = []      # list of incomplete DSMs (subdirs exist, but interrupted processing)
    DSMcatIDfail = []       # list of subdirs with at least 1 catID not found --> send to Julien
    catIDfails = []         # list of catIDs not found
    catIDsuccess = []       # list of catIDs found in my dB that werent in NGA db
    DSMfootprintFail = []   # list of DSMs that seem ok but failed to get footprinted
    DSMok = False
    have_info = False

    hdr, line = ('' for z in range(2))
    i = 0
    for root, subdirs, files in os.walk(outRoot):

        for subdir in subdirs:

            outASPdir = os.path.join(outRoot,subdir)
            ##print '\n\tDSM dir: %s' %(outASPdir)
            print "-------------"
            # Look for clr-shd: If exists, then DSM was likely output ok
            for root2, dirs, subdirfiles in os.walk(outASPdir):
                for each in subdirfiles:

                    # Make sure clr-shd exists, indicating the DEM has been fully processed
                    if 'DEM-clr-shd.tif' in each:
                        print "\tClr-shd exists. DSM can be footprinted."
                        clrShd = os.path.join(root2,each)
                        DSMok = True
                        break

            # Find the actual file you want to footprint (in case of ASTER, its the *proj.tif one level up
            if str_fp == '':
                print "\n\tSource for footprint is CLR: %s" %clrShd
            else:
                for root3, dirs, subdirfiles in os.walk(outASPdir):
                    for each in subdirfiles:
                        if str_fp in each:
                            src_fp = os.path.join(root3,each)
                            print "\n\tSource for footprint: %s" %src_fp
                            break


            if not DSMok:
                print "\n\tNo DSM yet for %s" %(subdir)
                DSMincomplete.append(subdir)
                DSMok = False

            if DSMok:
                print '\tDSM dir: %s' %(outASPdir)
                # Get the outASP subdir of WV DSMs
                isASTER = False
                if subdir.startswith('AST'):
                    outASPdir = os.path.join(outRoot,subdir,'outASP')

                    print '\n\tASTER L1A DSM dir: %s' %(outASPdir)
                    have_info = True
                    isASTER = True

                if subdir.startswith('GE') or subdir.startswith('WV'):

                    print "\n\t%s DSM." %(subdir[0:4])

                    # Copy the XMLs to inASP
                    catID_1 = subdir.split('_')[2]
                    catID_2 = subdir.split('_')[3]

                    # Get image-level & stereopair acquisition info
                    try:
                        print "\n\t[1] Trying to calc DSM attributes using XMLs in current dir."
                        c,b,a,hdr,line = g.stereopairs(outASPdir)
                        have_info = True

                    except Exception, e:
                        print "\n\t First try: stereo angles NOT calc'd."
                        print "\t Querying the NGA db for each catID..."

                        if not os.path.isdir(outASPdir):
                            os.mkdir(outASPdir)

                        for num, catID in enumerate([catID_1,catID_2]):
                            # Query the db
                            sList = db_query([catID])
                            print "\tcatID is %s" %(catID)

                            if len(sList[0]) == 0:
                                print "\n\tFailed to find %s in personal dir" %(catID)
                                catIDfails.append(catID)
                            else:
                                catIDsuccess.append(catID)
                                # Get file_paths of all images assoc'd with catID
                                for numimg, img in enumerate(range(0,len(sList[num-1])-1)):
                                    print "\tScene number is %s" %(numimg)
                                    file_path = sList[num-1][numimg-1][0]   # third position is the file_path
                                    file_path = file_path.replace('.ntf','.xml').replace('.tif','.xml')
                                    # Function to copy XML from NGA dB into inASP
                                    copy_over_symlink(file_path, outRoot, subdir)

                        # Get image-level & stereopair acquisition info
                        try:
                            print "\n\t[2] Trying again to calc DSM attributes, after NGA dB query."
                            c,b,a,hdr,line = g.stereopairs(outASPdir)
                            have_info = True

                        except Exception, e:
                            print "\n\tStereo angles not calc'd b/c there is no input for both catIDs. You're done with this one."
                            DSMcatIDfail.append(subdir)
                            have_info = False

            if have_info:
                # Reconfigure hdr and attribute line
                hdr = 'pairname,year,month,day,' + hdr
                if isASTER:
                    pairname    = os.path.basename(os.path.dirname(outASPdir))
                    valpixroot  = os.path.dirname(os.path.dirname(outASPdir))
                else:
                    pairname    = os.path.basename(outASPdir)
                    valpixroot  = os.path.dirname(outASPdir)
                try:
                    year        = pairname.split('_')[1].rstrip()[0:-4]
                    month       = pairname.split('_')[1].rstrip()[4:-2]
                    day         = pairname.split('_')[1].rstrip()[6:]
                except Exception,e:
                    year, month, day = ('' for i in range(3))

                line = pairname + ',' + year + ',' + month + ',' + day + ',' + line

                try:
                    print "\n\tBegin shapefile processing of DSM footprints"

                    runVALPIX(valpixroot, pairname, src_fp, hdr.split(','), line.split(','), outShp)
                    i = i + 1
                    print "\n\t\t >>>>> Success on # %s: Done with %s " %(i,pairname)
                except Exception,e:
                    print "\n\t\t >>>>> Fail on # %s: Could not get footprint of %s" %(i,pairname)
                    DSMfootprintFail.append(subdir)

            # Make VRTs, even if you dont have_info
            if DSMok:

                runVRT(pairname,outRoot)
                print "\n\n\n"

    # Output a CSV of:
    # [1] catIDs not found in nga db or personal
    # [2] catIDs not found in nga db but successfully found in personal db
    # [2] incomplete DSM dirs
    # [3] DSMs with at least 1 failed catID searches
    # [4] failed DSM footprints to the same dir as the output Shapefile (outASP)

    outCSVFileStrings = ['_failed_find_catID.csv', '_success_find_catID_personal.csv', '_failed_inc_DSM.csv', '_failed_find_DSMcatID.csv', '_failed_DSM_foots.csv']
    failList = [catIDfails, catIDsuccess, DSMincomplete, DSMcatIDfail, DSMfootprintFail]
    for num, outStr in enumerate(outCSVFileStrings):
        # Ouput a CSV, 1 line for each fail
        with open(os.path.join(outRoot,outShp.split('.')[0] + outStr), 'wb') as outCSV:
            for failline in failList[num]:
                outCSV.write(failline + '\n')
    if kml:
        make_kml(os.path.join(outRoot,outShp))

if __name__ == "__main__":
    import sys
    main(sys.argv[1], sys.argv[2], sys.argv[3])