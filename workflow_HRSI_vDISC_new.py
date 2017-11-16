#-------------------------------------------------------------------------------
# Name:         workflow_HRSI_v18.py
# Purpose:      science...
#               Refer to HRSI Workflow GSlides for diagram of this process
# Author:       paul.m.montesano@nasa.gov
#               301.614.6642
#
# Created:     June 9 2015
# Modified by:
#       Stephanie Miller    Aug 2015    : takes UTM zone from csv file; timeit
#       PM                  Aug 2015    : separated stereo from p2d; kicked off hillshade & colormap simultaneously; cleaned up
#       PM                  Sep 2015    : added strip boolean, dg_mosaic, wv_correct & functions 'wf.run_os' and 'wf.run_wait_os'
#       PM                  Oct 2015    : put into functions--> p2d, vrt stuff
#       PM                  Nov 2015    : v10 --> choose nodes_list based on launch node
#       PM                              : v11 --> set up runPair boolean; removed os 'wait' in run_parstereo
#       PM                  Dec 2015    : v12 --> changed runStereo to doStereo; made doP2D boolean
#       PM                  Feb 2016    : v12 --> try for NTF and TIF input to dg_mosaic; try for diff image_date formats; update for UTM south or north; update to tak in .ntf, .tif using list of possible input Exts
#       PM                  Apr 2016    : v13 --> wv_correct moved; clean-up
#       PM                  Apr 2016    : v14 --> point2dem adjusted
#       PM                  Aug 2016    : v15 --> image discovery done in searchDir will be main NGA dir; subseq cors and mosiacs will be written to a dir created in our space.
#                           Sep 2016    : v17 --> dB query with run_asp()
#       PM                  Sep 2016    : v18 --> if par = False, then do only stereo_pprc
#-------------------------------------------------------------------------------

# 11/15/2017: Creating this to incorporate the new workflow. That is, bulk of proocessing will take place in the dg_stereo.sh script
#             Also updating to account for new changes in query: new dir structure; input text list of pairs instead of csv
#             See #n tag




# #!/bin/python
# #!/usr/bin/env python

#!/usr/local/other/SSSO_Ana-PyD/4.2.0_py2.7_gcc-5.3-sp3/bin/python
###############################################
# Import and function definitions
import os, sys, math, osgeo, shutil, time, glob, platform, csv, subprocess as subp
from osgeo import ogr, osr, gdal
from datetime import datetime
from timeit import default_timer as timer
from time import gmtime, strftime
gdal.AllRegister() #register all raster format drivers
import workflow_functions as wf
#import psycopg2 # only need this for query
import LLtoUTM as convert
import get_stereopairs_v3 as g
import shapefile
from distutils.util import strtobool

##def run_parstereo(par, nodesList, imagePairs, imagePair_xmls, outStereoPre, mapprjDEM, test=False, mapprj=False):
def find_elapsed_time(start, end): # take two timer() objects and find elapsed time between them
    elapsed_min = (end-start)/60
    return float(elapsed_min)


def run_stereo(par, nodesList, imagePairs, imagePair_xmls, outStereoPre, DEM, mapprj, test):
    start_ps = timer()
    """
    Try to OPTIMIZE this step

    """
##    cmdStr = "parallel_stereo --nodes-list=" + nodesList + " --processes 18 --threads-multiprocess 16 --threads-singleprocess 32 --corr-timeout 360 --job-size-w 6144 --job-size-h 6144 " + imagePairs + imagePair_xmls + outStereoPre
    print("\n\tRunnning stereo on images: " + imagePairs)

    if test:    # TEST 'STEREO' on a small window
        cmdStr = "stereo --threads 18 --subpixel-kernel 9 9 --corr-timeout 300 --left-image-crop-win 5000 80000 10000 10000 " + imagePairs + imagePair_xmls + outStereoPre
        if 'GE01' in imagePairs: # if the images have sensor GE01, change command string to add -t rpc
            cmdStr = "stereo -t rpc --threads 18 --subpixel-kernel 9 9 --corr-timeout 300 --left-image-crop-win 5000 80000 10000 10000 " + imagePairs + imagePair_xmls + outStereoPre
    else:       # STEREO
        cmdStr = "stereo --threads 18 --subpixel-kernel 9 9 --corr-kernel 21 21 {} {} {}".format(imagePairs, imagePair_xmls, outStereoPre) # 2/13 edited test call per Paul gchat
        if 'GE01' in imagePairs: # if the images have sensor GE01, change command string to add -t rpc
            cmdStr = "stereo -t rpc --threads 18 --subpixel-kernel 9 9 --corr-kernel 21 21 {} {} {}".format(imagePairs, imagePair_xmls, outStereoPre)


    print "\n\t" + cmdStr

    wf.run_wait_os(cmdStr, print_stdOut=False)

def runP2D(outStereoPre, prj, strip=True):

    # Define P2D files:
    colormapFile = '/discover/nobackup/projects/boreal_nga/code/color_hrsi_dsm.txt' #hardcode

    PC_tif = '{}-PC.tif'.format(outStereoPre)
    DEM_tif = '{}-DEM.tif'.format(outStereoPre)
    DEM_txt = DEM_tif.replace('tif', 'txt')
    DEM_vrt = DEM_tif.replace('tif', 'vrt')
    hillshade_tif = '{}-DEM-hlshd-e25.tif'.format(outStereoPre)
    hillshade_txt = hillshade_tif.replace('tif', 'txt')
    colorshade_tif = '{}-DEM-clr-shd.tif'.format(outStereoPre)
    colorshade_txt = colorshade_tif.replace('tif', 'txt')
    DRG_tif = '{}-DRG.tif'.format(outStereoPre)


    # [5.4] point2dem
    if os.path.isfile(PC_tif) and not os.path.isfile(DEM_txt):
        # Output DSM has holes <50 pix filled with hole-fill-mode=2 (weighted avg of all valid pix within window of dem-hole-fill-len)
        # Ortho (-DRG.tif) produced

        print("\n\t [1] Create DEM: runnning point2dem on: {}".format(PC_tif))
        start_p2d = timer()
        #cmdStrDEM = "point2dem --threads=0 --t_srs " + prj + " --nodata-value -99 --dem-hole-fill-len 50 " + outStereoPre + "-PC.tif -o " + outStereoPre + "-holes-fill"	## --orthoimage --errorimage " + outStereoPre + "-L.tif"        ## -r earth
        cmdStrDEM = "point2dem --threads=0 --t_srs {} --nodata-value -99 --dem-hole-fill-len 50 {} -o {}".format(prj, PC_tif, outStereoPre)
        print("\n\t{}".format(cmdStrDEM)) #DEL (above command)
        p2dCmd2 = subp.Popen(cmdStrDEM.rstrip('\n'), stdout=subp.PIPE, shell=True)

        print("\n\t [2] Create Ortho Image")
        #cmdStrOrthoImage = "point2dem --threads=0 --t_srs " + prj + " --no-dem --nodata-value -99 --dem-hole-fill-len 50 " + outStereoPre + "-PC.tif -o " + outStereoPre + "-holes-fill --orthoimage " + outStereoPre + "-L.tif"
        cmdStrOrthoImage = "point2dem --threads=0 --t_srs {} --no-dem --nodata-value -99 --dem-hole-fill-len 50 {} -o {} --orthoimage {}-L.tif".format(prj, PC_tif, outStereoPre, outStereoPre)
        #print("\n\t{}".format(cmdStrOrthoImage)) #DEL gets printed in wf.run_os
        wf.run_os(cmdStrOrthoImage)
        """
        Not sure if I still want an Error Image...
        """
##        print("\n\t [3] Create Error Image")
##        #cmdStrErrorImage = "point2dem --threads=0 --t_srs " + prj + " --no-dem --nodata-value -99 --dem-hole-fill-len 50 " + outStereoPre + "-PC.tif -o " + outStereoPre + "-holes-fill --errorimage "
##        cmdStrErrorImage = "point2dem --threads=0 --t_srs {} --no-dem --nodata-value -99 --dem-hole-fill-len 50 {} -o {} --errorimage ".format(prj, PC_tif, outStereoPre)
##        #print("\n\t{}".format(cmdStrErrorImage)) #DEL get printed in wf.run_os
##        wf.run_os(cmdStrErrorImage)

##    # Communicate p2d holes
##    if os.path.isfile(outStereoPre + "-PC.tif") and not os.path.isfile(outStereoPre + "-holes-DEM.tif"):
##        stdOut, err = p2dCmd1.communicate()
##        print(str(stdOut) + str(err))
##        end_p2d = timer()
##        print("\n\tpoint2dem (#1) run time (mins): %f . Completed %s" \
##        % (   (end_p2d - start_p2d)/60, str(datetime.now() ) ))

    # Communicate p2d holes-fill
    #if os.path.isfile(outStereoPre + "-PC.tif") and not os.path.isfile(outStereoPre + "-holes-fill-DEM.txt"):
    if os.path.isfile(PC_tif) and not os.path.isfile(DEM_txt):
        stdOut, err = p2dCmd2.communicate()
        print(str(stdOut) + str(err))
        end_p2d = timer()
        print("\n\t point2dem run time (mins): %f . Completed %s" \
        % (   (end_p2d - start_p2d)/60, str(datetime.now() ) ))

        if str(stdOut) != "None":
            #with open(outStereoPre + "-holes-fill-DEM.txt",'w') as out_hf_txt:
            with open(DEM_txt,'w') as out_hf_txt:
                out_hf_txt.write("DEM processed")
    else: # else holes-fill-DEM does exist
        print "\n\tDEM already exists"

    # Avoid 'TIFF file size exceeded' errors with reduced-size Hillshades & VRTs
    # Logic to build VRTs from strip-holes-fill-DEM and the hillshade at 50% resolution in order to run colormap successfully
    if not os.path.isfile(DEM_vrt): #* 2/16 deleting if strip because strip will always be True now
        print("\n\tLaunching gdal_translate to create out-DEM.vrt ")
        #cmdStr = "gdal_translate -outsize 30% 30% -of VRT " + outStereoPre + "-holes-fill-DEM.tif " + outStereoPre + "-holes-fill-DEM.vrt"
        cmdStr = "gdal_translate -outsize 30% 30% -of VRT {} {}".format(DEM_tif, DEM_vrt)
        #print("\n\t{}".format(cmdStr)) #DEL gets printed below
        wf.run_wait_os(cmdStr)
    else: # else if the file exists
        print '\n\t{} already exists'.format(DEM_vrt)

    # [5.5]  gdaldem stuff to create viewable output: hillshade on the reduced DEM VRT
   # if os.path.isfile(outStereoPre + "-PC.tif") and not os.path.isfile(outStereoPre + "-holes-fill-DEM-hlshd-e25.txt"):
    if os.path.isfile(PC_tif) and not os.path.isfile(hillshade_txt):

        print("hillshade")
        #cmdStr = "hillshade  " + outStereoPre + "-holes-fill-DEM.vrt -o  " + outStereoPre + "-holes-fill-DEM-hlshd-e25.tif -e 25"
        #cmdStr = "hillshade  {0}-holes-fill-DEM.vrt -o  {0}-holes-fill-DEM-hlshd-e25.tif -e 25".format(outStereoPre)
        cmdStr = "hillshade  {} -o  {} -e 25".format(DEM_vrt, hillshade_tif)
        hshCmd = subp.Popen(cmdStr.rstrip('\n'), stdout=subp.PIPE, shell=True)
        stdOut_hill, err_hill = hshCmd.communicate()
        print(str(stdOut_hill) + str(err_hill))

        if str(stdOut_hill) != "None":
            #with open(outStereoPre + "-holes-fill-DEM-hlshd-e25.txt",'w') as out_hill_txt:
            with open(hillshade_txt,'w') as out_hill_txt:
                out_hill_txt.write("hillshade processed")
    else: # else hillshade does exist
        print "\n\thillshade already exists"

    #if os.path.isfile(outStereoPre + "-holes-fill-DEM.vrt") and os.path.isfile(outStereoPre + "-holes-fill-DEM-hlshd-e25.tif") and not os.path.isfile(outStereoPre + "-holes-fill-DEM-clr-shd.txt"):
    if os.path.isfile(DEM_vrt) and os.path.isfile(hillshade_tif) and not os.path.isfile(colorshade_txt):
        print("colormap")
        #cmdStr = "colormap  " + outStereoPre + "-holes-fill-DEM.vrt -s " + outStereoPre + "-holes-fill-DEM-hlshd-e25.tif -o " + outStereoPre + "-holes-fill-DEM-clr-shd.tif" + " --colormap-style " + colormapFile
        #cmdStr = "colormap  {0}-holes-fill-DEM.vrt -s {0}-holes-fill-DEM-hlshd-e25.tif -o {0}-holes-fill-DEM-clr-shd.tif --colormap-style {1}".format(outStereoPre,  colormapFile)
        cmdStr = "colormap  {} -s {} -o {} --colormap-style {}".format(DEM_vrt, hillshade_tif, colorshade_tif, colormapFile)
        clrCmd = subp.Popen(cmdStr.rstrip('\n'), stdout=subp.PIPE, shell=True)
        stdOut_clr, err_clr = clrCmd.communicate()
        print(str(stdOut_clr) + str(err_clr))

        if not "None" in str(stdOut_clr):
            #with open("{}-holes-fill-DEM-clr-shd.txt".format(outStereoPre),'w') as out_clr_txt:
            with open(colorshade_txt,'w') as out_clr_txt:
                out_clr_txt.write("colormap processed")
    else: # else colorshade text DOES exist
        print "\n\tcolorshade already exists"

    print("\n\tLaunch gdaladdo")
    print("\n\tKick off nearly simultaneously; i.e. dont wait for the first gdaladdo output to be communicated before launching the second")
    #pyrcmdStr1 = "gdaladdo -r average {} 2 4 8 16".format(DEM_tif)
    pyrcmdStr1 = "gdaladdo -ro -r average --config COMPRESS_OVERVIEW LZW --config BIGTIFF_OVERVIEW YES {} 2 4 8 16 32 64".format(DEM_tif) # new command format
    pyrcmdStr2 = "gdaladdo -ro -r average --config COMPRESS_OVERVIEW LZW --config BIGTIFF_OVERVIEW YES {} 2 4 8 16 32 64".format(colorshade_tif)
    pyrcmdStr3 = "gdaladdo -ro -r average --config COMPRESS_OVERVIEW LZW --config BIGTIFF_OVERVIEW YES {} 2 4 8 16 32 64".format(DRG_tif)

    # Initialize gdaladdos by scene for 1) -DEM.tif, 2) -DEM-clr-shd.tif, 3) -DRG.tif
    pyrCmd1 = subp.Popen(pyrcmdStr1.rstrip('\n'), stdout=subp.PIPE, shell=True)
    pyrCmd2 = subp.Popen(pyrcmdStr2.rstrip('\n'), stdout=subp.PIPE, shell=True)
    pyrCmd3 = subp.Popen(pyrcmdStr3.rstrip('\n'), stdout=subp.PIPE, shell=True)
    print("\n\tDon't communicate gdaladdos")
    stdOut_pyr1, err_py1 = pyrCmd1.communicate()
##    print(str(stdOut_pyr1) + str(err_py1))
    stdOut_pyr2, err_py2 = pyrCmd2.communicate()    # The clr-shd needs to finish before being used by gdal_polygonize
##    print(str(stdOut_pyr2) + str(err_py2))
    stdOut_pyr3, err_py3 = pyrCmd3.communicate()
##    print(str(stdOut_pyr3) + str(err_py3))

    # Remove txt files
    try:
        print "\n\tRemoving asp log txt files..."

        #cmdStr ='rm ' + outStereoPre + '-log*txt'
        cmdStr = 'rm {}-log*txt'.format(outStereoPre)
        Cmd1 = subp.Popen(cmdStr, stdout=subp.PIPE, shell=True)
    except Exception,e:
        print "\tDidn't remove asp log txt files."
    """
    Also, remove all the subdirs and other intermediate created from the stereo run
    """

def runVRT(outStereoPre,
            root#,
##            newFieldsForVALPIX,
##            newAttributesForVALPIX
            ):

    # Build a VRT of the strip clr-shd file
    # Build a VRT of the DRG file
    # Update footprints of each with gdaltindex
    # Note: VRTs need absolute paths!

    # --CLR
    srcSHD = outStereoPre + "-DEM-clr-shd.tif"
    #path = os.path.join(root,"vrt_clr_v7")
    clrpath = os.path.join(root, "clr")
    os.system('mkdir -p %s' % clrpath)
    dst = os.path.join(clrpath, outStereoPre.split('/')[-2] + '_' + outStereoPre.split('/')[-1] + "-DEM-clr-shd.vrt")
    if os.path.isfile(dst):
        os.remove(dst)
    cmdStr = "gdal_translate -of VRT " + srcSHD + " " + dst
    wf.run_os(cmdStr)
    print("\tWriting VRT " + dst)

    ## Update clr index shapefile
    #cmdStr = "gdaltindex -t_srs EPSG:4326 " + path + "clr_index.shp " + dst
    #run_os(cmdStr)

    # --DRG
    srcDRG = outStereoPre + "-DRG.tif"
    drgpath = os.path.join(root, "drg")
    os.system('mkdir -p %s' % drgpath)
    dst = os.path.join(drgpath, outStereoPre.split('/')[-2] + '_' + outStereoPre.split('/')[-1] + "-DRG.vrt")
    if os.path.isfile(dst):
        os.remove(dst)
    cmdStr = "gdal_translate -of VRT " + srcDRG + " " + dst
    wf.run_os(cmdStr)
    print("\tWriting VRT " + dst)

    # --Update DRG index shapefile
    #cmdStr = "gdaltindex -t_srs EPSG:4326 " + path + "drg_index.shp " + dst
    #run_os(cmdStr)

    # --DSM
    srcDEM = outStereoPre + "-DEM.tif"
    dempath = os.path.join(root, "dem")
    os.system('mkdir -p %s' % dempath)
    dst = os.path.join(dempath, outStereoPre.split('/')[-2] + '_' + outStereoPre.split('/')[-1] + "-DEM.vrt")
    if os.path.isfile(dst):
        os.remove(dst)
    cmdStr = "gdal_translate -of VRT " + srcDEM + " " + dst
    wf.run_os(cmdStr)

    print("\tWriting VRT " + dst)
    #print("\t ---------------")


    """
    runASP() is the main function that wraps the AMES Stereo Pipeline  processing steps that:
        1. Takes a csv footprint file input
        2. Gets the catIDs of stereo strips
        3. Finds all images belonging to each stereo strip
        4. runs wv_correct and then mosaics them to strip pairs (*dg_mosaic)
        5. processes these strip pairs: *mapproject, *parallel_stereo or stereo, point2dem
        6. adds the areas (polygon) of processed data to a shp that holds all valid areas for all processed DSMs (like an index of what we've processed)

        Args:

            csv             = full path and name to the csv file from the footprint shapefile of stereo pairs
            outDir          = an output dir used to place all ASP generated content
            #searchDir       = (DONT NEED THIS ANYMORE with SQL database query) a top level dir under which all stereo pairs will be found
            inDir           = dir in nobackup into which subdirs for each input stereo pair will be made for storing the symbolic link files and intermediate mosaics
            nodeList        = list of processing nodes for parallel_stereo
            mapprj          = boolean indicating whether or not to run mapproject (default = True)
            mapprjRes       = res for mapprocted input to par_stereo
            par             = run parallel_stereo instead of stereo (default = True)

            strip           = run stereo on image strips (default = True)
            searchExtList   = all extensions possible for input imagery
            csvSplit        = boolean indicating whether or not the run is associated with multiple smaller subset csv files derived from a larger main csv file
            doP2D           = boolean for running point2dem
            stereoDef       = the 'stereo.default' file that specifies the stereo processing parameters for ASP
            ##mapprjDEM       = an ASTER or SRTM dem to pre-align the stereo strips before stereo run
            prj             = projection for the mapproject run  --- needs to be specified uniquely for each
            test            = run stereo with a test window (default = False)
            rp              = for strip processing, spatial resolution reduce-by percent (default = 100; no reduction)

    """

def run_asp(
    pairname, #n
    header, #n will not need this
    imageDir,     ##  ='/att/gpfsfs/userfs02/ppl/pmontesa/inASP/
    mapprj,
    prj,
    utm_zone,
    doP2D,
    rp, # reduce percent value (100 when making real runs)
    preLogTextFile,
    batchID, # file arg to be written to list
    catIDlist, # list as string
    pIDlist, # list as string
    imageDate, # imageDate is a string in the format YYYY-mm-dd. to convert to datetime object: datetime.strptime(imageDate, "%Y-%m-%d")
    stereoDef='/discover/nobackup/projects/boreal_nga/code/stereo.default',
    searchExtList=['.ntf','.tif','.NTF','.TIF']
    ):


    start_main = timer()

    # hardcode stuff for now
    nodeName = platform.node()
    par = False #DEL or edit these 3 lines of code depending on what we wanna do with stereo
    nodesList = '/att/gpfsfs/home/pmontesa/code/nodes_{}'.format(nodeName)
    test = False
    found_catID = [True, True] # hardcode for now, should always be the case if we are running workflow

##    # first we need to convert all list-->str arguments back to list
##    header = header.split('::join::')
##    linesplit = line.strip().split(',')
    catIDlist = catIDlist.split('::join::')
    pIDlist = pIDlist.split('::join::')

    # also need to read preLogText file into list
    with open(preLogTextFile, 'r') as tf:
        preLogText = tf.read()

    # convert boolean vars to boolean
    mapprj = bool(strtobool(mapprj)) # strtobool converts to 1 or 0, bool converts to True, False
    doP2D = bool(strtobool(doP2D))

    # set up directories and other variables from inputs
    outDir = os.path.dirname(imageDir).replace('inASP/batch{}'.format(batchID), 'outASP') # don't want the batch subdir for outASP. This does not include the pairname # out imageDir will be outASPcur
    pairname = os.path.basename(imageDir)

    mapprjDEM = os.path.join(imageDir, "dem-%s.tif" % pairname) # may or may not exist

    # get date components from imageDate
    year = imageDate.split('-')[0]
    month = imageDate.split('-')[1]
    day = imageDate.split('-')[2]

    # For logging on the fly
    logdir = os.path.join(outDir, 'logs')
    os.system('mkdir -p %s' % logdir) # make log dir if it doesn't exist
    start_time = strftime("%Y%m%d-%H%M%S")
    lfile = os.path.join(outDir, 'logs', 'run_asp_LOG_%s__batch%s_%s_%s.txt' % (pairname, batchID, start_time, nodeName)) #* 2/8: putting date/time before node so it's in chrono order

    so = se = open(lfile, 'w', 0)                       # open our log file
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0) # re-open stdout without buffering
    os.dup2(so.fileno(), sys.stdout.fileno())           # redirect stdout and stderr to the log file opened above
    os.dup2(se.fileno(), sys.stderr.fileno())

    # print some things to the log file
    print "--LOGFILE------------------"
    print(lfile)
    print "\n"
    print "--PYTHON FILE-----------"
    print os.path.basename(__file__)
    print "\n"

    # print input parameters to log file:
    print '-------runASP parameters:-------'
    print 'mapprj = {}'.format(mapprj)
    print 'doP2D = {}'.format(doP2D)
    print 'test = {}'.format(test)
    print 'rp = {}'.format(rp)
    print 'batchID = {}'.format(batchID)
    print 'imageDir = {}'.format(imageDir)
    print '\nBEGIN: {}\n\n'.format(start_time)

    print preLogText #mw 1/25: preLogText is now one big string
    print "\n"

    # get the header indices, try using pairname field first
    pairname_idx = -999 # -999 is the new false. This will be overwritten if pairname_idx does exist in the input line
    try:
        pairname_idx = header.index('pairname')
    except ValueError:
        catID_1_idx = header.index('catalogid')
        catID_2_idx = header.index('stereopair')
        sensor_idx  = header.index('platform')
        imageDate_idx  = header.index('acqdate')

    avSunElev_idx   = header.index('avsunelev')
    avSunAzim_idx   = header.index('avsunazim')
    avOffNadir_idx  = header.index('avoffnadir')
    avTargetAz_idx  = header.index('avtargetaz')

    # also get the information from the line using the indices from above
    if pairname_idx != -999: # if there is a pairname idx, it will be something other than -999
        pairname = linesplit[pairname_idx]
        catID_1    = linesplit[pairname_idx].split('_')[2]
        catID_2    = linesplit[pairname_idx].split('_')[3]
        sensor     = linesplit[pairname_idx].split('_')[0]
        #imageDate  = linesplit[pairname_idx].split('_')[1] # already got imageDate in query and passed it along

    else:
        catID_1 = linesplit[catID_1_idx]
        catID_2 = linesplit[catID_2_idx]
        sensor = str(linesplit[sensor_idx])
        #imageDate  = linesplit[imageDate_idx]

    # keep all of this the same
    avSunElev  = round(float(linesplit[avSunElev_idx]),0)
    avSunAz    = round(float(linesplit[avSunAzim_idx]),0)
    avOffNadir = round(float(linesplit[avOffNadir_idx]),0)
    avTargetAz = round(float(linesplit[avTargetAz_idx]),0)
    if avTargetAz <= 180:
        avSatAz = avTargetAz + 180
    else:
        avSatAz = avTargetAz - 180

    print "________________________"
    print "><><><><><><><><><><><><"
    print("\n\tACQ_DATE in line is: %s" % (str(imageDate)))##(line.split(',')[2]))
    try:
        print("\tutm_zone = " + utm_zone)
    except Exception,e:
        print("\tutm_zone = " + 'NA')
    print "\tSun Elev Angle = %s" %avSunElev

    DSMdone = False

    # Get stereo geometry angles
    conv_ang, bie_ang, asym_ang = ("" for i in range(3))
    try:
        print "\n\tStereo angles calc output:"
        conv_ang, bie_ang, asym_ang, hdr, attrbs = g.stereopairs(imageDir) #q this function returns pairname, output csv file, and the three angles, OK? Also giving error below. Why?

    except Exception, e:
        print "\n\tStereo angles not calculated because there is no input for both catIDs"

    print "\n\n\tWorking on scenes in dir: %s\n" % imageDir

    # Go to the dir that holds all the indiv pairs associated with both stereo strips
    os.chdir(imageDir)

    # Set up data for stereo processing
    imagePairs, imagePair_xmls = ("" for i in range(2))                     # This will hold each image used in the parallel_stereo command

    # [5.1]Initialize Lists: For processing image pairs
    sceneNTFList, sceneXMLList, sceneTIFList, stripList, inSearchCatList = ([] for i in range(5))
    corExt = '_cor.tif'

    imageExt = '.tif'
    dgCmdList = []

    # Establish stripList with each catalog ID of the pair
    fullPathStrips = [] # (4/4/17) also create a list to hold the full paths of the output strips for recording purposes
    for catNum, catID in enumerate(catIDlist): # catNum is 0 or 1, catID is the corresponding catID
        print "\n\tCATID:", catID

        # Set search string
        end = ""
        raw_imageList = []
        cor_imageList = []

        # On a catID: Get all raw images for wv_correct
        for root, dirs, files in os.walk(imageDir): # root is imageDir, dirs is subsirs in imageDir (none)

            for searchExt in searchExtList:
                for each in files:
                    #print each #DEL
                    if each.endswith(searchExt) and 'P1BS' in each and catID in each and pIDlist[catNum] in each:
                        raw_imageList.append(each) # if file in imageDir fits the bill, add it to the list

        print("\tProduct ID for raw images: " + str(pIDlist[catNum]))
        print("\tRaw image list: " + str(raw_imageList))

        # On a catID: Prep for dg_mosaic: This is the output strip prefix
        #outPref = sensor.upper() + "_" + imageDate.strftime("%y%b%d").upper() + "_" + catID ## e.g., WV01_JUN1612_102001001B6B7800

        outPref = sensor.upper() + "_" + datetime.strptime(imageDate, "%Y-%m-%d").strftime("%y%b%d").upper() + "_" + catID ## e.g., WV01_JUN12_102001001B6B7800 # 2/2 had to change because imageDate is str
        outStrip = outPref + '.r' + str(rp) + imageExt

##        print "outStrip:", outStrip                                      ## e.g., WV01_JUN1612_102001001B6B7800.r100.tif
        fullPathStrips.append(os.path.abspath(outStrip))
        stripList.append(outStrip)
        #print "\n\tCatID: " + catID, "(like it here better?)"

        # On a catID: If the mosaic already exists, dont do it again, dummy
        #q is there a better way to do this? why do we do this here? if dg_mosaic already exists, can we skip to the next catID-nothing else needs to be done?
        if os.path.isfile("dg_mosaic_done_strip1_rp{}.txt".format(rp)) and os.path.isfile("dg_mosaic_done_strip2_rp{}.txt".format(rp)): #q why are we testing for both 1 and 2 right now? confused- is 1 and 2 the 2 catIDs or what ?
            print("\n\t Mosaic strip already exists: " + outStrip)
            dg_mos = False
            continue # skip to the next catID #q see above confusion

        # On a catID: Get seachExt for wv_correct and dg_mosaic
        wv_cor_cmd = False
        """
        We may have solved this problem of communicating in serial the wv_correct Cmds that were run in near-parallel
        """
        wvCmdList = []

        for imgNum, raw_image in enumerate(raw_imageList):
            for searchExt in searchExtList:
                if searchExt in raw_image and not 'cor' in raw_image:
                    cor_imageList.append(raw_image.replace(searchExt, corExt))
                    break

            if 'WV01' in sensor or 'WV02' in sensor:
            # --------
            # wv_correct loop
                try:
                    cmdStr ="wv_correct --threads=4 " + raw_image + " " + raw_image.replace(searchExt, ".xml") + " " + raw_image.replace(searchExt, corExt)
                    Cmd = subp.Popen(cmdStr.rstrip('\n'), stdout=subp.PIPE, shell=True)
                    wv_cor_cmd = True
                    wvCmdList.append(Cmd)

                    # Make copies of xmls to match *cor.tif
                    shutil.copy(raw_image.replace(searchExt,".xml"), raw_image.replace(searchExt,corExt.replace('.tif',".xml")))

                except Exception, e:
                    #print '\t' + str(e)
                    print "\n\t Tried using this extension: " + searchExt

        if not wv_cor_cmd: #q what is this?
            print "\tRaw image search extension for dg_mosaic: %s: " % searchExt

        # On a catID: Communicate the wv_correct cmd
        if wv_cor_cmd:
            wvc_cnt = 0
            start_wvc = timer()
            #print wvCmdList #DEL
            for num, c in enumerate(wvCmdList):
                s,e = c.communicate()
                print("\n\tRunnning wv_correct on raw image: " + raw_image) #* 2/8 mw it makes more sense to have this down here right?
                print "\twv_correct run # %s" %num
                ##print "\twv_correct output:"
                ##print "\tStandard out: %s" %str(s) #q do we not want this as well?
                print "\t\tStandard error: %s" %str(e)
                if not "None" in str(s): wvc_cnt += 1 # if the command ran, add to counter
            end_wvc = timer()
            print "\n\tElapsed time to run worldview correct {} times: {} minutes".format(wvc_cnt, round(find_elapsed_time(start_wvc, end_wvc), 2))
        # --------
        # On a catID: dg_mosaic    This has to one once for each of the image strips.
        dg_mos = False

        if (not os.path.isfile(outStrip) ): # is this 'if' even necessary now?

            # Create a seach string with catID and extension
            if 'WV01' in sensor or 'WV02' in sensor:
                inSearchCat = "*{}*{}".format(catID, corExt) # i.e. *10200100406E4500*_cor.tif

            else:
                inSearchCat = "*{}*-P1BS*{}*{}".format(catID, pIDlist[catNum], searchExt) # i.e. (made up) *10200100406E4500*-P1BS*500455526030*.tif
                print "\tSensor is %s so wv_correct was not run" %(sensor)

            try:
                #q i feel like we should have something else here like preparing the dg mosaic command sor something
                cmdStr = "dg_mosaic {} --output-prefix {} --reduce-percent={}".format(inSearchCat, outPref, rp)
                Cmd = subp.Popen(cmdStr.rstrip('\n'), stdout=subp.PIPE, shell=True)
                dg_mos = True
                dgCmdList.append(Cmd)
            except Exception, e:
                print '\t' + str(e)

    #  If true, communicate both dg_mosaic before proceeding
    if dg_mos:
        start_dgm = timer()
        dgm_cnt = 0
        for num, Cmd in enumerate(dgCmdList):
            s,e = Cmd.communicate()
            print("\n\n\tRunnning (and waiting) dg_mosaic on catID: " + catID) #q doesn't this make more sense to be here? this is where i actually runs
            print "\n\tFinal dg_mosaic output:" #q ?? should have something here or no?
            print "\tStandard out: %s" %str(s)
            print "\tStandard error: %s" %str(e)

            # Write txt file to indicate that dg_mosaic completed successfully
            if not "None" in str(s):
                with open(os.path.join(imageDir, "dg_mosaic_done_strip{}_rp{}.txt".format(num+1, rp)),'w') as out_mos_txt:
                    out_mos_txt.write("dg_mosaic processed")
                dgm_cnt += 1

        end_dgm = timer()
        print "\n\tElapsed time to run dgmosaic {} times: {} minutes".format(dgm_cnt, find_elapsed_time(start_dgm, end_dgm))

    print "\n\tNow, delete *cor.tif files (space management)..."
##    print "\timageDir: %s" % imageDir
    corList = glob.glob("*cor.*")
    print "\tNumber of cor files to delete: {}".format(len(corList)) #q is this ok instead
    for f in corList:
        os.remove(f)
        print "\tDeleted %s" %f

    runPair=True # Set up boolean to execute ASP routines after finding pairs

    print("\n\tStripList: %s" %(stripList))

    for outStrip in stripList: # looping thru the left and right mosaics

        outStrip_xml = outStrip.replace('.tif', '.xml')
        if os.path.isfile(outStrip_xml):
            print("\tFile exists: {}".format(outStrip_xml))
        else:

            print("\n\t !!! -- Looks like mosaic does not exist: " + outStrip )
            print("\n\t The pair is incomplete, so no stereo dataset will come from this dir.")
            print("\n\t Ending job for pair {}".format(pairname))
            mapprj=False
            DSMdone=False
            runPair=False

    ##------------------------------------------------------------------
    ##              Set up list of pairs
    ##------------------------------------------------------------------
    if runPair:
        """
        Now, you need to get:
            imagePairs
            imagePair_xmls
        """

        leftScene = stripList[0]
        rightScene = stripList[1]
        imagePairs = leftScene + " " + rightScene + " "
        imagePair_xmls = leftScene.replace(imageExt,'.xml') + " " + rightScene.replace(imageExt,'.xml') + " "
        print("\n--------------")
        print("\tImage pairs for stereo run: " + imagePairs)
        print("\tNodelist = " + nodesList)
        print("--------------")

        # [5.3] Prep for Stereo
        #
        ## Still in dir of data...
        # Copy stereo.default file in home dir to current dir
        #shutil.copy(stereoDef,os.getcwd()) #* #q #Q BE SURE TO UNCOMMENT IF NECESSARY *HERE

        #outASPcur will look like this: outASP/batch$batchID/WV01_20150610_102001003EBA8900_102001003E8CA400
        outASPcur = os.path.join(outDir, 'batch{}'.format(batchID), pairname) # outASP/pairname:

        #outStereoPre looks like this: outASP/batch$batchID/WV01_20150610_102001003EBA8900_102001003E8CA400/out-strip
        outStereoPre = outASPcur + "/out-strip"
        doStereo = False

        # If outASPcur doesnt yet exist, run stereo
        if not os.path.isdir(outASPcur):
            os.makedirs(outASPcur) #* 1/22 added, don't we need to make the directory if it doesn't exist?
            doStereo = True
        else:
            if len(glob.glob("{}*".format(outStereoPre))) == 0:
                doStereo = True

        # If PC file doesnt exist, run stereo. Sometimes PC file may exist, but is incomplete.
        PC_tif = "{}-PC.tif".format(outStereoPre) # this will be created with stereo process
        if not os.path.isfile(PC_tif):
            doStereo = True

        #print("--------------")
        print("\n\n\t Do stereo? " + str(doStereo))
        if doStereo:
            print("\t\tparallel? " + str(par) )
            print("\t\tmapproject? " + str(mapprj) )
        print("\n\n--------------")

        if doStereo:

            if mapprj:
                """
                resetting imagePairs var when they are being replaced by the mapprojected images
                imagePair_xmls doesnt need to be changed
                """
                imagePairs = ""
                CmdList = []
                """
                replace leftscene and rightScene vars with the corresponding vars from StripList
                """
                mp_start = timer()
                for num, unPrj in enumerate([os.path.join(imageDir,leftScene), os.path.join(imageDir, rightScene)]):

                    print "\nRunning mapproject on strip %s..." %(num + 1) #q but this isn't actually running yet, it's just preparing the commands
                    mapPrj_img = unPrj.replace('.tif', '_mapprj.tif')
                    if not os.path.exists(mapPrj_img):
                        cmdStr = "mapproject --nodata-value=-99 -t rpc {} {} {} {}".format(mapprjDEM, unPrj, unPrj.replace('tif','xml'), mapPrj_img)

                        print "\n\t" + cmdStr
                        Cmd = subp.Popen(cmdStr.rstrip('\n'), stdout=subp.PIPE, shell=True)

                        CmdList.append(Cmd) # Hold Cmd in a list for later communication

                    imagePairs += mapPrj_img + " "


                start_mp = timer()
                mp_cnt = 0
                for num, c in enumerate(CmdList): # may be no need to enumerate this: see wv_correct block
                    # Now communicate both so they have been launched in parallel but will be reported in serial
                    print "\n\tWaiting to communicate results of mapprojects..."
                    stdOut, err = CmdList[num].communicate()
                    print "ERROR here:", err #DEL later?
                    print "\n\tCommunicating output from mapproject %s:"%str(num+1)
                    print "\t" + str(stdOut) + str(err)
                    print("\tEnd of mapproject %s"%str(num+1))
                if not "None" in str(stdOut): mp_cnt += 1
                end_mp = timer()
                print "\nElapsed time to run mapproject {} times: {} minutes\n".format(mp_cnt, find_elapsed_time(start_mp, end_mp))

            # we're probably gonna set par=False and no run parallel_stereo; stereo instead
            start_stereo = timer()
            print '\nBegin Stereo processing: %s\n' % (datetime.now().strftime("%I:%M%p  %a, %m-%d-%Y"))
            run_stereo(par, nodesList, imagePairs, imagePair_xmls, outStereoPre, mapprjDEM, mapprj, test) #* fix this function call and function
            #if os.path.isfile(mapprjDEM): # need this ?
            #    os.remove(mapprjDEM)
            end_stereo = timer()
            print "End Stereo processing: %s" % (datetime.now().strftime("%I:%M%p  %a, %m-%d-%Y"))
            print "Elapsed time to run stereo: {} minutes\n".format(round(find_elapsed_time(start_stereo, end_stereo),3))

        if doP2D and os.path.isfile(PC_tif):
            print("\nRunning p2d function...\n")
            start_p2d = timer()
            runP2D(outStereoPre, prj) #* 1/22: removing strip=True assuming we don't need it anymore
            end_p2d = timer()
            print "\nElapsed time to run point2dem: {} minutes\n".format(round(find_elapsed_time(start_p2d, end_p2d),3))

            if os.path.isfile("{}-DEM.tif".format(outStereoPre)):
                DSMdone = True

            outAttributes = pairname + "," + str(found_catID[0]) + "," + str(found_catID[1]) + "," + str(mapprj) + "," + str(year) + "," + str(month) + "," + str(avSunElev)+ "," + str(avSunAz) + "," + str(avOffNadir) + "," + str(avTargetAz) + "," + str(avSatAz) + "," +str(conv_ang) + "," + str(bie_ang) + "," + str(asym_ang) + "," + str(DSMdone) +"\n"
            outAttributesList = outAttributes.rstrip().split(',')

##            if DSMdone:
##                print("\nRunning VRT function...\n")
##                start_vrt = timer()
##                runVRT(outStereoPre, outDir)#, outHeaderList, outAttributesList)
##                end_vrt = timer()
##                print "\nElapsed time to run VRT: {} minutes\n".format(round(find_elapsed_time(start_vrt, end_vrt),3))
##            else:
##                print("\n\tVRTs not done b/c DSM not done. Moving on...")
        else:
            print("\n\tNo PC.tif file found")

    # remove all the unneeded files, but first cd to the outASP/batch/pairname
    os.chdir(outASPcur)
    print "\n\nDeleting the following files from %s:" % os.getcwd()
    os.system('find . -type f -name "*.tif" ! -newer out-strip-F.tif ! -iname "out-strip-L.tif"') # all tiffs F.tif, EXCLUDING anything newer than F.tif and L.tif

    cmdDelete = 'find . -type f -name "*.tif" ! -newer out-strip-F.tif ! -iname "out-strip-L.tif" -exec rm -rf {} \;'
    print "Deleted files using command:", cmdDelete
    os.system(cmdDelete)

    # now copy all of the input XMLs into the outASP/batch/pairname
    globXMLs = os.path.join(imageDir, '*P1BS*.xml') # this will exclude the mosaic xml's, which don't have P1BS in the name
    print '\n Copying XML files ({}) to outASP ({})'.format(globXMLs, outASPcur)
    os.system('cp {} {}'.format(globXMLs, outASPcur))

    # also move the slurm.out file(s) to outASP/slurmOuts/batchID and rename them to the pairname_slurm.out
    # loop through all slurm files in pairname directory and rename/copy them to outSlurm dir -- if we are rerunning a pair process it will just name/recopy the slurm.out files to outSlurm
    inSlurmGlob = glob.glob(os.path.join(imageDir, 'slurm*out')) # list of all slurm files in pairname dir
    outSlurmDir = os.path.join(outDir, 'outSlurm') # doing just one big dir for outSlurm now. files will have batch names in them though
    os.system('mkdir -p {}'.format(outSlurmDir))
    print ''
    for inSlurm in inSlurmGlob: # loop through slurm files
        outSlurm = os.path.join(outSlurmDir, os.path.basename(inSlurm).replace("slurm", "batch{}__{}__slurm".format(batchID, pairname)))
        print " Copying outSlurm file {} to a new name {}".format(inSlurm, outSlurm)
        os.system('cp {} {}'.format(inSlurm, outSlurm)) # review this after we are sure it works

    print "\n Adding pair {} to completedPairs text file and recording run time information to spreadsheet".format(pairname)

    # we got to this point, append the pairname to the completed pairs text file
    if doP2D and os.path.isfile(PC_tif): # but only add to list if PC exists
        comp_pair_dir = os.path.join(outDir, 'completedPairs')
        os.system('mkdir -p %s' % comp_pair_dir)
        completed_pairs_txt = os.path.join(comp_pair_dir, 'batch{}_completedPairs.txt'.format(batchID))
        with open (completed_pairs_txt, 'a') as cp:
            cp.write('{}\n'.format(pairname))

    end_main = timer()
    total_time = find_elapsed_time(start_main, end_main)

    # add some info to the run_times csv for NCCS
    # then print batchID, pairname, total_time (minutes and hours) to csv
    strip1size = round(os.path.getsize(fullPathStrips[0])/1024.0/1024/1024, 3)
    strip2size = round(os.path.getsize(fullPathStrips[1])/1024.0/1024/1024, 3)
    run_times_csv = os.path.join(outDir, 'run_times.csv')
    with open(run_times_csv, 'a') as rt:
        rt.write('{}, {}, {}, {}, {}, {}, {}\n'.format(batchID, pairname, total_time, (total_time/60), strip1size, strip2size, nodeName))

    print("\n\n-----------------------------")
    print("\n\t ")
    print("Finished processing {}".format(pairname))
    print 'End time: {}'.format(strftime("%Y%m%d-%H%M%S"))
    print "Elapsed time = {} minutes".format(round(total_time, 3))
    print("\n\t ")
    print("-----------------------------\n\n")


    # try to close the out/err files-- http://stackoverflow.com/questions/7955138/addressing-sys-excepthook-error-in-bash-script
    try:
        sys.stdout.close()
    except:
        pass
    try:
        sys.stderr.close()
    except:
        pass


if __name__ == "__main__":
    #import sys
    # get variables being passed along from query_db and run_asp with them
    #run_asp( sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8]  )
    # TESTING passing vars thru
    run_asp( sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6],  sys.argv[7], sys.argv[8],  sys.argv[9], sys.argv[10], sys.argv[11], sys.argv[12], sys.argv[13] ) # 13 arguments (plus python script)



