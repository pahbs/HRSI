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
# #!/bin/python
# #!/usr/bin/env python
###############################################
# Import and function definitions
import os, sys, math, osgeo, shutil, time, glob, platform, csv, subprocess as subp
from osgeo import ogr, osr, gdal
from datetime import datetime
from timeit import default_timer as timer
from time import gmtime, strftime
gdal.AllRegister() #register all raster format drivers
import workflow_functions as wf
import psycopg2
import LLtoUTM as convert
import get_stereopairs_v3 as g
import shapefile
from distutils.util import strtobool

##def run_parstereo(par, nodesList, imagePairs, imagePair_xmls, outStereoPre, mapprjDEM, test=False, mapprj=False):
def run_stereo(par, nodesList, imagePairs, imagePair_xmls, outStereoPre, DEM, mapprj, test=False):
    start_ps = timer()
    """
    Try to OPTIMIZE this step

    """
    cmdStr = "parallel_stereo --nodes-list=" + nodesList + " --processes 18 --threads-multiprocess 16 --threads-singleprocess 32 --corr-timeout 360 --job-size-w 6144 --job-size-h 6144 " + imagePairs + imagePair_xmls + outStereoPre

    if par:         # PARALLEL_STEREO
        print("\n\tRunnning stereo on images: " + imagePairs)
        if mapprj:
            cmdStr = cmdStr + " " + DEM
        else:
            cmdStr = cmdStr
    else:
        cmdStr = "stereo --threads 18 --corr-timeout 360 " + imagePairs + imagePair_xmls + outStereoPre
##        if test:    # TEST 'STEREO' on a small window
##            outStereoPre = outStereoPre + "-sub"
##            cmdStr = "stereo --threads 18 --corr-timeout 300 --left-image-crop-win 10000 15000 3000 3000 " + imagePairs + imagePair_xmls + outStereoPre
##        else:       # STEREO
##            cmdStr = "stereo --threads 18 --corr-timeout 360 " + imagePairs + imagePair_xmls + outStereoPre

    print "\n\t" + cmdStr

    wf.run_wait_os(cmdStr, print_stdOut=False)

    end_ps = timer()
    print("\n\n\tEnd stereo run ")
    print("\tStereo run time (decimal minutes): " + str((end_ps - start_ps)/60) )

def runP2D(outStereoPre, prj, strip=True):

    # [5.4] point2dem
    # Launch p2d holes-fill
    if os.path.isfile(outStereoPre + "-PC.tif") and not os.path.isfile(outStereoPre + "-holes-fill-DEM.txt"):
        # Output DSM has holes <50 pix filled with hole-fill-mode=2 (weighted avg of all valid pix within window of dem-hole-fill-len)
        # Ortho (-DRG.tif) produced

        print("\n\t [1] Create DEM: runnning point2dem (holes-fill) on: " + outStereoPre + "-PC.tif")
        start_p2d = timer()
        cmdStrDEM = "point2dem --threads=18 --t_srs " + prj + " --nodata-value -99 --dem-hole-fill-len 50 " + outStereoPre + "-PC.tif -o " + outStereoPre + "-holes-fill"	## --orthoimage --errorimage " + outStereoPre + "-L.tif"        ## -r earth
        print("\n\t" + cmdStrDEM )
        p2dCmd2 = subp.Popen(cmdStrDEM.rstrip('\n'), stdout=subp.PIPE, shell=True)

        print("\n\t [2] Create Ortho Image")
        cmdStrOrthoImage = "point2dem --threads=18 --t_srs " + prj + " --no-dem --nodata-value -99 --dem-hole-fill-len 50 " + outStereoPre + "-PC.tif -o " + outStereoPre + "-holes-fill --orthoimage " + outStereoPre + "-L.tif"
        wf.run_os(cmdStrOrthoImage)
        """
        Not sure if I still want an Error Image...
        """
        print("\n\t [3] Create Error Image")
        cmdStrErrorImage = "point2dem --threads=18 --t_srs " + prj + " --no-dem --nodata-value -99 --dem-hole-fill-len 50 " + outStereoPre + "-PC.tif -o " + outStereoPre + "-holes-fill --errorimage "
        wf.run_os(cmdStrErrorImage)

##    # Communicate p2d holes
##    if os.path.isfile(outStereoPre + "-PC.tif") and not os.path.isfile(outStereoPre + "-holes-DEM.tif"):
##        stdOut, err = p2dCmd1.communicate()
##        print(str(stdOut) + str(err))
##        end_p2d = timer()
##        print("\n\tpoint2dem (#1) run time (mins): %f . Completed %s" \
##        % (   (end_p2d - start_p2d)/60, str(datetime.now() ) ))

    # Communicate p2d holes-fill
    if os.path.isfile(outStereoPre + "-PC.tif") and not os.path.isfile(outStereoPre + "-holes-fill-DEM.txt"):
        stdOut, err = p2dCmd2.communicate()
        print(str(stdOut) + str(err))
        end_p2d = timer()
        print("\n\t point2dem (w/o DRG and ErrorImage) run time (mins): %f . Completed %s" \
        % (   (end_p2d - start_p2d)/60, str(datetime.now() ) ))
        ##print("Total ASP run time for this scene (mins): " + str((end_p2d - start_ps)/60) )

        if str(stdOut) != "None":
            with open(outStereoPre + "-holes-fill-DEM.txt",'w') as out_hf_txt:
                out_hf_txt.write("holes-fill-DEM processed")

    # Avoid 'TIFF file size exceeded' errors with reduced-size Hillshades & VRTs
    # Logic to build VRTs from strip-holes-fill-DEM and the hillshade at 50% resolution in order to run colormap successfully
    if strip:
        print("\n\tLaunching gdal_translate to create out-strip-holes-fill-DEM.vrt ")
        cmdStr = "gdal_translate -outsize 30% 30% -of VRT " + outStereoPre + "-holes-fill-DEM.tif " + outStereoPre + "-holes-fill-DEM.vrt"
        wf.run_wait_os(cmdStr)

    # [5.5]  gdaldem stuff to create viewable output: hillshade on the reduced DEM VRT
    if os.path.isfile(outStereoPre + "-PC.tif") and not os.path.isfile(outStereoPre + "-holes-fill-DEM-hlshd-e25.txt"):

        print("hillshade")
        cmdStr = "hillshade  " + outStereoPre + "-holes-fill-DEM.vrt -o  " + outStereoPre + "-holes-fill-DEM-hlshd-e25.tif -e 25"
        print(cmdStr)
        hshCmd = subp.Popen(cmdStr.rstrip('\n'), stdout=subp.PIPE, shell=True)
        stdOut_hill, err_hill = hshCmd.communicate()
        print(str(stdOut_hill) + str(err_hill))

        if str(stdOut_hill) != "None":
            with open(outStereoPre + "-holes-fill-DEM-hlshd-e25.txt",'w') as out_hill_txt:
                out_hill_txt.write("hillshade processed")

    if os.path.isfile(outStereoPre + "-holes-fill-DEM.vrt") and os.path.isfile(outStereoPre + "-holes-fill-DEM-hlshd-e25.tif") and not os.path.isfile(outStereoPre + "-holes-fill-DEM-clr-shd.txt"):
        print("colormap")
        cmdStr = "colormap  " + outStereoPre + "-holes-fill-DEM.vrt -s " + outStereoPre + "-holes-fill-DEM-hlshd-e25.tif -o " + outStereoPre + "-holes-fill-DEM-clr-shd.tif" + " --colormap-style /att/gpfsfs/home/pmontesa/code/color_lut_v7.txt"
        print(cmdStr)
        clrCmd = subp.Popen(cmdStr.rstrip('\n'), stdout=subp.PIPE, shell=True)
        stdOut_clr, err_clr = clrCmd.communicate()
        print(str(stdOut_clr) + str(err_clr))

        if not "None" in str(stdOut_clr):
            with open(outStereoPre + "-holes-fill-DEM-clr-shd.txt",'w') as out_clr_txt:
                out_clr_txt.write("colormap processed")

    print("\n\tLaunch gdaladdo")
    print("\n\tKick off nearly simultaneously; i.e. dont wait for the first gdaladdo output to be communicated before launching the second")
    pyrcmdStr1 = "gdaladdo -r average " + outStereoPre + "-holes-fill-DEM.tif 2 4 8 16"
    pyrcmdStr2 = "gdaladdo -r average " + outStereoPre + "-holes-fill-DEM-clr-shd.tif 2 4 8 16"
    pyrcmdStr3 = "gdaladdo -r average " + outStereoPre + "-holes-fill-DRG.tif 2 4 8 16"

    # Initialize gdaladdos by scene for 1) -DEM.tif, 2) -DEM-clr-shd.tif, 3) -DRG.tif
    pyrCmd1 = subp.Popen(pyrcmdStr1.rstrip('\n'), stdout=subp.PIPE, shell=True)
    pyrCmd2 = subp.Popen(pyrcmdStr2.rstrip('\n'), stdout=subp.PIPE, shell=True)
    pyrCmd3 = subp.Popen(pyrcmdStr3.rstrip('\n'), stdout=subp.PIPE, shell=True)
    print("\n\tDon't communicate gdaladdos")
##    stdOut_pyr1, err_py1 = pyrCmd1.communicate()
##    print(str(stdOut_pyr1) + str(err_py1))
    stdOut_pyr2, err_py2 = pyrCmd2.communicate()    # The clr-shd needs to finish before being used by gdal_polygonize
##    print(str(stdOut_pyr2) + str(err_py2))
##    stdOut_pyr3, err_py3 = pyrCmd3.communicate()
##    print(str(stdOut_pyr3) + str(err_py3))

    # Remove txt files
    try:
        print "\tRemoving asp log txt files..."
        cmdStr ='rm ' + outStereoPre + '-log*txt'
        Cmd1 = subp.Popen(cmdStr, stdout=subp.PIPE, shell=True)
    except Exception,e:
        print "\tDidn't remove asp log txt files."
    """
    Also, remove all the subdirs and other intermediate created from the stereo run
    """
def footprint_dsm(outRoot, inRoot):
        """
        Find all DSM in subdirs of an outRoot dir.
        Find matching input files in corresponding dirs of inRoot
        Gather list of image level attributes
        Output to shp with runVALPIX
        """
        from os import listdir
        import get_stereopairs_v3 as g

        for subdir in os.listdir(outRoot):

            # Get the outASP dir
            outASPdir = os.path.join(outRoot,subdir)

            print '\n\tHRSI DSM dir: %s' %(outASPdir)

            # Look for clr-shd: If exists, then DSM was likely output ok
            for root, dirs, files in os.walk(outASPdir):
                for each in files:
                    if 'holes-fill-DEM-clr-shd' in each:
                        print '\n\tDEM and Color-shaded relief exist'
                        DSMok = True

            if DSMok:

                # Get the inASP dir
                inASPdir = os.path.join(inRoot,subdir)

                if os.path.isdir(inASPdir):

                    c,b,a,hdr,line = g.stereopairs(inASPdir)

                    runVALPIX(outASPdir+'/out-strip', outASPdir, hdr, line, 'outASP_test.shp')






def runVALPIX(outStereoPre, root, newFieldsList, newAttribsList, outSHP):
        # -- Update Valid Pixels Shapefile
        # Updates a merged SHP of all valid pixels from individual DSM strips
        #   example:
        #       outStereoPre --> /att/nobackup/pmontesa/outASP/WV01_20130617_1020010022894400_1020010022BB6400/out-strip
        # [1] Create out-strip-holes-fill-DEM-clr-shd_VALID.shp files for each strip
        srcSHD = outStereoPre + "-holes-fill-DEM-clr-shd.tif"
        outValTif_TMP = os.path.join(root,outStereoPre.split('/')[-2], "VALIDtmp.tif")
        outValShp_TMP = os.path.join(root,outStereoPre.split('/')[-2], "VALIDtmp.shp")
        outValShp     = os.path.join(root,outStereoPre.split('/')[-2], "VALID.shp")
        outValShp_prj = os.path.join(root,outStereoPre.split('/')[-2], "VALID_WGS84.shp")
        outValShp_agg = os.path.join(root,outStereoPre.split('/')[-2], "VALID_agg.shp")
        # Coarsen the CLR-SHD
    	cmdStr = "gdal_translate -outsize 1% 1% -co compress=lzw -b 4 -ot byte -scale 1 1 "  + srcSHD + " " + outValTif_TMP
        wf.run_wait_os(cmdStr)
        # POLYGONIZE
    	cmdStr = "gdal_polygonize.py " + outValTif_TMP + " -f 'ESRI Shapefile' " + outValShp_TMP
        wf.run_wait_os(cmdStr)
        # REMOVE NODATA HOLES
    	cmdStr = "ogr2ogr " + outValShp + " " + outValShp_TMP + " -where 'DN>0' -overwrite"
        wf.run_wait_os(cmdStr)

        # [2] Reproject to WGS and merge
        cmdStr = "ogr2ogr -f 'ESRI Shapefile' -t_srs EPSG:4326 " + outValShp_prj + " " + outValShp + " -overwrite"
        wf.run_wait_os(cmdStr)

        # [3] Dissolve/Aggregate Polygons into 1 feature
        input_basename = os.path.split(outValShp_prj)[1].replace(".shp","")
        cmdStr = "ogr2ogr " + outValShp_agg + " " + outValShp_prj + " -dialect sqlite -sql 'SELECT ST_Union(geometry), DN FROM " + input_basename + " GROUP BY DN'"
        wf.run_wait_os(cmdStr)

        # Add fields
        ##https://gis.stackexchange.com/questions/3623/how-to-add-custom-feature-attributes-to-shapefile-using-python
        # Open a Shapefile, and get field names
        shp = ogr.Open(outValShp_agg, 1)
        layer = shp.GetLayer()

        for newfieldStr in newFieldsList:
            # Add a new field
            new_field = ogr.FieldDefn(newfieldStr, ogr.OFTString)
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

        mainVALID = os.path.join(root,outSHP)
        if os.path.isfile(mainVALID):
            print("\n\t Updating VALID shp with this individual file...")
            cmdStr = "ogr2ogr -f 'ESRI Shapefile' -update -append " + mainVALID + " " + outValShp_agg  #-nln merge
            wf.run_wait_os(cmdStr)
        else:
            print("\n\t Creating a main VALID shp...")
            cmdStr = "ogr2ogr -f 'ESRI Shapefile' " + mainVALID + " " + outValShp_agg
            wf.run_wait_os(cmdStr)

        # Clean up tmp files
        cleanUPdir = os.path.join(root,outStereoPre.split('/')[-2])
        files = os.listdir(cleanUPdir)
        for f in files:
            if "VALID" in os.path.join(cleanUPdir,f):
    	       os.remove(os.path.join(cleanUPdir,f))
               print("\t Removed: "+ f)

def runVRT(outStereoPre,
            root,
            newFieldsForVALPIX,
            newAttributesForVALPIX
            ):

    # Build a VRT of the strip clr-shd file
    # Build a VRT of the DRG file
    # Update footprints of each with gdaltindex
    # Note: VRTs need absolute paths!

    # --CLR
    srcSHD = outStereoPre + "-holes-fill-DEM-clr-shd.tif"
    path = os.path.join(root,"vrt_clr_v7")
    dst = os.path.join(path, outStereoPre.split('/')[-2] + '_' + outStereoPre.split('/')[-1] + "-holes-fill-DEM-clr-shd.vrt")
    if os.path.isfile(dst):
        os.remove(dst)
    cmdStr = "gdal_translate -of VRT " + srcSHD + " " + dst
    wf.run_os(cmdStr)
    print("\tWriting VRT " + dst)

    ## Update clr index shapefile
    #cmdStr = "gdaltindex -t_srs EPSG:4326 " + path + "clr_index.shp " + dst
    #run_os(cmdStr)

    # --DRG
    srcDRG = outStereoPre + "-holes-fill-DRG.tif"
    path = os.path.join(root,"vrt_drg")
    dst = os.path.join(path, outStereoPre.split('/')[-2] + '_' + outStereoPre.split('/')[-1] + "-holes-fill-DRG.vrt")
    if os.path.isfile(dst):
        os.remove(dst)
    cmdStr = "gdal_translate -of VRT " + srcDRG + " " + dst
    wf.run_os(cmdStr)
    print("\tWriting VRT " + dst)

    # --Update DRG index shapefile
    #cmdStr = "gdaltindex -t_srs EPSG:4326 " + path + "drg_index.shp " + dst
    #run_os(cmdStr)

    runVALPIX(outStereoPre,root,newFieldsForVALPIX, newAttributesForVALPIX, "outASP_strips_valid_areas.shp")


    print("\n\t ---------------")
    print("\n\t ")
    print("\n\t -- Finished processing " + outStereoPre.split('/')[-2])
    print("\n\t ")
    print("\n\t ---------------")

def run_asp(
    csv,
    outDir,     ##  ='/att/gpfsfs/userfs02/ppl/pmontesa/outASP',         #'/att/gpfsfs/userfs02/ppl/cneigh/nga_veg/outASP',
    inDir,
    nodesList,
    mapprj,
    mapprjRes,
    par,
    strip=True,
    searchExtList=['.ntf','.tif','.NTF','.TIF'],        ## All possible extentions for input imagery ['.NTF','.TIF','.ntf','.tif']
    csvSplit=False,
    doP2D=True,
    stereoDef='/att/gpfsfs/home/pmontesa/code/stereo.default',
    dirDEM='/att/nobackup/cneigh/nga_veg/in_DEM/aster_gdem',
    #mapprjDEM='/att/nobackup/cneigh/nga_veg/in_DEM/aster_gdem2_siberia_N60N76.tif',     ## for testing
    prj='EPSG:32647',                                                                   ## default is for Siberia
    test=False,                                                                         ## for testing
    rp=100):

    LogHeaderText = []

    # Strings to booleans
    mapprj = bool(strtobool(mapprj))
    par = bool(strtobool(par))

    # List of processing nodes for parallel_stereo
    # v10 --> now we have a set of nodes for each 'launch' node.
    # When this script is launched on a launch node, it will identify the corresponding (1) csv file to loop through, and (2) the nodes list to parallel process
    # This ensures that multiple parallel_stereo calls will not hit the same VM
    #if par:
    nodesList = '/att/gpfsfs/home/pmontesa/code/nodes_' + platform.node()          #'/att/gpfsfs/home/pmontesa/code/pmontesa_GPFSnodes_parStereo'
    #else:
    #    nodesList = ''

    LogHeaderText.append("Input csv file:")
    if csvSplit:
        # For use with pupsh cmd: Has the csv been split into smaller files?
        # Re-configure the csv input name according to node specification
        ##nid = "%02d" % (nid)
        csv = csv.split('.csv')[0] + '_' + platform.node() +'.csv'
        LogHeaderText.append("\tWorking on csv file: " + csv)
    else:
        # Now the norm. No csv splitting done. Use csv specified in argument.
        # This option used for looping one at a time through a main file, or smaller 'clean-up' type runs
        LogHeaderText.append(csv)

    """
    Here is the main function that wraps the AMES Stereo Pipeline  processing steps that:
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
    start = timer()

    # [1] Read csv of stereo shapefile footprints
    # This shapefile is provided by PGC or DG, and thus, the col names are specific to the attribute table of each
    # We have footprint code that we can run also. When want to run this script on a csv from a SHP kicked out from our footprint code,
    # we need to make sure we have coded for the same col names OR we need to change the col names specified in [2]
    csvStereo = open(csv, 'r')

    # Get the header
    header = csvStereo.readline().lower().rstrip().split(',')  #moved the split to this stage to prevent redudant processing - SSM


    # [2] From the header, get the indices of the attributes you need
    catID_1_idx     = header.index('catalogid')
    catID_2_idx     = header.index('stereopair')
    sensor_idx      = header.index('platform')
    avSunElev_idx   = header.index('avsunelev')
    avSunAzim_idx   = header.index('avsunazim')
    imageDate_idx   = header.index('acqdate')
    avOffNadir_idx  = header.index('avoffnadir')
    avTargetAz_idx  = header.index('avtargetaz')

    # Save all csv lines; close file
    csvLines = csvStereo.readlines()
    csvStereo.close()

    # Used for output CSV and VALPIX shapefile
    outHeader = "pairname, catID_1, catID_2, mapprj, year, month, avsunelev, avsunaz, avoffnad, avtaraz, avsataz, conv_ang, bie_ang, asym_ang, DSM\n"
    outHeaderList = outHeader.rstrip().split(',')

    # Set up an output summary CSV that matches input CSV
    with open(csv.split(".")[0] + "_output_smry.csv",'w') as csvOut:
        csvOut.write(outHeader)

        #------------------------------------------------------------------
        #       CSV Loop --> runs parallel_stereo for each line of CSV across all VMs
        #------------------------------------------------------------------
        # [3] Loop through the lines (the records in the csv table), get the attributes and run the ASP commands
        for line in csvLines:
            preLogText = []

            # Get attributes from the CSV
            linesplit = line.rstrip().split(',')
            preLogText.append("Current line from CSV file:")
            preLogText.append(line)
            preLogText.append(linesplit)

            catID_1    = linesplit[catID_1_idx]
            catID_2    = linesplit[catID_2_idx]
            sensor     = str(linesplit[sensor_idx])
            imageDate  = linesplit[imageDate_idx]
            avSunElev  = round(float(linesplit[avSunElev_idx]),0)
            avSunAz    = round(float(linesplit[avSunAzim_idx]),0)
            avOffNadir = round(float(linesplit[avOffNadir_idx]),0)
            avTargetAz = round(float(linesplit[avTargetAz_idx]),0)
            if avTargetAz <= 180:
                avSatAz = avTargetAz + 180
            else:
                avSatAz = avTargetAz - 180

            # Initialize DEM string
            mapprjDEM = ''

            # Get Image Date
            if imageDate != '':
                try:
                    imageDate = datetime.strptime(imageDate,"%m/%d/%Y")
                    preLogText.append( '\tTry 1: ' + str(imageDate))
                except Exception, e:
                    pass
                    try:
                        imageDate = datetime.strptime(imageDate,"%Y-%m-%d")
                        preLogText.append( '\tTry 2: ' + str(imageDate))
                    except Exception, e:
                        pass

            # [4] Search ADAPT's NGA database for catID_1 and catid_2

            # Establish the database connection
            with psycopg2.connect(database="NGAdb01", user="anon", host="ngadb01", port="5432") as dbConnect:

                cur = dbConnect.cursor() # setup the cursor
                catIDlist = [] # build now to indicate which catIDs were found, used later
                pIDlist = []
                found_catID = [False,False]
                """
                Search 1 catID at a time
                """
                # setup and execute the query on both catids of the stereopair indicated with the current line of the input CSV
                for num, catID in enumerate([catID_1,catID_2]):

                    selquery =  "SELECT s_filepath, sensor, acq_time, cent_lat, cent_long FROM nga_files_footprint_v2 WHERE catalog_id = '%s'" %(catID)
                    preLogText.append( "\n\t Now executing database query on catID '%s' ..."%catID)
                    cur.execute(selquery)
                    """
                    'selected' will be a list of all raw scene matching the catid and their associated attributes that you asked for above
                    """
                    selected=cur.fetchall()
                    preLogText.append( "\n\t Found '%s' scenes for catID '%s' "%(len(selected),catID))
                    # Get info from first item returned
                    #
                    #
                    if len(selected) == 0:
                        found_catID[num] = False
                    else:
                        found_catID[num] = True
                        # Get center coords for finding UTM zone and getting ASTER GDEM tiles
                        lat = float(selected[0][3])
                        lon = float(selected[0][4])
                        path_0 = os.path.split(selected[0][0])[0]
                        path_0 = path_0.replace('NGA_Incoming/','')
                        """
                        gets the path of the first scene in strip
                        """
                        preLogText.append("\n\tNGA dB path: %s" %path_0 )

                        # Get productcatalogid from this first dir: sometimes 2 are associated with a catid, and represent duplicate data from different generation times
                        pID = os.path.split(path_0)[1].split('_')[-2]   ## each file of a given catID also needs to have this string
                        preLogText.append("\tProduct ID: %s" %str(pID))
                        preLogText.append("\tCenter Lat: %s" %str(lat))
                        preLogText.append("\tCenter Lon: %s" %str(lon))

                        # If > 0 items returned from search, add catID to list and add product ID to list
                        """
                        This is a 2 element list holding the catid of the left and the right strip
                        """
                        catIDlist.append(catID)
                        pIDlist.append(pID)

                        # [4.1] Make imageDir
                        """
                        this dir holds the sym links to the NTF files that will form both strips for the stereo run
                        nobackup\mwooten\inASP\WV01_20130604_catid1_catid2\
                            sym link to raw scene in this dir
                        """
                        #   into which you'll direct the symbolic link inputs and store intermediate mosaics
                        #   Return date from first row (formatted for filename)
                        """
                        Getting needed info from just the first rec in the returned table called 'selected'
                        s_filepath, sensor, acq_time, cent_lat, cent_long
                        """
                        sensor = str(selected[0][1])                        # eg. WV02
                        date = str(selected[0][2]).replace("-","")          # eg. 20110604
                        year = date.strip()[:-4]
                        month = date.strip()[4:].strip()[:-2]
                        """
                        pairname is important: indicates that data on which the DSM was built..its unique..used for subdir names in outASP and inASP
                        """
                        pairname = sensor + "_" + date + "_" + catID_1 + "_" + catID_2
                        imageDir = os.path.join(inDir,pairname)
                        if not os.path.exists(imageDir):
                            os.mkdir(imageDir)

                       # Create symbolic links in imageDir of each item in the selected rows AND their corresponding xml
                        preLogText.append("\n\t Creating symbolic links to input in NGA database:")
                        for row in selected:
                            """
                            symlink gets me that path to the NTF in NCCS, and its used to give me the XML too
                            """
                            symbolicLink = os.path.join(imageDir,os.path.split(row[0])[1])

                            # If symlink exists, remove it to create a new one. This gets rid of broken links problem

                            if os.path.lexists(symbolicLink):
                                os.remove(symbolicLink)
                            os.symlink(row[0], symbolicLink )
                            preLogText.append("\t" + symbolicLink)

                            if os.path.lexists(symbolicLink.split(".")[0] + ".xml"):
                                os.remove(symbolicLink.split(".")[0] + ".xml")                            # image
                            os.symlink(row[0].split('.')[0] + ".xml", symbolicLink.split(".")[0] + ".xml" )    # xml
                """
                You might be able to dedent this if/else block to outside of WITH statement

                Now we have all the raw data in the inASP subdir identified with the pairname
                """

                if len(catIDlist) == 0:
                    try:
                        year = "%04d" % imageDate.year
                        month = "%02d" % imageDate.month
                        day = "%02d" % imageDate.day
                        date = year+month+day
                    except Exception,e:
                        date = 'XXXXXXXX'
                    pairname = sensor + "_" + date + "_" + catID_1 + "_" + catID_2
                    imageDir = os.path.join(inDir,pairname)
                else:
                    # [4.2] UTM zone
                    utm_zone, easting, northing = convert.LLtoUTM(23, lat, lon)
                    utm_zone = utm_zone[:-1]

                    if abs(int(utm_zone)) < 10:
                        utm_zone = "0" + str(utm_zone)

                    if lat < 0:
                        ns = "S"
                        prj = "EPSG:327" + utm_zone
                    else:
                        ns = "N"
                        prj = "EPSG:326" + utm_zone
                    if lon < 0:
                        ew = "W"
                    else:
                        ew = "E"

                    # [4.3] Get list for ASTER GDEM vrt
                    vrtDEMList = []
                    # Check if we have the v2 GDEM first
                    if lon > 0:
                        lonstr = "%03d" % (abs(int(lon)))
                    else:
                        lonstr = "%03d" % (abs(int(lon) - 1))
                    if lat > 0:
                        latstr = str(abs(int(lat)))
                    else:
                        latstr = str(abs(int(lat) - 1))
                    demTileTail = ns + latstr + ew + lonstr + "_dem.tif"
                    v2DEM = os.path.join(dirDEM,"v2","ASTGTM2_" + demTileTail)
                    v1DEM = os.path.join(dirDEM,"v1","ASTGTM_"  + demTileTail)

                    if os.path.exists(v2DEM):
                        preLogText.append( "\n\tASTER GDEM v2 exists")
                        gdem_v_dir = "v2"
                        gdem_v = "2"
                        vrtDEMList.append(v2DEM)
                    elif os.path.exists(v1DEM):
                        preLogText.append( "\tASTER GDEM v1 exists")
                        gdem_v_dir = "v1"
                        gdem_v = ""
                        vrtDEMList.append(v1DEM)
                    else:
                        preLogText.append( "\tNeither v2 nor v1 ASTER GDEM tiles exist for this stereopair:")
                        preLogText.append( "\tv2: %s" %v2DEM)
                        preLogText.append( "\tv1: %s" %v1DEM)
                        preLogText.append( "\tCannot do mapproject on input")
                        mapprj=False

                    if mapprj:
                        preLogText.append( "\tBuilding GDEM tile list...")
                        # Get list of DEMs from 8 surrounding tiles
                        # top 3 tiles
                        p1p1 = os.path.join(dirDEM,gdem_v_dir,"ASTGTM" + gdem_v + "_" + ns + str(abs(int(lat+1))) + ew + str(abs(int(lon+1))) + "_dem.tif")
                        if os.path.exists(p1p1):
                            vrtDEMList.append(p1p1)
                        p1p0 = os.path.join(dirDEM,gdem_v_dir,"ASTGTM" + gdem_v + "_" + ns + str(abs(int(lat+1))) + ew + str(abs(int(lon+0))) + "_dem.tif")
                        if os.path.exists(p1p0):
                            vrtDEMList.append(p1p0)
                        p1m1 = os.path.join(dirDEM,gdem_v_dir,"ASTGTM" + gdem_v + "_" + ns + str(abs(int(lat+1))) + ew + str(abs(int(lon-1))) + "_dem.tif")
                        if os.path.exists(p1m1):
                            vrtDEMList.append(p1m1)
                        # middle 2 tiles
                        p0p1 = os.path.join(dirDEM,gdem_v_dir,"ASTGTM" + gdem_v + "_" + ns + str(abs(int(lat+0))) + ew + str(abs(int(lon+1))) + "_dem.tif")
                        if os.path.exists(p0p1):
                            vrtDEMList.append(p0p1)
                        p0m1 = os.path.join(dirDEM,gdem_v_dir,"ASTGTM" + gdem_v + "_" + ns + str(abs(int(lat+0))) + ew + str(abs(int(lon-1))) + "_dem.tif")
                        if os.path.exists(p0m1):
                            vrtDEMList.append(p0m1)
                        # bottom 3 tiles
                        m1p1 = os.path.join(dirDEM,gdem_v_dir,"ASTGTM" + gdem_v + "_" + ns + str(abs(int(lat-1))) + ew + str(abs(int(lon+1))) + "_dem.tif")
                        if os.path.exists(m1p1):
                            vrtDEMList.append(m1p1)
                        m1p0 = os.path.join(dirDEM,gdem_v_dir,"ASTGTM" + gdem_v + "_" + ns + str(abs(int(lat-1))) + ew + str(abs(int(lon+0))) + "_dem.tif")
                        if os.path.exists(m1p0):
                            vrtDEMList.append(m1p0)
                        m1m1 = os.path.join(dirDEM,gdem_v_dir,"ASTGTM" + gdem_v + "_" + ns + str(abs(int(lat-1))) + ew + str(abs(int(lon-1))) + "_dem.tif")
                        if os.path.exists(m1m1):
                            vrtDEMList.append(m1m1)

                        # [4.4] Save list and build DEM vrt from list for mapproject
                        with open(os.path.join(imageDir,"vrtDEMTxt.txt"),'w') as vrtDEMTxt:
                            for item in vrtDEMList:
                                vrtDEMTxt.write("%s\n" %item)
                        preLogText.append( "\tBuilding GDEM vrt...")
                        """
                        gdalwarp?? maybe you dont need tiff..vrt might be fine
                        """
                        mapprjDEM = os.path.join(imageDir,"tmp_dem.tif")
                        cmdStr = "gdalbuildvrt -input_file_list " + os.path.join(imageDir,"vrtDEMTxt.txt") + " " + os.path.join(imageDir,"tmp_dem.vrt")
                        wf.run_wait_os(cmdStr, print_stdOut=False)
                        cmdStr = "gdal_translate -of GTiff " + os.path.join(imageDir,"tmp_dem.vrt") + " " + os.path.join(imageDir,"tmp_dem.tif")
                        wf.run_wait_os(cmdStr, print_stdOut=False)

            """
            Now outside of the WITH statement
            """
            # -----------------------
            # For logging on the fly
            lfile = os.path.join(outDir,'logs','run_asp_LOG_' + imageDir.split('/')[-1].rstrip('\n') +'_' + platform.node() + '_' + strftime("%Y%m%d_%H%M%S") + '.txt')
            so = se = open(lfile, 'w', 0)                       # open our log file
            sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0) # re-open stdout without buffering
            os.dup2(so.fileno(), sys.stdout.fileno())           # redirect stdout and stderr to the log file opened above
            os.dup2(se.fileno(), sys.stderr.fileno())
            print "--LOGFILE---------------"
            print(lfile)
            print "\n"
            print "--PYTHON FILE-----------"
            print os.path.basename(__file__)
            print "\n"
            print "--Header Text-----------"
            for row in LogHeaderText:
                print row
            print "\n"
            print "--DB Querying Text------"
            for row in preLogText:
                print row
            print "\n"
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
                conv_ang, bie_ang, asym_ang, hdr, attrbs = g.stereopairs(imageDir)
            except Exception, e:
                print "\n\tStereo angles not calc'd b/c there is no input for both catIDs"

            # [5.0] AMES Stereo Pipeline
            if len(catIDlist) < 2:
                print "\n\tMissing a catalog_id, can't do stereogrammetry."
                mapprj = False
                DSMdone = False
                outAttributes = pairname + "," + str(found_catID[0]) + "," + str(found_catID[1]) + "," + str(mapprj) + "," + str(year) + "," + str(month) + "," + str(avSunElev)+ "," + str(avSunAz) + "," + str(avOffNadir) + "," + str(avTargetAz) + "," + str(avSatAz) + "," +str(conv_ang) + "," + str(bie_ang) + "," + str(asym_ang) + "," + str(DSMdone) +"\n"
                """
                potential for simplfying flow around here with 'continue'
                just remember to store outAttributes
                """
            else:
                print("\tWorking on sym links to scenes in dir: " + imageDir)
                print "\n"
                # Go to the dir that holds all the indiv pairs associated with both stereo strips
                os.chdir(imageDir)

                # Set up data for stereo processing
                imagePairs, imagePair_xmls = ("" for i in range(2))                     # This will hold each image used in the parallel_stereo command

                # [5.1]Initialize Lists: For processing image pairs
                sceneNTFList, sceneXMLList, sceneTIFList, stripList, inSearchCatList = ([] for i in range(5))
                corExt = '_cor.tif'

                ##------------------------------------------------------------------
                ##              Process by strip
                ##------------------------------------------------------------------
                if strip:
                    imageExt = '.tif'
                    dgCmdList = []

                    # Establish stripList with each catalog ID of the pair
                    for catNum, catID in enumerate(catIDlist):
                        # Set search string
                        end = ""
                        raw_imageList = []
                        cor_imageList = []

                        # Get all raw images for wv_correct
                        for root, dirs, files in os.walk(imageDir):
                            for searchExt in searchExtList:
                                for each in files:
                                    if each.endswith(searchExt) and 'P1BS' in each and catID in each and pIDlist[catNum] in each:
                                        raw_imageList.append(each)
                        print("\n\tProduct ID for raw images: " + str(pIDlist[catNum]))
                        print("\n\tRaw image list: " + str(raw_imageList))

                        # Prep for dg_mosaic: This is the output strip prefix
                        outPref = sensor.upper() + "_" + imageDate.strftime("%y%b%d").upper() + "_" + catID ## e.g., WV01_JUN1612_102001001B6B7800
                        outStrip = outPref + '.r' + str(rp) + imageExt                                      ## e.g., WV01_JUN1612_102001001B6B7800.r100.tif
                        stripList.append(outStrip)
                        print("\n\tCatID: " + catID)

                        # If the mosaic already exists, dont do it again, dummy
                        if os.path.isfile(outStrip):
                            print("\n\t Mosaic strip already exists: " + outStrip)
                            dg_mos = False
                        else:
                            # Get seachExt for wv_correc and dg_mosaic
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
                                #

                                    try:
                                        print("\n\tRunnning wv_correct on raw image: " + raw_image)
                                        cmdStr ="wv_correct --threads=4 " + raw_image + " " + raw_image.replace(searchExt, ".xml") + " " + raw_image.replace(searchExt, corExt)
                                        Cmd = subp.Popen(cmdStr.rstrip('\n'), stdout=subp.PIPE, shell=True)
                                        wv_cor_cmd = True
                                        wvCmdList.append(Cmd)

                                        # Make copies of xmls to match *cor.tif
                                        shutil.copy(raw_image.replace(searchExt,".xml"), raw_image.replace(searchExt,corExt.replace('.tif',".xml")))

                                    except Exception, e:
                                        #print '\t' + str(e)
                                        print "\n\t Tried using this extension: " + searchExt

                            if not wv_cor_cmd:
                                print "\n\tRaw image search extension for dg_mosaic: %s: " %searchExt

                            # Communicate the wv_correct cmd
                            """
                            Probably should find a way to DEDENT the wv_correct AND dg_mosaic blocks so that ALL wv_corrects can be running simultaneously
                                and then BOTH mosaics can be running simultaneously.
                            """
                            if wv_cor_cmd:
                                print wvCmdList
                                for num, c in enumerate(wvCmdList):
                                    s,e = c.communicate()

                                    print "\n\twv_correct run # %s" %num
                                    print "\twv_correct output:"
                                    ##print "\tStandard out: %s" %str(s)
                                    print "\tStandard error: %s" %str(e)

                            # --------
                            # dg_mosaic:    This has to one once for each of the image strips.
                            ##               Don't communicate()..both dg_mosiac runs will be kicked off simultaneously
                            dg_mos = False
                            """
                            is this 'if' even necessary now?
                            """
                            if (not os.path.isfile(outStrip) ):

                                # Create a seach string with catID and extension
                                if 'WV01' in sensor or 'WV02' in sensor:
                                    inSearchCat = "*" + catID + "*" + corExt
                                else:
                                    inSearchCat = "*" + catID + "*" + "-P1BS" + "*" + pIDlist[catNum] + "*" + searchExt
                                    print "\n\tSensor is %s so wv_correct was not run" %(sensor)

                                try:
                                    print("\n\tRunnning (and waiting) dg_mosaic on catID: " + catID)
                                    cmdStr = "dg_mosaic " + inSearchCat + " --output-prefix "+ outPref + " --reduce-percent=" + str(rp)
                                    Cmd = subp.Popen(cmdStr.rstrip('\n'), stdout=subp.PIPE, shell=True)
                                    dg_mos = True
                                    dgCmdList.append(Cmd)
                                except Exception, e:
                                    print '\t' + str(e)

                    #  Communicate both dg_mosaic before proceeding
                    if dg_mos:
                        for Cmd in dgCmdList:
                            s,e = Cmd.communicate()
                            print "\n\tFinal dg_mosaic output:"
                            print "\tStandard out: %s" %str(s)
                            print "\tStandard error: %s" %str(e)

                    print "\n\t Now, delete *cor.tif files (space management)..."
                    print "\t imageDir: " + imageDir
                    corList = glob.glob("*cor.*")
                    print "\tList of cor files to delete: %s" %corList
                    for f in corList:
                        os.remove(f)
                        print "\tDeleted %s." %f

                    # Set up boolean to execute ASP routines after finding pairs
                    runPair=True

                    # Use stripList: copy XMLs to new name
                    #   Update stripList with the names of the cor versions of the strips
                    print("\tStripList: %s" %(stripList))
                    for n,i in enumerate(stripList):

                        outStrip = stripList[n]
                        """
                        put in replace instead of strip
                        continue
                        """
                        if os.path.isfile(outStrip.strip('.tif') + '.xml'):
                            print("\tFile exists: " + outStrip.strip('.tif') + '.xml')
                        else:

                            print("\n\t !!! -- Looks like mosaic does not exist: " + outStrip )
                            print("\n\t The pair is incomplete, so no stereo dataset will come from this dir.")
                            print("\n\t Skipping to next pair in CSV file.")
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
                        But this block below can be deleted...
                    """
                    # This list will hold the sorted NTFs or TIFs
                    sceneList = []

                    if strip:                             # Processing by strip
                        sceneList = sorted(stripList)
                    else:                                   # Just processing the orig NTF scenes
                        sceneList = sorted(sceneNTFList)

                    print("\timageExt: " + imageExt)
                    print("--------------")
                    print("\tLength of sceneList: %d" % len(sceneList))
                    print("--------------")

                    # Check to make sure sceneList has even number of scenes, if not remove smallest scene from list
                    if not len(sceneList) % 2 == 0:
                        szList = []

                        # Get list of sizes
                        for scene in sceneList:
                            szList.append(os.stat(scene).st_size)

                        min_sz = min(szList) ## min of list  -- removed list comprehension - SSM
                        del sceneList[szList.index(min_sz)] ## Delete the index of the list with the min file

                    increment = len(sceneList)/2
                    ##------------------------------------------------------------------
                    ##              Begin processing each pair from list
                    ##------------------------------------------------------------------
                    # Go through the list of scenes in the FIRST HALF of list
                    for sceneIdx in range(increment):

                        # Setup scene number for output (formatted)

                        if strip:
                            outType = 'strip'
                            sceneNum = ''
                        else:
                            outType = 'scene'
                            sceneNum = "%02d" % (sceneIdx+1)
                        print("\n\t" + outType)
                        print("\t" + sceneNum)

                        # Since stereo strips are collected in Forward and Reverse mode, they wont be listed in spatially corresponding order, but rather in collection order
                        # Need to look at scan direction
                        #   Assume both are same SCAN_DIR: setup right and left images
                        #       Corresponding scene is in corresponding position of SECOND HALF of list
                        leftScene   = sceneList[sceneIdx]
                        rightScene  = sceneList[sceneIdx + increment]

                        # Get each xml
                        stemLeft = sceneList[sceneIdx].rstrip(imageExt)
                        stemRight = sceneList[sceneIdx + increment].rstrip(imageExt)

                        # Make list of with each xml
                        xmlPairs = [stemLeft + ".xml",stemRight + ".xml"]
                        scDirPairs = []     ## list to hold the scan dir from each of the pairs
                        print('\tLeft XML: ' + stemLeft + ".xml" )
                        print('\tRight XML: ' + stemRight + ".xml" )

                        # Open each xml and get scan direction
                        for xml in xmlPairs:
                            with open(xml,'r') as curXML:  ## changing to with structure to allow automatic closure even if error - SSM
                                for line in curXML.readlines():
                                    if 'SCANDIRECTION' in line:
                                        scDirPairs.append(line.replace('<','>').split('>')[2])

                        # Now check for dif scan dirs
                        if scDirPairs[0] != scDirPairs[1]:
                            # If different, go to end of list and count back by sceneIdx to find image corresponding with left (current) scene
                            #    Corresponding scene is in inverted corresponding position in SECOND HALF of list
                            rightScene = sceneList[len(sceneList) - (sceneIdx+1)]

                        # Get image pairs and xmls to feed into stereo or parallel_stereo
                        """
                        End of block that is prob irrelevant now
                        """
                        imagePairs = leftScene + " " + rightScene + " "
                        imagePair_xmls = leftScene.replace(imageExt,'.xml') + " " + rightScene.replace(imageExt,'.xml') + " "
                        print("--------------")
                        print("--------------")
                        print("\tImage pairs for stereo run: " + imagePairs)
                        print("\tNodelist = " + nodesList)
                        print("--------------")

                        # [5.3] Prep for Stereo
                        #
                        ## Still in dir of data...
                        # Copy stereo.default file in home dir to current dir
                        shutil.copy(stereoDef,os.getcwd())

                        # Out ASP dir with out file prefix
                        """
                        outASPcur will look like this: outDir/WV01_20150610_102001003EBA8900_102001003E8CA400
                        """
                        outASPcur = outDir + "/" + imageDir.split('/')[-1].rstrip('\n')
                        """
                        outStereoPre looks like this: outDir/WV01_20150610_102001003EBA8900_102001003E8CA400/out
                        """
                        outStereoPre = outASPcur + "/out-" + outType + sceneNum
                        doStereo = False

                        # If outASPcur doesnt yet exist, run stereo
                        if not os.path.isdir(outASPcur):
                            doStereo = True
                        else:
                            if len(glob.glob(outASPcur+"/out-" + outType + sceneNum + "*")) == 0:
                                doStereo = True

                        # If PC file doesnt exist, run stereo
                        # Sometimes PC file may exist, but is incomplete.
                        if not os.path.isfile(outStereoPre + "-PC.tif"):
                            doStereo = True

                        print("--------------")
                        print("\n\n\t Do stereo? " + str(doStereo))
                        if doStereo:
                            print("\t\tparallel? " + str(par) )

                            ##mapprj = False
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
                                for num, unPrj in enumerate([os.path.join(imageDir,leftScene), os.path.join(imageDir, rightScene)]):

                                    print "\n\t Running mapproject on strip %s..." %(num + 1)
                                    mapPrj_img = unPrj.replace('.tif', '_mapprj.tif')
                                    if not os.path.exists(mapPrj_img):
                                        #cmdStr = "mapproject --nodata-value=-99 -t rpc --t_srs " + prj + " " + mapprjDEM + " " + unPrj + " " + unPrj.replace('tif','xml') + " " + mapPrj_img
                                        #cmdStr = "mapproject --nodata-value=-99 -t rpc --t_srs EPSG:4326 " + mapprjDEM + " " + unPrj + " " + unPrj.replace('tif','xml') + " " + mapPrj_img
                                        cmdStr = "mapproject --nodata-value=-99 -t rpc " + mapprjDEM + " " + unPrj + " " + unPrj.replace('tif','xml') + " " + mapPrj_img
                                        print "\n\t" + cmdStr
                                        Cmd = subp.Popen(cmdStr.rstrip('\n'), stdout=subp.PIPE, shell=True)

                                        # Hold Cmd in a list for later communication
                                        CmdList.append(Cmd)

                                    imagePairs += mapPrj_img + " "

                                """
                                no need to enumerate this: see wv_correct block
                                """
                                for num, c in enumerate(CmdList):
                                    # Now communicate both so they have been launched in parallel but will be reported in serial
                                    print "\n\tWaiting to communicate results of mapprojects..."
                                    stdOut, err = CmdList[num].communicate()
                                    print "\n\tCommunicating output from mapproject %s:"%str(num+1)
                                    print "\t" + str(stdOut) + str(err)
                                    print("\tEnd of mapproject %s"%str(num+1))

                            print("\n\t Beginning stereo processing...")
                            """
                            we'll probably gonna set par=False and no run parallel_stereo; stereo instead
                            """
                            run_stereo(par, nodesList, imagePairs, imagePair_xmls, outStereoPre, mapprjDEM, mapprj)
                            #if os.path.isfile(mapprjDEM):
                            #    os.remove(mapprjDEM)

                        if doP2D and os.path.isfile(outStereoPre + "-PC.tif"):
                            print("\n\t Running p2d function...")
                            runP2D(outStereoPre, prj, strip=True)

                            if os.path.isfile(outStereoPre + "-holes-fill-DEM.tif"):
                                DSMdone = True

                            outAttributes = pairname + "," + str(found_catID[0]) + "," + str(found_catID[1]) + "," + str(mapprj) + "," + str(year) + "," + str(month) + "," + str(avSunElev)+ "," + str(avSunAz) + "," + str(avOffNadir) + "," + str(avTargetAz) + "," + str(avSatAz) + "," +str(conv_ang) + "," + str(bie_ang) + "," + str(asym_ang) + "," + str(DSMdone) +"\n"
                            outAttributesList = outAttributes.rstrip().split(',')

                            if DSMdone:
                                print("\n\t Running VRT function...")
                                runVRT(outStereoPre,outDir, outHeaderList, outAttributesList)
                            else:
                                print("\n\t VRTs not done b/c DSM not done. Moving on...")
                        else:
                            print("\n\t No PC.tif file. Moving on...")

            outAttributes = pairname + "," + str(found_catID[0]) + "," + str(found_catID[1]) + "," + str(mapprj) + "," + str(year) + "," + str(month) + "," + str(avSunElev)+ "," + str(avSunAz) + "," + str(avOffNadir) + "," + str(avTargetAz) + "," + str(avSatAz) + "," +str(conv_ang) + "," + str(bie_ang) + "," + str(asym_ang) + "," + str(DSMdone) +"\n"

            # Write out CSV summary info
            csvOut.write(outAttributes)



if __name__ == "__main__":
    import sys
    run_asp( sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7]  )