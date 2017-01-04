#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      mwooten3
#
# Created:     10/11/2016
# Copyright:   (c) mwooten3 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import os, sys, math, osgeo, shutil, time, glob, platform, csv, subprocess as subp # edited for ADAPT (no gdalinfo- do we need it?)
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

def main(csv, inDir, batchID): #* batchID to keep track of groups of pairs for processing

##    def run_asp(
##    csv,
##    outDir,     ##  ='/att/gpfsfs/userfs02/ppl/pmontesa/outASP',         #'/att/gpfsfs/userfs02/ppl/cneigh/nga_veg/outASP',
##    inDir,
##    nodesList,
##    mapprj,
##    mapprjRes,
##    par,
##    strip=True,
##    searchExtList=['.ntf','.tif','.NTF','.TIF'],        ## All possible extentions for input imagery ['.NTF','.TIF','.ntf','.tif']
##    csvSplit=False,
##    doP2D=True,
##    stereoDef='/att/gpfsfs/home/pmontesa/code/stereo.default',
##    DEMdir='/att/nobackup/cneigh/nga_veg/in_DEM/aster_gdem',
##    #mapprjDEM='/att/nobackup/cneigh/nga_veg/in_DEM/aster_gdem2_siberia_N60N76.tif',     ## for testing
##    prj='EPSG:32647',                                                                   ## default is for Siberia
##    test=False,                                                                         ## for testing
##    rp=100):

    DEMdir = '/att/pubrepo/ASTERGDEM/'
    DISCdir = '/discover/nobackup/projects/boreal_nga' # DISCOVER path, for writing the job scripts
    batchDir = os.path.join(inDir, 'batch%s' % batchID)
    os.system('mkdir -p %s' % batchDir)

    LogHeaderText = []


    LogHeaderText.append("Input csv file: %s" % csv)
    LogHeaderText.append("BatchID: %s" % batchID)


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


    ##* everything up until now has stayed (pretty much) the same
    ##* here I am removing the rest of the runASP code outside of the "with open output summary csv as csvOut" and will simply store the out Attrbiutes in a table then write them to the outCsv at the end
    # Set up an output summary CSV that matches input CSV
    # csvOutFile = csv.split(".")[0] + "_output_smry.csv" ##* old way, below is the same thing but more readable
    csvOutFile = csv.replace('.csv', '_output_smry.csv')
    print ''

    #csvOutFile = [] # this will store the out attributes so we can write to summary csv
    with open(csvOutFile, 'w') as c: c.write(outHeader)


    # create submission script file which will contain all commands needed to submit the job to slurm
    submission_file = os.path.join(batchDir, 'submit_jobs_batch%s.sh' % batchID)
    # ?? what all do we need here to run all the jobs ??
    with open(submission_file, 'w') as ff:
        ff.write('Enter parameters needed here\nAnd here\nEtc.\n\n')


    #------------------------------------------------------------------
    #       CSV Loop --> runs parallel_stereo for each line of CSV across all VMs
    #------------------------------------------------------------------
    # [3] Loop through the lines (the records in the csv table), get the attributes and run the ASP commands
    n_lines = len(csvLines) # number of pairs we are attempting to process
    pair_count = 0 # to print which pair we are at
    n_pair_copy = 0 # number of succeffully copied pairs
    for line in csvLines[0:2]: # AKA for pair, or record in the input table # TEST just 2 lines for now
        pair_count += 1
        print "Attemping to copy data for pair %d of %d" % (pair_count, n_lines)

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

        # Get Image Date ##** can probably simplify this--- need to check with Paul to see which is the correct date format
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
##            catIDlist = [] # build now to indicate which catIDs were found, used later
##            pIDlist = []
            catIDlist = ['XXXXXXX', 'XXXXXXX']
            pIDlist = ['XXXXXXX', 'XXXXXXX']
            found_catID = [False,False]
            """
            Search 1 catID at a time
            """

            # setup and execute the query on both catids of the stereopair indicated with the current line of the input CSV
            selected_list = [[],[]] ##** to store the list of lists (selected_list[0] will give list of scenes for catID 1, select_list[1] will give for catID2
##            pathlist = ['XXX', 'XXX']
##            latlist = [-999, -999]
##            lonlist = [-999, -999]
            for num, catID in enumerate([catID_1,catID_2]): #* loop thru catID of the pairs

                selquery =  "SELECT s_filepath, sensor, acq_time, cent_lat, cent_long FROM nga_files_footprint WHERE catalog_id = '%s'" %(catID)
                preLogText.append( "\n\t Now executing database query on catID '%s' ..." % catID)
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
                    continue ##** if we don't have data for catID X, set it to false and move to the next catID

                ##** removed else here because continue should take care of the flow

                ##** moved this following block from down below
                # If > 0 items returned from search, add catID to list, add product ID to list, and add the resulting scenes to the list
                """
                This is a 2 element list holding the catid of the left and the right strip
                """
##                latlist[num] = float(selected[0][3])
##                lonlist[num] = float(selected[0][4])
##                pathlist[num] = os.path.split(selected[0][0])[0] # gets the path of the first scene in first strip

                catIDlist[num] = catID
                pID = os.path.split(os.path.split(selected[0][0])[0])[1].split('_')[-2] # get pID from first entry in selected
                pIDlist[num] = pID
                found_catID[num] = True
                selected_list[num] = selected

                print ''
               # for select in selected: selected_list.append(select) ##** add the list of scenes to the list of lists (index0 for catID 1, index1 fir catID2)
        print pIDlist
        conv_ang, bie_ang, asym_ang = ("" for i in range(3)) ##* set these to empty strings for later

        if len(catIDlist) == 0: ##** if neither of the catIDs returned data
            try:
                year = "%04d" % imageDate.year
                month = "%02d" % imageDate.month
                day = "%02d" % imageDate.day
                date = year+month+day
            except Exception,e: ##** if we can't get the info
                year = 'XXXX'
                month = 'XX'
                day = 'XX'
                date = year+month+day # so date will be 'XXXXXXXX'

            pairname = sensor + "_" + date + "_" + catID_1 + "_" + catID_2
            imageDir = os.path.join(batchDir,pairname)

            mapprj = False
            DSMdone = False

            outAttributes = pairname + "," + str(found_catID[0]) + "," + str(found_catID[1]) + "," + str(mapprj) + "," + str(year) + "," + str(month) + "," + str(avSunElev)+ "," + str(avSunAz) + "," + str(avOffNadir) + "," + str(avTargetAz) + "," + str(avSatAz) + "," +str(conv_ang) + "," + str(bie_ang) + "," + str(asym_ang) + "," + str(DSMdone) +"\n"
            with open(csvOutFile, 'a') as c:
                c.write(outAttributes) ##* append the attributes (mostly blank at this point) to the csv file list

            ##Q Print statement here??? or do we just need to print one statement if one or both catID data is not present
            preLogText.append("\n\t There is no data for either catID in our archive **review this print statement/placement\n\n")

            continue ##* and move on to the next pair in the list

        ##** now get info from first scene in selected list, regardless of whether or not we have one or two catIDs. if we get to this point we know we have items in selected list

        #* at this point we know that we have data for at least one catID
        """
        Getting needed info from just the first rec in the returned table called 'selected' ##** now it's a list called selected_list
        s_filepath, sensor, acq_time, cent_lat, cent_long
        """
        # Get center coords for finding UTM zone and getting ASTER GDEM tiles
        ##* selected_list contains all scene entries that make up both pairs (path plus sensor, etc.)

##        print selected_list
##        print selected_list[0]
##        print selected_list[0][0]
##        print os.path.split(selected_list[0][0])
##        print path_0


        ##** get info that we need from selected to make the imageDir, but don't actually make the imageDir unless data for both catIDs were found
        sensor = str(selected[0][1])                        # eg. WV02
        date = str(selected[0][2]).replace("-","")          # eg. 20110604
        year = date.strip()[:-4]
        month = date.strip()[4:].strip()[:-2]
        print "!!!"
        print sensor
        print date
        print year

        """
        pairname is important: indicates that data on which the DSM was built..its unique..used for subdir names in outASP and inASP
        """
        pairname = sensor + "_" + date + "_" + catID_1 + "_" + catID_2
        imageDir = os.path.join(batchDir,pairname)
##        print "3:" #del
##        print pairname
##        print imageDir
##        print ''



        if len(catIDlist) < 2: ##** if there was one but not two catIDs of data for the pair, we want to get the info for the outCsv and move on to the next pair
            #print "\n\tMissing a catalog_id, can't do stereogrammetry. **review this print statement/placement with the one below in mind\n\n"
            preLogText.append("\n\tMissing a catalog_id, can't do stereogrammetry. **review this print statement/placement with the one below in mind\n\n")
            mapprj = False
            DSMdone = False
            outAttributes = pairname + "," + str(found_catID[0]) + "," + str(found_catID[1]) + "," + str(mapprj) + "," + str(year) + "," + str(month) + "," + str(avSunElev)+ "," + str(avSunAz) + "," + str(avOffNadir) + "," + str(avTargetAz) + "," + str(avSatAz) + "," +str(conv_ang) + "," + str(bie_ang) + "," + str(asym_ang) + "," + str(DSMdone) +"\n"
            with open(csvOutFile, 'a') as c:
                c.write(outAttributes)
            ##Q print statement here?
            preLogText.append("\n\t One of the catIDs does not have data in our archive **review this print statement/placement\n\n")
            continue ##* move on to the next pair






        #** we will only get to this point if there is data for both catIDs- ##Q is that OK?
        # now that we have data for both, loop thru the strips again
        for num, catID in enumerate([catID_1,catID_2]):
            ##Q do we need to be in a pair loop (i.e. looping through the two catIDs?)

            ##Q does the info below (NGA path/pID/lat/lon) need to be printed to log if pairs arent going to be processed? If so, this block below needs to be moved up before if len(catIDlist) < 2

            # retrieve list of scenes for catID
            selected = selected_list[num]

            # get lat long and path from first
            lat= float(selected[0][3])
            lon = float(selected[0][4])
            path_0 = os.path.split(selected[0][0])[0]


            preLogText.append("\n\tNGA dB path: %s" %path_0 )
            # Get productcatalogid from this first dir: sometimes 2 are associated with a catid, and represent duplicate data from different generation times
            pID = pIDlist[num]
            ##Q but this is onle the pID of the first/left pair...what about the others?
##            print "PID!:"
##            print pID
##            print path_0
##            print ''




            preLogText.append("\tProduct ID: %s" %str(pID))
            preLogText.append("\tCenter Lat: %s" %str(lat))
            preLogText.append("\tCenter Lon: %s" %str(lon))




            # [4.1] Make imageDir ##** only want to do this if both pairs exist

            """
            this dir holds the sym links to the NTF files that will form both strips for the stereo run
            nobackup\mwooten\inASP\WV01_20130604_catid1_catid2\
                sym link to raw scene in this dir
            """
            #   into which you'll direct the symbolic link inputs and store intermediate mosaics
            #   Return date from first row (formatted for filename)
            """
            already got the necessary info above
            """
            # COPY data from archive to ADAPT
            os.system('mkdir -p %s' % imageDir)
            preLogText.append("\n\tMoving data from NGA database to %s" % imageDir)

            for row in selected: ##** now we are looping through the list of selected scenes for catID X
                ntf = row[0]
                xml = ntf.replace('.ntf', '.xml')

                # ** FOR NOW: copy files if it exists. assumming if it doesnt exist the path changed to NGA, copy that instead
                if not os.path.isfile(os.path.join(imageDir, os.path.basename(ntf))): # if the file is not in the imageDir
                    if os.path.isfile(ntf):
                        os.system('cp %s %s' % (ntf, imageDir))
                    else:
                        ntf = ntf.replace('NGA_Incoming/NGA', 'NGA')
                        print "Using %s" % ntf
                        os.system('cp %s %s' % (ntf, imageDir))
                if not os.path.isfile(os.path.join(imageDir, os.path.basename(xml))):
                    if os.path.isfile(xml):
                        os.system('cp %s %s' % (xml, imageDir))
                    else:
                        print "Using %s" % xml
                        xml = xml.replace('NGA_Incoming/NGA', 'NGA')
                        os.system('cp %s %s' % (xml, imageDir))
            """
              Now we have all the raw data in the inASP subdir identified with the pairname
            """

        # try new method:
         # now we are out of the catID loop and back at the pair loop- only wanna do DEM stuff once per pair
        # [4.2] UTM zone
        ##Q this will only get run if both catIDs have data in our archive---is that OK? or do we want this to be done if one catID exists? if so, move to before if len(catIDlist) < 2
        ##Q move into function??
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
        DEMlist = []
        DEM_inputs = ''
        # Check if we have the v2 GDEM first
        lonstr = "%03d" % (abs(int(lon)))
        demTileTail = ns + str(abs(int(lat))) + ew + lonstr + "_dem.tif"
        v2DEM = os.path.join(DEMdir,"v2","ASTGTM2_" + demTileTail)
        v1DEM = os.path.join(DEMdir,"v1","ASTGTM_"  + demTileTail)

        if os.path.exists(v2DEM):
            preLogText.append( "\n\tASTER GDEM v2 exists")
            gdem_v_dir = "v2"
            gdem_v = "2"
            DEM_inputs += v2DEM + ' '
            DEMlist.append(v2DEM)

        elif os.path.exists(v1DEM):
            preLogText.append( "\tASTER GDEM v1 exists")
            gdem_v_dir = "v1"
            gdem_v = ""
            DEMlist.append(v1DEM)
            DEM_inputs += v1DEM + ' '

        else:
            preLogText.append( "\tNeigther v2 or v1 ASTER GDEM tiles exist for this stereopair:")
            preLogText.append( "\tv2: %s" %v2DEM)
            preLogText.append( "\tv1: %s" %v2DEM)
            preLogText.append( "\tCannot do mapproject on input")
            mapprj=False
            #? What to do if this else is true...gdem_v does not get set. Should the below only happen if mapprj is True? editing to assume yes. Also set mapprj=True in the first 2 cases. that OK?

        if mapprj: #? Build the GDEM tile list and create the DEM if we are doing mapprj
            preLogText.append( "\tBuilding GDEM tile list...")
            # Get list of DEMs from 8 surrounding tiles
            # top 3 tiles

            p1p1 = os.path.join(DEMdir,gdem_v_dir,"ASTGTM" + gdem_v + "_" + ns + str(abs(int(lat+1))) + ew + str(abs(int(lon+1))) + "_dem.tif")
            if os.path.exists(p1p1):
                DEMlist.append(p1p1)
                DEM_inputs += p1p1 + ' '
            p1p0 = os.path.join(DEMdir,gdem_v_dir,"ASTGTM" + gdem_v + "_" + ns + str(abs(int(lat+1))) + ew + str(abs(int(lon+0))) + "_dem.tif")
            if os.path.exists(p1p0):
                DEMlist.append(p1p0)
                DEM_inputs += p1p0 + ' '
            p1m1 = os.path.join(DEMdir,gdem_v_dir,"ASTGTM" + gdem_v + "_" + ns + str(abs(int(lat+1))) + ew + str(abs(int(lon-1))) + "_dem.tif")
            if os.path.exists(p1m1):
                DEMlist.append(p1m1)
                DEM_inputs += p1m1 + ' '
            # middle 2 tiles
            p0p1 = os.path.join(DEMdir,gdem_v_dir,"ASTGTM" + gdem_v + "_" + ns + str(abs(int(lat+0))) + ew + str(abs(int(lon+1))) + "_dem.tif")
            if os.path.exists(p0p1):
                DEMlist.append(p0p1)
                DEM_inputs += p0p1 + ' '
            p0m1 = os.path.join(DEMdir,gdem_v_dir,"ASTGTM" + gdem_v + "_" + ns + str(abs(int(lat+0))) + ew + str(abs(int(lon-1))) + "_dem.tif")
            if os.path.exists(p0m1):
                DEMlist.append(p0m1)
                DEM_inputs += p0m1 + ' '
            # bottom 3 tiles
            m1p1 = os.path.join(DEMdir,gdem_v_dir,"ASTGTM" + gdem_v + "_" + ns + str(abs(int(lat-1))) + ew + str(abs(int(lon+1))) + "_dem.tif")
            if os.path.exists(m1p1):
                DEMlist.append(m1p1)
                DEM_inputs += m1p1 + ' '
            m1p0 = os.path.join(DEMdir,gdem_v_dir,"ASTGTM" + gdem_v + "_" + ns + str(abs(int(lat-1))) + ew + str(abs(int(lon+0))) + "_dem.tif")
            if os.path.exists(m1p0):
                DEMlist.append(m1p0)
                DEM_inputs += m1p0 + ' '
            m1m1 = os.path.join(DEMdir,gdem_v_dir,"ASTGTM" + gdem_v + "_" + ns + str(abs(int(lat-1))) + ew + str(abs(int(lon-1))) + "_dem.tif")
            if os.path.exists(m1m1):
                DEMlist.append(m1m1)
                DEM_inputs += m1m1 + ' '

            # [4.4] Save list and build DEM vrt from list for mapproject
            with open(os.path.join(imageDir,"vrtDEMTxt.txt"),'w') as vrtDEMTxt:
                for item in DEMlist:
                    vrtDEMTxt.write("%s\n" %item)
            preLogText.append( "\tBuilding GDEM geoTIFF...") # *keep using vrt method so we can have list of DEMs used for pairDEM

            pair_DEM = os.path.join(imageDir,"dem-%s.tif" % pairname)
            cmdStr = "gdalwarp -t_srs EPSG:4326 -ot Int16  %s %s" % (DEM_inputs.strip(' '), pair_DEM)

            if not os.path.isfile(pair_DEM): wf.run_wait_os(cmdStr, print_stdOut=False)


        n_pair_copy += 1 # if we get to this point we have successfully copied data for the pair


        # Create the individual job script:
        job_script = os.path.join(imageDir, 'slurm_%s.j' % pairname)

        # CHANGE THESE:
        job_name = '%s_job' % pairname
        time_limit = '12:00:00'
        num_nodes = '1'
        python_script_args = '%s arg1 arg2 arg3 etc' % (os.path.join(DISCdir, 'code', '<scriptName.py>'))

        discover_imageDir = os.path.join(DISCdir, 'inASP/%s' % pairname)


        with open(job_script, 'w') as f:
            f.write('#!/bin/csh -f\n#SBATCH --job-name=%s\n#SBATCH --time=%s\n#SBATCH --nodes=%s\n\n.%s' % (job_name, time_limit, num_nodes, python_script_args))

        with open(submission_file, 'a') as ff:
            ff.write('\ncd %s\nsbatch %s' % (discover_imageDir, os.path.basename(job_script)))

        #print preLogText
        print ''

    print "\nSuccessfully copied data for %d of %d pairs" % (n_pair_copy, n_lines)

    # NOW TAR everything inASP
    archive = os.path.join(inDir, 'batch%s-archive' % batchID)
    print archive
    tarComm = 'tar -zcvf %s %s' % (archive, inASP)
    print tarComm



if __name__ == '__main__':
    import sys
    main(sys.argv[1], sys.argv[2], sys.argv[3])
