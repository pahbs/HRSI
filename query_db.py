#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      mwooten3
#
# Created:     10/11/2016
# Copyright:   (c) mwooten3 2016
# Licence:     <your licence>


# 1/24: Instead of passing along preLogText list, write preLogText to text file (saved to inASP dir) and pass filename as arg; then read file into list on DISCOVER
# 1/24: Previously made changes are commented throughout the code (search #*, ##*, #Q, ##Q)




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


def find_elapsed_time(start, end):
    elapsed_min = (end-start)/60
    return float(elapsed_min)


#def main(csv, inDir, batchID, mapprj=True, doP2D=True, rp=100): #* batchID to keep track of groups of pairs for processing # old way- without argparse
def main(csv, inDir, batchID, mapprj, noP2D, rp, debug): #the 4 latter args are optional

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



    start_main = timer() # start timer object for entire batch

    # set variables using CL args
    doP2D = not noP2D # doP2D is the opposite of noP2D
    DEMdir = '/att/pubrepo/ASTERGDEM/'
    DISCdir = '/discover/nobackup/projects/boreal_nga' # DISCOVER path, for writing the job scripts
    batchDir = os.path.join(inDir, 'batch%s' % batchID)
    os.system('mkdir -p %s' % batchDir)

    ##LogHeaderText = []
    workflowCodeName = 'workflow_HRSI_vDISC.py'

##    #DEl
##    print "After getting inputs from CL:"
##    print "inDir:", inDir
##    print "DEMdir:", DEMdir
##    print "DISCdir:", DISCdir
##    print "batchDir:", batchDir
##    print ''

## # 1/30: moved this to preLog write-up
##    LogHeaderText.append("Input csv file: %s" % csv)
##    LogHeaderText.append("BatchID: %s" % batchID)


    # [1] Read csv of stereo shapefile footprints
    # This shapefile is provided by PGC or DG, and thus, the col names are specific to the attribute table of each
    # We have footprint code that we can run also. When want to run this script on a csv from a SHP kicked out from our footprint code,
    # we need to make sure we have coded for the same col names OR we need to change the col names specified in [2]
    csvStereo = open(csv, 'r')

    # Get the header
    hdr = csvStereo.readline().lower() # this is what will get written to the new query csv
    header = hdr.rstrip().replace('shape *', 'shape').split(',')  #moved the split to this stage to prevent redudant processing - SSM

    # 2/13 if SHAPE* is in the header, replace with shape to header can be passed


    # [2] From the header, get the indices of the attributes you need
    # 2/13: there are two possible input csv types - one with a stereopair column and one with a pairname column. if stereopair column exists catID_2_idx will exist, if not, it will be false

    pairname_idx = -999 # this will be something other than -999 if the try statement below does not fail (ie if there is pairname field)
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



##    catID_1_idx     = header.index('catalogid') # this column should always exist
##    catID_2_idx = False # set this to false for now unless this column exists. only way it will be set to True is if pairname fieldn does not exist
##    try:
##        catID_2_idx     = header.index('stereopair') # this may not exist, so just try
##    except ValueError: # this will happen if it doesn't exist
##        pairname_idx = header.index('pairname') # in which case we have a pairname idx and catID_2_idx is False
##
##    sensor_idx      = header.index('platform')
##    avSunElev_idx   = header.index('avsunelev')
##    avSunAzim_idx   = header.index('avsunazim')
##    imageDate_idx   = header.index('acqdate')
##    avOffNadir_idx  = header.index('avoffnadir')
##    avTargetAz_idx  = header.index('avtargetaz')

    # Save all the rest of the csv lines; close file
    csvLines = csvStereo.readlines()
    csvStereo.close()
    n_lines = len(csvLines) # number of pairs we are attempting to process

    # go ahead and get the name for the reQuery csv file. This csv will be a subset of the incsv, but including only those lines that had data missing and could not be processed # 4/5/2017
    oldQvers = int(os.path.basename(csv).split('_')[-1].split('.')[0][1]) # this will grab the ? from the *_q_?.csv to figure out which query version we are on (0 is initial)
    newQvers = oldQvers + 1 # we will only need this if there are pairs with no data
    newQcsv = csv.replace('q{}.csv'.format(oldQvers), 'q{}.csv'.format(newQvers))

    # log ADAPT output for bash
    logdir = os.path.join(os.path.dirname(inDir.rstrip('/')), 'queryLogs')
    os.system('mkdir -p %s' % logdir)
    lfile = os.path.join(logdir, 'batch%s_ADAPT_query_log.txt' % batchID)
    print "Attempting to process {} pairs for batch {}. See log file for output:\n{}".format(n_lines, batchID, lfile)
    so = se = open(lfile, 'a', 0)                       # open our log file
    sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0) # re-open stdout without buffering
    os.dup2(so.fileno(), sys.stdout.fileno())           # redirect stdout and stderr to the log file opened above
    os.dup2(se.fileno(), sys.stderr.fileno())

    if debug: print "!!!!! DEBUG mode !!!!!\n\n"
    print "BATCH: %s" % batchID
    print "Attempting to process %d pairs\n" % n_lines
    print "Begin:", datetime.now().strftime("%m%d%y-%I%M%p"), "\n"

    # Used for output failed pairs
    outHeader = "batchID, pairname, catID_1_exists, catID_2_exists, mapprj, year, month, avsunelev, avsunaz, avoffnad, avtaraz, avsataz, conv_ang, bie_ang, asym_ang\n"
    outHeaderList = outHeader.rstrip().split(',')

    ##* everything up until now has stayed (pretty much) the same
    ##* here I am removing the rest of the runASP code outside of the "with open output summary csv as csvOut" and will simply store the out Attrbiutes in a table then write them to the outCsv at the end
    # Set up an output summary CSV that matches input CSV
    # csvOutFile = csv.split(".")[0] + "_output_smry.csv" ##* old way, below is the same thing but more readable
    # set up batch level failure csv. this is where outAtributes will go unless the pair succeeded
##    summary_csv = os.path.join(os.path.dirname(inDir.rstrip('/')), 'batch_failure_csvs', 'batch%s_failed_pairs.csv' % batchID) # old batch failure script
    summary_csv = os.path.join(batchDir, 'batch%s_output_summary.csv' % batchID)
    # if summary csv does not exist, create it and write header:
    if not os.path.isfile(summary_csv):
        with open(summary_csv, 'w') as sc:
            sc.write("batchID, pairname, catID_1, catID_1_found, catID_2, catID_2_found, mapprj, year, month, queryResult\n")
##  #csvOutFile = [] # this will store the out attributes so we can write to summary csv
##    with open(summary_csv, 'a') as c: c.write(outHeader)

    # also set up text file that will contain list of catIDs that are missing data
    missing_catID_file = os.path.join(os.path.dirname(inDir.rstrip('/')), 'missing_catID_lists', 'batch%s_missing_catIDs.txt' % batchID)
    n_missing_catIDs = 0 # count starts at 0


    # create submission script file which will contain all commands needed to submit the job to slurm
    submission_file = os.path.join(batchDir, 'submit_jobs_batch%s.sh' % batchID)
    # ?? what all do we need here to run all the jobs ??
    with open(submission_file, 'w') as ff:
        ff.write('#!/bin/bash\n\n')



    #------------------------------------------------------------------
    #       CSV Loop --> runs parallel_stereo for each line of CSV across all VMs
    #------------------------------------------------------------------
    # [3] Loop through the lines (the records in the csv table), get the attributes and run the ASP commands
    pair_count = 0 # to print which pair we are at
    n_pair_copy = 0 # number of succeffully copied pairs
    unique_pairnames = [] # to store the pairnames you copy for the batch
    for line in csvLines: # AKA for pair, or record in the input table # TEST just 2 lines for now

        start_pair = timer()

        pair_count += 1
        print "\nAttemping to query and copy data for pair %d of %d" % (pair_count, n_lines) # print to ADAPT screen
        #print line

        preLogText = [] # start over with new preLog everytime you go to another pair


        # Get attributes from the CSV
        linesplit = line.rstrip().split(',')
        #print linesplit
        # 1/30: Edited pre-Log text to account for getting rid of LogHEader text in workflow
        preLogText.append("--DB Querying Text (ADAPT)------\nInput csv file:\n%s\n\nLine from CSV file:\n%s\nBatch ID: %s\n\n" %(os.path.abspath(csv), line, batchID))
        #preLogText.append(line)
        #preLogText.append(linesplit)


        if pairname_idx != -999: # this statement will be True if there is a pairname index
            pairname   = linesplit[pairname_idx]
            catID_1    = linesplit[pairname_idx].split('_')[2]
            catID_2    = linesplit[pairname_idx].split('_')[3]
            sensor     = linesplit[pairname_idx].split('_')[0]
            imageDate  = linesplit[pairname_idx].split('_')[1]

        else:
            catID_1 = linesplit[catID_1_idx]
            catID_2 = linesplit[catID_2_idx]
            sensor = str(linesplit[sensor_idx])
            imageDate  = linesplit[imageDate_idx]


        #* ! at this point, imageDate is not in a consistent format, it's whatever format it was in on the csv
##                #DEL :
##        print "Variables from CSV:"
##        print "catID_1:", catID_1
##        print "catID_2:", catID_2
##        print "sensor:", sensor
##        print "imageDate:", imageDate
##        print type(imageDate)
##        print ''





##
##
##        catID_1 = linesplit[catID_1_idx]
##        # get catID_2 either from catID_2 field or pairname field
##        if catID_2_idx: # if this is True (i.e. it's something other than False), then catalog ID field exists
##            catID_2    = linesplit[catID_2_idx]
##        else: # if catID_2_idx is false (ie stereopair field does not exist), pairname_idx should exist
##            csv_pairname = linesplit[pairname_idx] # pairname from the csv
##            catID_2 = csv_pairname.split('_')[3] # fourth piece of i.e. WV01_20130604_102001002138EC00_1020010021AA3000
##            if catID_2 == catID_1: # make sure this is not the same as the original catID
##                catID_2 = csv_pairname.split('_')[2] # if it is, then get the other piece of the pairname
##
##        sensor     = str(linesplit[sensor_idx])
##        imageDate  = linesplit[imageDate_idx]



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
                #preLogText.append( '\tTry 1: ' + str(imageDate))
                preLogText.append( '\tDate format 1: ' + str(imageDate))
            except Exception, e:
                pass
                try:
                    imageDate = datetime.strptime(imageDate,"%Y-%m-%d")
                    #preLogText.append( '\tTry 2: ' + str(imageDate))
                    preLogText.append( '\tDate format 2: ' + str(imageDate))
                except Exception, e:
                    pass
                    try:
                        imageDate = datetime.strptime(imageDate,"%Y%m%d")
                        preLogText.append( '\tDate format 3: ' + str(imageDate))
                    except Exception, e:
                        pass
        #* at this point, imageDate is not a datetime object

##        # DEL
##        print "if first imageDate was '':"
##        print "imageDate:", imageDate
##        print ''

        # [4] Search ADAPT's NGA database for catID_1 and catid_2
        # Establish the database connection
        with psycopg2.connect(database="ngadb01", user="anon", host="ngadb01", port="5432") as dbConnect:

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

               # selquery =  "SELECT s_filepath, sensor, acq_time, cent_lat, cent_long FROM nga_files_footprint WHERE catalog_id = '%s'" %(catID)
                selquery =  "SELECT s_filepath, sensor, acq_time, cent_lat, cent_long FROM nga_inventory WHERE catalog_id = '%s' AND prod_code = 'P1BS'" %(catID) # 2/13 change nga_inventory_footprint to nga_inventory # 4/13 add AND prod_code so we only get Pan data
                preLogText.append( "\n\tNow executing database query on catID '%s' ..." % catID)
                print "  Executing database query on catID '%s' ..." % catID
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
                    print "    No data found for catID %s. Writing to missing catID text file" % catID
                    #missing_catIDs.append(catID)
                    # we can just assume we will never run batch more than once when we get shit figured out
##                    write_method = 'a' # assume we are appending the file
##                    if n_missing_catIDs == 0: write_method = 'w' # unless we havent yet found any missing catIDs yet this time around running the batch, in which case we wanna overwrite output file
                    with open(missing_catID_file, 'a') as mf:
                        mf.write(catID +'\n')
                    n_missing_catIDs += 1 # add one to number of missing catIDs for batch
                    continue ##** if we don't have data for catID X, set it to false and move to the next catID

                # only want to print number of scenes found if there were scenes found
                print "   -Found {} scenes".format(len(selected))
                if debug: print "    Selected list: {}".format(selected)
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
                selected_list[num] = selected # selected list is a list of len 2, where the first index contains the matching files from the first catID, and second index contains from second catID
##                print selected # selected will be emtpy if there was no data for catID, so selected_list[index of catID that was True] will give you a selected list


##        # DEL
##        print "After the db query:"
##        print "catIDlist:", catIDlist
##        print "PIDlist:", pIDlist
##        print "found_catID:", found_catID
##        print "selected_list:", selected_list
##        print ''


        conv_ang, bie_ang, asym_ang = ("" for i in range(3)) ##* set these to empty strings for later

        #if len(catIDlist) == 0: ##** if neither of the catIDs returned data
        #* 2/24 the above won't work because catIDlist will at least be [XXXXXX, XXXXXX]
        if found_catID.count(False) == 2: # if both values of found_catID are False, no data was found

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

           # pairname = sensor + "_" + date + "_" + catID_1 + "_" + catID_2
            pairname = "{}_{}_{}_{}".format(sensor, date, catID_1, catID_2)
            imageDir = os.path.join(batchDir, pairname)

            mapprj = False
            DSMdone = False

            #outAttributes = batchID + "," + pairname + "," + str(found_catID[0]) + "," + str(found_catID[1]) + "," + str(mapprj) + "," + str(year) + "," + str(month) + "," + str(avSunElev)+ "," + str(avSunAz) + "," + str(avOffNadir) + "," + str(avTargetAz) + "," + str(avSatAz) + "," +str(conv_ang) + "," + str(bie_ang) + "," + str(asym_ang) +"\n"
            outAttributes = '{},{},{},{},{},{},{},{},{},missingData\n'.format(batchID, pairname, catID_1, found_catID[0], catID_2, found_catID[1], mapprj, year, month)
            with open(summary_csv, 'a') as c:
                c.write(outAttributes) ##* append the attributes (mostly blank at this point) to the csv file list

            # now write to input line of the missingData pair to the new query csv. but first, if it doesnt exist. write the header
            if not os.path.exists(newQcsv):
                with open(newQcsv, 'w') as nq:
                    nq.write(hdr)

            with open(newQcsv, 'a') as nq:
                nq.write(line)

            ##Q Print statement here??? or do we just need to print one statement if one or both catID data is not present
            preLogText.append("\n\t There is no data for either catID in our archive for pair {}\n\n".format(pairname))
            print "Neither catID returned data from our query. Moving to next pair\n"

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
##        #* 1/17 already have this info, right>?
##        print "sensor:", sensor
##        print ""
        #sensor = str(selected[0][1])                        # eg. WV02
##        print imageDate
##        print date
##        print selected_list
##        print selected[0]
##        print selected[0][2]
##        print str(selected[0][2]).replace("-","")


        # get a selected list (like from the query loop) that is definitely not empty
        selected = selected_list[found_catID.index(True)] # this will give the selected list that has data (works for scenarios where one catID has data or both)
        date = str(selected[0][2]).replace("-","")          # eg. 20110604
        year = date.strip()[:-4]                            # e.g. 2011
        month = date.strip()[4:].strip()[:-2]               # e.g. 06
##        print "!!!"
##        print sensor
##        print date
##        print year
##        print month
##        print ""

        """
        pairname is important: indicates that data on which the DSM was built..its unique..used for subdir names in outASP and inASP
        """

        pairname = sensor + "_" + date + "_" + catID_1 + "_" + catID_2
        imageDir = os.path.join(batchDir, pairname)

##        #DEL
##        print "Still running if catIDlist = 1:"
##        print "pairname:", pairname
##        print "imageDir:", imageDir
##        print ''



        #if len(catIDlist) < 2: ##** if there was one but not two catIDs of data for the pair, we want to get the info for the outCsv and move on to the next pair
        if found_catID.count(False) == 1:
            #print "\n\tMissing a catalog_id, can't do stereogrammetry. **review this print statement/placement with the one below in mind\n\n"
            preLogText.append("\n\tMissing a catalog_id, can't do stereogrammetry. **review this print statement/placement with the one below in mind\n\n")
            mapprj = False
            DSMdone = False
            #outAttributes = batchID + "," + pairname + "," + str(found_catID[0]) + "," + str(found_catID[1]) + "," + str(mapprj) + "," + str(year) + "," + str(month) + "," + str(avSunElev)+ "," + str(avSunAz) + "," + str(avOffNadir) + "," + str(avTargetAz) + "," + str(avSatAz) + "," +str(conv_ang) + "," + str(bie_ang) + "," + str(asym_ang) + "\n"
            outAttributes = '{},{},{},{},{},{},{},{},{},missingData\n'.format(batchID, pairname, catID_1, found_catID[0], catID_2, found_catID[1], mapprj, year, month)
            with open(summary_csv, 'a') as c:
                c.write(outAttributes)
            ##Q print statement here?

            # now write to input line of the missingData pair to the new query csv. but first, if it doesnt exist. write the header
            if not os.path.exists(newQcsv):
                with open(newQcsv, 'w') as nq:
                    nq.write(hdr)
            with open(newQcsv, 'a') as nq:
                nq.write(line)

            #* 1/17 print "\n\tMissing a catalog_id, can't do stereogrammetry." was how it was done in the workflow script
            preLogText.append("\n\t One of the catIDs does not have data in our archive for pair {}\n\n".format(pairname))
            print "One of the catIDs returned no data from our query. Moving to next pair\n"
            continue ##* move on to the next pair



##        # DEL
##        print "Before we are sure that catIDlist == 2:"
##        print "mapprj:", mapprj
##        try:
##            print "DSMdone:", DSMdone
##            print "outAttributes:", outAttributes
##        except Exception as e:
##            print "Couldn't print: ", e
##
##        print ""

##        # BEFORE doing copy, check to be sure imageDir does not exist. If it does, and there is at least two *ntf and *xml files in there, then skip.
##        if os.path.isdir(imageDir) and len(glob.glob(os.path.join(imageDir, '*ntf'))) >= 2 and len(glob.glob(os.path.join(imageDir, '*xml'))) >= 2:
##            print "imageDir (%s) already exists and contains at least two .ntf and .xml files. Skipping pair"
##            continue

        # let's be sure the pairname we are trying to run has not been run before in this batch:
        if pairname in unique_pairnames: # if it's in our list already
            print "Pairname %s has already been processed for this batch. Moving to next pair\n" % pairname
            outAttributes = '{},{},{},{},{},{},{},{},{},duplicate\n'.format(batchID, pairname, catID_1, found_catID[0], catID_2, found_catID[1], mapprj, year, month) # record that this is a same-batch duplicate
            with open(summary_csv, 'a') as c:
                c.write(outAttributes)
            continue

        # also check to be sure pairname was not already processed in an earlier batch by seeing if it exsits in outASP on ADAPT:
        checkOut = "/att/pubrepo/DEM/hrsi_dsm/{}/out-strip-DEM.txt".format(pairname)
        if os.path.isfile(checkOut): # already ran successfully
            print "Pairname %s has already been processed in a previous batch. Moving to next pair\n" % pairname
            outAttributes = '{},{},{},{},{},{},{},{},{},alreadyProcessed\n'.format(batchID, pairname, catID_1, found_catID[0], catID_2, found_catID[1], mapprj, year, month) # record that this was already processed earlier
            with open(summary_csv, 'a') as c:
                c.write(outAttributes)
            continue

        start_copy = timer()
        #** we will only get to this point if there is data for both catIDs- ##Q is that OK?
        # now that we have data for both, loop thru the strips again
        pair_data_exists = [False, False] # keeps track of whether scene data for either catID exists in ADAPT or not
        for num, catID in enumerate([catID_1,catID_2]):
            ##Q do we need to be in a pair loop (i.e. looping through the two catIDs?)

            ##Q does the info below (NGA path/pID/lat/lon) need to be printed to log if pairs arent going to be processed? If so, this block below needs to be moved up before if len(catIDlist) < 2

            print "  Copying data for pair %d of %d, catalog ID %s" % (pair_count, n_lines, catID) # print to ADAPT screen
            # retrieve list of scenes for catID
            selected = selected_list[num]

            # get lat long and path from first
            lat= float(selected[0][3])
            lon = float(selected[0][4])
            path_0 = os.path.split(selected[0][0])[0]


            preLogText.append("\n\tNGA dB path: %s" % path_0 )
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

            scene_exist_cnt = 0 # if this remains 0, uh oh. skip pair
            for row in selected: ##** now we are looping through the list of selected scenes for catID X
                ntf = row[0]
                filename, fileExt = os.path.splitext(ntf)
                xml = ntf.replace(fileExt, '.xml') # for ntf files

                if debug:
                    print ntf
                    print xml
                    print os.path.isfile(ntf)
                    print os.path.isfile(xml)
                    continue

                # ** FOR NOW: copy files if it exists. assumming if it doesnt exist the path changed to NGA, copy that instead
                if not os.path.isfile(os.path.join(imageDir, os.path.basename(ntf))): # if the file is not in the imageDir
                    ntf_replace = ntf.replace('NGA_Incoming/NGA', 'NGA')
                    if os.path.isfile(ntf):
                        #print "Copying %s" % ntf
                        os.system('cp %s %s' % (ntf, imageDir))
                    elif os.path.isfile(ntf_replace):
                        ntf = ntf_replace
                       # print "Copying %s" % ntf
                        os.system('cp %s %s' % (ntf, imageDir))
                    else: # if the file exists in none of these places
                        #print "   file does not exist in (%s) - delete later?" % ntf
                        continue # move to next scene, don't even try to get the xml

                if not os.path.isfile(os.path.join(imageDir, os.path.basename(xml))):
                    xml_replace = xml.replace('NGA_Incoming/NGA', 'NGA')
                    if os.path.isfile(xml):
                       # print "Copying %s" % xml
                        os.system('cp %s %s' % (xml, imageDir))
                    elif os.path.isfile(xml_replace):
                        xml = xml_replace
                       # print "Copying %s" % xml
                        os.system('cp %s %s' % (xml, imageDir))
                    else:
                        #print "   file does not exist in (%s) - delete later?" % xml
                        os.remove(ntf) # remove ntf file if xml does not exist
                        continue # move to next scene

                    # if we get here, both xml and ntf existed (we bypassed both else - continue statements)
                    # TD 4/5: we need to be sure the count goes up only if the above statements works
                scene_exist_cnt += 1 # add one to count

            if debug: continue # don't actually copy data

            if scene_exist_cnt == 0: # if no data was found in the NGA database for catID
                print "No data was found in ADAPT archive for pair %s. Skipping to next catID\n\n"
                continue
            else:
                pair_data_exists[num] = True # set catID side to True since scenes do exist

        if debug: sys.exit()

        if pair_data_exists == [True, True]: # both pairs have data
            #print "Data exists for each catID in pair"
            pass # then keep going

        else: # if there was no data for one or both catIDs
            print "There was no data for one or both catIDs in the ADAPT archive. Skipping to next pair\n\n"

            # now write to input line of the missingData pair to the new query csv. but first, if it doesnt exist. write the header
            if not os.path.exists(newQcsv):
                with open(newQcsv, 'w') as nq:
                    nq.write(hdr)
            with open(newQcsv, 'a') as nq:
                nq.write(line)

            # write out attributes to failue csv
            #outAttributes = batchID + "," + pairname + "," + str(found_catID[0]) + "," + str(found_catID[1]) + "," + str(mapprj) + "," + str(year) + "," + str(month) + "," + str(avSunElev)+ "," + str(avSunAz) + "," + str(avOffNadir) + "," + str(avTargetAz) + "," + str(avSatAz) + "," +str(conv_ang) + "," + str(bie_ang) + "," + str(asym_ang) + ", data does not exist on ADAPT\n"
            outAttributes = '{},{},{},{},{},{},{},{},{},missingData-ADAPT\n'.format(batchID, pairname, catID_1, found_catID[0], catID_2, found_catID[1], mapprj, year, month) # we should theoretically never get to this point but just in case have a separate queryResult value
            with open(summary_csv, 'a') as c:
                c.write(outAttributes)
            continue






            """
              Now we have all the raw data in the inASP subdir identified with the pairname
            """
        end_copy = timer()
        time_copy = round((end_copy - start_copy)/60, 3)
        print "Elapsed time to copy data for pair {} of {}, pairname {}: {} minutes\n".format(pair_count, n_lines, pairname, time_copy)

##        # DEL
##        print "After looping through catIDs to copy data:"
##        print "path_0", path_0
##        print ''


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

##        print '\n\npairname:', pairname
##        print 'prj:', prj #DEL

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
            mapprj=False # set mapprj to false. If mapprj is True, this will turn it to False. If it's false, nothing changes
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

            pair_DEM = os.path.join(imageDir, "dem-%s.tif" % pairname)
            cmdStr = "gdalwarp -t_srs EPSG:4326 -ot Int16  %s %s" % (DEM_inputs.strip(' '), pair_DEM)

            if not os.path.isfile(pair_DEM):
                wf.run_wait_os(cmdStr, print_stdOut=False)
                preLogText.append("\tCreated %s" % pair_DEM)
            else:
                preLogText.append("\tDEM (%s) already exists" % pair_DEM)


        n_pair_copy += 1 # if we get to this point we have successfully copied data for the pair
        unique_pairnames.append(pairname) # add pairname to list
       # print unique_pairnames, " (Delete)"

##        #DEL
##        print "After running if mapprj"
##        print "DEM_inputs:", DEM_inputs
##        print "pair_DEM:", pair_DEM
##        print ''

        #* 2/23 only writing to csv if pair fails
##        outAttributes = pairname + "," + str(found_catID[0]) + "," + str(found_catID[1]) + "," + str(mapprj) + "," + str(year) + "," + str(month) + "," + str(avSunElev)+ "," + str(avSunAz) + "," + str(avOffNadir) + "," + str(avTargetAz) + "," + str(avSatAz) + "," +str(conv_ang) + "," + str(bie_ang) + "," + str(asym_ang) + "," + "\n"
##        with open(csvOutFile, 'a') as c:
##            c.write(outAttributes)

        discover_imageDir = os.path.join(DISCdir, 'inASP/batch%s/%s' % (batchID, pairname)) # where data will be copied to (and thus the imageDir we need to pass)


        # write preLogText to a text file
        preLogTextFile = os.path.join(imageDir, 'preLogText_%s.txt' % pairname)
        with open(preLogTextFile, 'w') as tf:
            for r in preLogText:
                tf.write(r + '\n')
        preLogTextFile_DISC = os.path.join(discover_imageDir, os.path.basename(preLogTextFile)) # the path to where it's stored on DISCOVER

        # get the pair arguments that we need to send to DISCOVER:
        arg1 = '"{}"'.format(line.strip('"').strip().strip('"').strip()) # line is a string arg, remove any and all quotes or spaces then send it in double quotes
        arg2 = '::join::'.join(header).strip() # header is a list arg
        arg3 = discover_imageDir # imageDir on workflow side
        arg4 = mapprj
        arg5 = prj
        arg6 = utm_zone
        #arg6 = par - do we need? always false? #*
        #arg7 = test # do we need? always false?
        #arg8 = searchExtList = ...
        arg7 = doP2D
        #arg10 = stereoDef = ...
        #arg11 = dirDEM = ...
        arg8 = str(rp)
        #arg14 = preLogText # list arg
        arg9 = preLogTextFile_DISC
        arg10 = batchID

        arg11 = '::join::'.join(catIDlist)
        arg12 = '::join::'.join(pIDlist)
        arg13 = '"{}"'.format(imageDate.strftime("%Y-%m-%d")) # pass along imageDate as dtring in format "yyyy-mm-dd"
##        print arg19
##        print type(arg19)

        # imageDir
        # inDir/outDir
        # outAttributes ?
        # LogHeaderText - no, has been added to preLog text
        # outHeader
        # outHeaderList
        # catIDlist?
        # pIDlist ?


        # Create the individual job script:
        #job_script = os.path.join(imageDir, 'slurm_%s.j' % pairname)
        job_script = os.path.join(imageDir, 'slurm_batch%s_%s.j' % (batchID, pairname)) # do it like this instead?

        # CHANGE THESE ?:
        job_name = '%s__%s__job' % (batchID, pairname) # identify job with batchID and pairname??
        time_limit = '6-00:00:00'
        num_nodes = '1'
        #python_script_args = '%s %s %s arg3 etc' % (os.path.join(DISCdir, 'code', '<scriptName.py>'), arg1, arg2)
        python_script_args = 'python %s %s %s %s %s %s %s %s %s %s %s %s %s %s' % (os.path.join(DISCdir, 'code', workflowCodeName), arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13)
        #print python_script_args

        # slurm.j file (calls the python code in discover for just one pair)
        with open(job_script, 'wb') as f:
            f.write('#!/bin/csh -f\n')
            f.write('#SBATCH --job-name=%s\n' % job_name)
            f.write('#SBATCH --nodes=%s\n' % num_nodes)
            f.write('#SBATCH --constraint=hasw\n\n')
            f.write('#SBATCH --time=%s\n' % time_limit)
            f.write('#SBATCH --qos=boreal_b0217\n')
            f.write('#SBATCH --partition=single\n\n')
            f.write('source /usr/share/modules/init/csh\n\n')
            f.write('unlimit\n')
            f.write('module load other/comp/gcc-5.3-sp3\n')
            #f.write('module load other/SSSO_Ana-PyD/SApd_4.2.0_py2.7_gcc-5.3-sp3\r\n') # test2
            #f.write('module load other/SSSO_Ana-PyD/SApd_4.2.0_py2.7_gcc-5.3-sp3' + os.linesep + python_script_args) # test3
            #f.write('module load other/SSSO_Ana-PyD/SApd_4.2.0_py2.7_gcc-5.3-sp3\n%s' % python_script_args) # test4
            f.write('module load other/SSSO_Ana-PyD/SApd_4.2.0_py2.7_gcc-5.3-sp3\n\n') # test5
##            f.write(' \n')
            f.write(python_script_args + '\n')

        #TD: write to summary csv
        #outAttributes = batchID + "," + pairname + "," + str(found_catID[0]) + "," + str(found_catID[1]) + "," + str(mapprj) + "," + str(year) + "," + str(month) + "," + str(avSunElev)+ "," + str(avSunAz) + "," + str(avOffNadir) + "," + str(avTargetAz) + "," + str(avSatAz) + "," +str(conv_ang) + "," + str(bie_ang) + "," + str(asym_ang) +"\n"
        outAttributes = '{},{},{},{},{},{},{},{},{},processing\n'.format(batchID, pairname, catID_1, found_catID[0], catID_2, found_catID[1], mapprj, year, month)
        with open(summary_csv, 'a') as c:
            c.write(outAttributes) ##* append the attributes (mostly blank at this point) to the csv file list


        #TD: do I need to add arguments to the slurm.j call? don't think so because all it does is call the slurm.j script which WILL have args
        with open(submission_file, 'a') as ff:
            #ff.write('\ncd %s\nchmod 755 %s\nsbatch %s' % (discover_imageDir, os.path.basename(job_script), os.path.basename(job_script)))
            #ff.write("\ncd {0}\nchmod 755 {1}\nod -xc {1} | tail -5\nsed -i '$a\\' {1}\nod -xc {1} | tail -5\nsbatch {1}".format(discover_imageDir, os.path.basename(job_script))) # test5
            #ff.write('\ncd %s\nchmod 755 %s\ndos2unix %s\nsbatch %s' % (discover_imageDir, os.path.basename(job_script), os.path.basename(job_script), os.path.basename(job_script)))
            ff.write("\ncd {0}\nchmod 755 {1}\nsed -i '$a\\' {1}\nsbatch {1}".format(discover_imageDir, os.path.basename(job_script))) # do the sed just in case. this arg says add newline to end of file only if one is not already there

       # print ''

    #print "\n%d of the %d input pairs were successfully copied" % (n_pair_copy, n_lines)

    # now write list of catIDs that do not have data to the batch-level text file that was set up earlier. only if there are missing catIDs -- this is done above now
##    n_missing_catIDs = len(missing_catIDs) # counting/writing to list as we go now
    if n_missing_catIDs > 0: print "\n- Wrote %d catIDs to missing catID list %s" % (n_missing_catIDs, missing_catID_file) # only thing we wanna do is print how many files
##        with open(missing_catID_file, 'w') as mf:
##            for m in missing_catIDs: mf.write(m +'\n')
##        print "\n- Wrote %d catIDs to missing catID list %s" % (n_missing_catIDs, missing_catID_file)

    # copy summary csv to summary_csvs directory:
    os.system('cp %s %s' % (summary_csv, os.path.join(os.path.dirname(inDir.rstrip('/')), 'batch_summary_csvs')))

    # NOW TAR everything in the batchDir into archive
    start_tarzip = timer()
    archive = os.path.join(inDir, 'batch%s-archive.tar.gz' % batchID)
    print "\n\n--------------------------------------------\nAttempting to archive data now for entire batch (%d of %d pairs)..." % (n_pair_copy, n_lines)
    if not os.path.exists(archive): # if data has not yet been tarred up (careful with this)
        print "\n Begin archiving:", datetime.now().strftime("%I:%M%p  %a, %m-%d-%Y")
       # tarComm = 'tar -zcvf %s %s' % (archive, batchDir) #TD get rid of -v possibly when doing real
       # tarComm = 'tar -zcvf %s %s -C %s batch%s' % (archive, batchDir, inDir, batchID) #TD get rid of -v possibly when doing real
        tarComm = 'tar -zcf %s -C %s batch%s' % (archive, inDir, batchID) # archive=output archive; cd into inDir then tar up only batchdir (i.e. batch$batchID in the inDir )
        print ' ' + tarComm
        os.system(tarComm)
        print " Finish archiving:", datetime.now().strftime("%I:%M%p  %a, %m-%d-%Y")
        end_tarzip = timer()
        time_tarzip = round(find_elapsed_time(start_tarzip, end_tarzip),3)
        print "Elapsed time for tarring/zipping %d pairs: %s minutes" % (n_pair_copy, time_tarzip)
    else:
        print " Archive %s already exists" % archive
        time_tarzip = 0


    end_main = timer()
    time_main = round(find_elapsed_time(start_main, end_main), 3)
    print "\nElapsed time for entire run (%d out of %d pairs): %s minutes" % (n_pair_copy, n_lines, time_main)

    # lastly we need to append to the main processing summary: batchID/date, input csv file, number of pairs attempted, number succeeded, time to zip, total time
    main_summary = os.path.join(os.path.dirname(inDir.rstrip('/')), 'main_processing_summary.csv') # this is not in Paul_TTE/inASP but in Paul_TTE/
    with open(main_summary, 'a') as ms:
        ms.write('{}, {}, {}, {}, {}, {}, {}\n'.format(batchID, os.path.abspath(csv), n_pair_copy, n_lines, n_missing_catIDs, time_tarzip, time_main))


    print "End:", datetime.now().strftime("%m%d%y-%I%M%p"), "\n\n"

if __name__ == '__main__':

    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", help = "Input CSV with pairs to be queried and processed") #required
    ap.add_argument("inDir", help = "inASP directory where batch/pair input data will be stored") # required
    ap.add_argument("batchID", help = "Batch identifier") #required
    ap.add_argument("-mapprj", action='store_true', help="Include -mapprj tag at the command line if you wish to mapproject") # if "-mapprj" is NOT included at the command line, it defaults to False. if it IS, mapprj gets set to True
    ap.add_argument("-noP2D", action='store_true', help="Include -noP2D tag at the command line if you do NOT wish to run P2D") # if "-noP2D" is NOT included at the CL, it defaults to False. doP2D = not noP2D
    ap.add_argument("-rp", default=100, type=int, help="Reduce Percent, default = 100")
    ap.add_argument("-debug", action='store_true', help="Include -debug if you wish to run in debug mode") # if -debug is NOT included at the CL, it defaults to False

    kwargs = vars(ap.parse_args()) # parse args and convert to dict

    main(**kwargs) # run main with arguments


##    if len(sys.argv) == 4: # i.e. 3 arguments are supplied
##        main(sys.argv[1], sys.argv[2], sys.argv[3])
##
##    if len(sys.argv) == 5: # i.e. 4 arguments are supplied
##        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
##
##    if len(sys.argv) == 6: # i.e. 5 arguments are supplied
##        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
##
##    if len(sys.argv) == 7: # i.e. 6 arguments are supplied
##        main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])
##
##    #main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])#, sys.argv[4], sys.argv[5], sys.argv[6])
