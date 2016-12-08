#-------------------------------------------------------------------------------
# Name:         workflow_HRSI_v17_smry_run.py
# Purpose:      science...
#
# Author:       paul.m.montesano@nasa.gov
#               301.614.6642
#
# Created:
# Modified by:
#       PM                  Dec 2016    : run_asp_smry() --> produces symlinks and inASP subdir, but just returns an output *smry.csv file that reports which catids from the input CSV are found in the postgres DB.
#-------------------------------------------------------------------------------
# #!/bin/python
# #!/usr/bin/env python
###############################################
# Import and function definitions
import os, sys, math, osgeo, shutil, time, glob, gdalinfo, platform, csv, subprocess as subp
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


def run_asp_smry(
    csv,
    outDir,
    inDir,
    prj='EPSG:32647'
    ):

    LogHeaderText = []

    mapprj = False

    LogHeaderText.append("Input csv file:")

    # Now the norm. No csv splitting done. Use csv specified in argument.
    # This option used for looping one at a time through a main file, or smaller 'clean-up' type runs
    LogHeaderText.append(csv)

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

        #  CSV Loop --> runs parallel_stereo for each line of CSV across all VMs
        #
        # [3] Loop through the lines (the records in the csv table), get the attributes and run the ASP commands
        for line in csvLines:
            preLogText = []

            # Get attributes from the CSV
            linesplit = line.rstrip().split(',')
            preLogText.append("Current line from CSV file:")
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
            #
            # Establish the database connection
            with psycopg2.connect(database="NGAdb01", user="anon", host="ngadb01", port="5432") as dbConnect:

                cur = dbConnect.cursor() # setup the cursor
                catIDlist = [] # build now to indicate which catIDs were found, used later
                pIDlist = []
                found_catID = [False,False]

                # Setup and execute the query on both catids of the stereopair indicated with the current line of the input CSV
                for num, catID in enumerate([catID_1,catID_2]):

                    selquery =  "SELECT s_filepath, sensor, acq_time, cent_lat, cent_long FROM nga_files_footprint_v2 WHERE catalog_id = '%s'" %(catID)
                    preLogText.append( "\n\t Now executing database query on catID '%s' ..."%catID)
                    cur.execute(selquery)
                    selected=cur.fetchall()
                    preLogText.append( "\n\t Found '%s' scenes for catID '%s' "%(len(selected),catID))

                    # Get info from first item returned
                    if len(selected) == 0:
                        found_catID[num] = False
                    else:
                        found_catID[num] = True
                        # Get center coords for finding UTM zone and getting ASTER GDEM tiles
                        lat = float(selected[0][3])
                        lon = float(selected[0][4])
                        path_0 = os.path.split(selected[0][0])[0]
                        preLogText.append("\n\tNGA dB path: %s" %path_0 )

                        # Get productcatalogid from this first dir: sometimes 2 are associated with a catid, and represent duplicate data from different generation times
                        pID = os.path.split(path_0)[1].split('_')[-2]   ## each file of a given catID also needs to have this string
                        preLogText.append("\tProduct ID: %s" %str(pID))
                        preLogText.append("\tCenter Lat: %s" %str(lat))
                        preLogText.append("\tCenter Lon: %s" %str(lon))

                        # If > 0 items returned from search, add catID to list and add product ID to list
                        catIDlist.append(catID)
                        pIDlist.append(pID)

                        # [4.1] Make imageDir
                        #   into which you'll direct the symbolic link inputs and store intermediate mosaics
                        #   Return date from first row (formatted for filename)
                        sensor = str(selected[0][1])                        # eg. WV02
                        date = str(selected[0][2]).replace("-","")          # eg. 20110604
                        year = date.strip()[:-4]
                        month = date.strip()[4:].strip()[:-2]
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

            # -----------------------
            # For logging on the fly
            lfile = os.path.join(outDir,'logs','run_smryDB_LOG_' + imageDir.split('/')[-1].rstrip('\n') +'_' + platform.node() + '_' + strftime("%Y%m%d_%H%M%S") + '.txt')
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
                conv_ang, bie_ang, asym_ang = g.stereopairs(imageDir)
            except Exception, e:
                print "\n\tStereo angles not calc'd b/c there is no input for both catIDs"

            outAttributes = pairname + "," + str(found_catID[0]) + "," + str(found_catID[1]) + "," + str(mapprj) + "," + str(year) + "," + str(month) + "," + str(avSunElev)+ "," + str(avSunAz) + "," + str(avOffNadir) + "," + str(avTargetAz) + "," + str(avSatAz) + "," +str(conv_ang) + "," + str(bie_ang) + "," + str(asym_ang) + "," + str(DSMdone) +"\n"
            # Write out CSV summary info
            csvOut.write(outAttributes)

if __name__ == "__main__":
    import sys
    run_asp_smry( sys.argv[1], sys.argv[2], sys.argv[3] )