#-------------------------------------------------------------------------------
# Name:        Get_stereo_pairs.py
# Purpose:
#
# Author:      pmontesa
#
# Created:     30/04/2014
#               June 2014: jvandenh edits >>
#                   1. output unique (pseudo-)stereo pairs to csv
#                   2. include deltaTime variables and full metadata for imagery in csv
#                   3. include options to filter by (pseudo-)stereo pair parameters
#                   4. calculate convergence angle directly in this script and export to csv
#                   5. include all csv fields in shp dbf
#
# Copyright:   (c) pmontesa 2014
# Licence:     <your licence>
#-------------------------------------------------------------------------------
###############################################
# Import and function definitions
import os, sys, math, osgeo, csv
from osgeo import ogr, osr, gdal
import shapefile as shp
#import gdalinfo
import tarfile
import datetime, time
from datetime import datetime
gdal.AllRegister() #register all raster format drivers
###############################################
start_time = time.time()
# Function for calculating a 3x3 determinant
def det3(a1, b1, c1, a2, b2, c2, a3, b3, c3):
    res = a1*b2*c3+a2*b3*c1+a3*b1*c2-a1*b3*c2-a2*b1*c3-a3*b2*c1
    return res

def stereoAngles(alpha1,theta1,alpha2,theta2,x1,y1,z1,x2,y2,z2,lat,lon):
    """
    alpha1  =   meanSatEl of image 1
    theta1  =   meanSatAz of image 1
    alpha2  =   "" image 2
    theta2  =   "" image 2
    x,y,z   = satellite empheris

    http://www.mdpi.com/2072-4292/7/4/4549/remotesensing-07-04549.pdf
    http://www.geoimage.com.au/media/brochure_pdfs/DEMBrochure_FEB2015.pdf
    www.isprs.org/proceedings/XXXVII/congress/1_pdf/195.pdf

    In the case of the VHR satellites with their pointable telescopes, the B/H ratio is not appropriate as a measure
    of the effectiveness of the stereo pair for DEM generation. In such cases, three angular measures of
    convergent stereo imaging geometry: the convergence angle, the asymmetry angle, and the bisector elevation angle (BIE) are used.

    These measure the geometrical relationship between two rays that intersect at a common ground point, one
    from the fore image and one from the aft image as shown in the diagram.

    Convergence Angle:
    The angle between two rays of a stereo pair
    The most important of the three stereo angles is the convergence and is the angle between the two rays in the
    convergence or epipolar plane. An angle between 30 and 60 degrees is ideal (<--- ideal for measuring what?? which heights? short trees??)

    Asymetry Angle:
    Asymmetry describes the apparent offset from the centre view that a stereo pair has. For instance, a stereo pair
    with an asymmetry of 0? will have parallax due to elevations that appear equivalent in the left and right images.
    An asymmetrical collection is preferred as it gives a different look angle to discern ground features more
    accurately but should be under 20 deg.

    Bisector Elevation Angle:
    The obliqueness of the epipolar plane. BIE = 90 is orthogonal to ground surface
    The elevation angle of the bisector of the convergence angle
    The BIE angle is the angle between the horizontal plane and the epipolar plane and defines the amount of parallax that will
    appear in the vertical direction after alignment. The angle should be between 60 and 90 degrees.
    """
    # Converts degrees to radians
    dtr = math.atan(1.0)/45.0

    # Set Earth Radius
    r = 6378137   # WGS84 equatorial earth radius in meters

    a = math.sin(alpha1*dtr) * math.sin(alpha2*dtr)+ math.cos(alpha1*dtr) * math.cos(alpha2*dtr)* math.cos((theta1-theta2)*dtr)
    con_ang = round(math.acos(a)/dtr, 2)

    x0 = r * math.cos(lat*dtr) * math.cos(lon*dtr)
    y0 = r * math.cos(lat*dtr) * math.sin(lon*dtr)
    z0 = r * math.sin(lat*dtr)

    a = det3(y0,z0,1.0,y1,z1,1.0,y2,z2,1.0)
    b = -det3(x0,z0,1.0,x1,z1,1.0,x2,z2,1.0)
    c = det3(x0,y0,1.0,x1,y1,1.0,x2,y2,1.0)

##    print alpha1,alpha2,theta1,theta2
##    print a,b,c
##    print x0,y0,z0

    if int(a) == 0 or int(b) == 0 or int(c) == 0:
        return (-99999,-99999,-99999)
    else:
        sc = abs(a*x0 + b*y0 + c*z0)/(math.sqrt(x0*x0+y0*y0+z0*z0) * math.sqrt(a*a + b*b + c*c))
        bie_ang = round(math.asin(sc)/dtr, 2)
        a = x1+x2-2*x0
        b = y1+y2-2*y0
        c = z1+z2-2*z0
        sc = abs(a*x0 + b*y0 + c*z0)/(math.sqrt(x0*x0+y0*y0+z0*z0) * math.sqrt(a*a + b*b + c*c))
        asym_ang = round(math.asin(sc) / dtr,2)
        return (con_ang,asym_ang,bie_ang)


#############################

def stereopairs(imageDir):
    """
    imageDir        =   Top dir from which a pair of XMLs are read in

    Make lists of each set of XMLs for each catID in the iamgeDir name
    Calc angles for all combinations of XMLs from each catID list

    Output a CSV file with stereo angles and input XML info
    """
    # Create header
    hdr = "catID,SatEl,SatAz,SunEl,SunAz,CTVA,ONVA,ephemX,ephemY,ephemZ,ullon,ullat,lllon,lllat,urlon,urlat,lrlon,lrlat,centLon,centLat"##+\
            ##"catID,meanSatEl,meanSatAz,meanSunEl,meanSunAz,ephemX,ephemY,ephemZ,ullon,ullat,lllon,lllat,urlon,urlat,lrlon,lrlat,centLon,centLat"
    hdr =   "left_scene,right_scene,"+\
            "SatEl,SatAz,SunEl,SunAz,CTVA,ONVA,SatEl,SatAz,SunEl,SunAz,CTVA,ONVA,"+\
            "centLon,centLat,centLon,centLat,"+\
            "ephemX,ephemY,ephemZ,ephemX,ephemY,ephemZ,"+\
            "ullon,ullat,lllon,lllat,urlon,urlat,lrlon,lrlat,ullon,ullat,lllon,lllat,urlon,urlat,lrlon,lrlat"

    # Get pairname from input image dir
    baseDir, pairname = os.path.split(imageDir)
    print("\tPairname: %s" %(pairname))

    # Split pairname into catids
    cat1,cat2 = pairname.split("_")[2:]

    # Initialize lists
    cat1list = []
    cat2list = []

    # Create catid lists
    for root, dirs, files in os.walk(imageDir):
        for each in files:
            # Identify only xmls belonging to scenes
            if each.endswith('.xml') and '1BS' in each:
                if cat1 in each:
                    cat1list.append(each)
                if cat2 in each:
                    cat2list.append(each)

    # Name output csv with the pairname and put in output ASP dir
    outCSV = os.path.join(imageDir,pairname + ".csv")
    ##print("\tOuput CSV file: %s" %(outCSV))

    # Open a CSV for writing
    with open(outCSV,'wb') as csvfile:

        # Write the header
        csvfile.write(hdr + ",ang_conv,ang_bie,ang_asym\n")

        # Get all combos of scenes from each catid strip:
        for leftXML in cat1list:
            for rightXML in cat2list:
                with open(os.path.join(imageDir,leftXML), 'r') as file1, open(os.path.join(imageDir,rightXML),'r') as file2:
                    i = 0

                    # Initialize vars
                    outline, catID = ('' for i in range(2))
                    meanSatEl,meanSatAz,meanSunEl,meanSunAz,meanCTVA,meanONVA,\
                    ephemX,ephemY,ephemZ,\
                    ullat,ullon,lllat,lllon,urlat,urlon,lrlat,lrlon,\
                    maxLat,minLat,maxLon,minLon,centLat,centLon = (0 for i in range(23))
                    catIDs, SSGangles, ephemeris, centCoords, cornerCoords = ('' for i in range(5))

                    # Loop through XML files
                    for file in (file1,file2):
                        # Keep track of file
                        i += 1
                        # Read  XML line by line
                        for line in file.readlines():
                            ##print(line)
                            # Get needed vars initialize above
                            if 'CATID' in line:
                                catID = str(line.replace('<','>').split('>')[2])
                            if 'MEANSATEL' in line:
                                meanSatEl = float(line.replace('<','>').split('>')[2])
                            if 'MEANSATAZ' in line:
                                meanSatAz = float(line.replace('<','>').split('>')[2])
                            if 'MEANSUNEL' in line:
                                meanSunEl = float(line.replace('<','>').split('>')[2])
                            if 'MEANSUNAZ' in line:
                                meanSunAz = float(line.replace('<','>').split('>')[2])
                            if 'MEANCROSSTRACKVIEWANGLE' in line:
                                meanCTVA = float(line.replace('<','>').split('>')[2])
                            if 'MEANOFFNADIRVIEWANGLE' in line:
                                meanONVA = float(line.replace('<','>').split('>')[2])
                            # Get Satellite Ephemeris using the first entry in EPHEMLISTList.
                            if '<EPHEMLIST>' in line and float(line.replace('<','>').replace('>', ' ').split(' ')[2]) == 1:
                                ephemX = float(line.replace('<','>').replace('>', ' ').split(' ')[3])
                                ephemY = float(line.replace('<','>').replace('>', ' ').split(' ')[4])
                                ephemZ = float(line.replace('<','>').replace('>', ' ').split(' ')[5])
                            if 'Upper Left' in line:
                                ullon = float(line.replace('(',')').split((')'))[1].split(',')[0])
                                ullat = float(line.replace('(',')').split((')'))[1].split(',')[1])
                            if 'Lower Left' in line:
                                lllon = float(line.replace('(',')').split((')'))[1].split(',')[0])
                                lllat = float(line.replace('(',')').split((')'))[1].split(',')[1])
                            if 'Upper Right' in line:
                                urlon = float(line.replace('(',')').split((')'))[1].split(',')[0])
                                urlat = float(line.replace('(',')').split((')'))[1].split(',')[1])
                            if 'Lower Right' in line:
                                lrlon = float(line.replace('(',')').split((')'))[1].split(',')[0])
                                lrlat = float(line.replace('(',')').split((')'))[1].split(',')[1])
                            if 'ULLON' in line:
                                ullon = float(line.replace('<','>').split(('>'))[2])
                            if 'ULLAT' in line:
                                ullat = float(line.replace('<','>').split(('>'))[2])
                            if 'URLON' in line:
                                urlon = float(line.replace('<','>').split(('>'))[2])
                            if 'URLAT' in line:
                                urlat = float(line.replace('<','>').split(('>'))[2])
                            if 'LLLON' in line:
                                lllon = float(line.replace('<','>').split(('>'))[2])
                            if 'LLLAT' in line:
                                lllat = float(line.replace('<','>').split(('>'))[2])
                            if 'LRLON' in line:
                                lrlon = float(line.replace('<','>').split(('>'))[2])
                            if 'LRLAT' in line:
                                lrlat = float(line.replace('<','>').split(('>'))[2])

                            maxLat = max(ullat,urlat,lllat,lrlat)
                            minLat = min(ullat,urlat,lllat,lrlat)
                            maxLon = max(ullon,urlon,lllon,lrlon)
                            minLon = min(ullon,urlon,lllon,lrlon)
                            centLat = minLat + (maxLat - minLat)/2
                            centLon = minLon + (maxLon - minLon)/2

                                # Just capture these vars for the first file.
                        if i == 1:
                            meanSatAz_1 = meanSatAz
                            meanSatEl_1 = meanSatEl
                            ephemX_1 = ephemX
                            ephemY_1 = ephemY
                            ephemZ_1 = ephemZ

                        # From both images:
##                        # get catIDs
##                        catIDs      +=  str(catID)      + ','
                        # Get scene names instead of catids
                        Names  =  leftXML + ',' + rightXML + ','

                        # gather Sun-Sensor Geometry Angles
                        SSGangles   +=  str(meanSatEl)  + ',' + str(meanSatAz) + ',' + \
                                        str(meanSunEl)  + ',' + str(meanSunAz) + ',' + \
                                        str(meanCTVA)  + ',' + str(meanONVA) + ','

                        # gather satellite ephemeris XYZ
                        ephemeris   +=  str(ephemX)     + ',' + str(ephemY) + ',' + str(ephemZ) + ','

                        # gather center coords
                        centCoords  +=  str(centLon)    + ',' + str(centLat) + ','

                        # gather corners
                        cornerCoords+=  str(ullon)      + ',' + str(ullat) + ',' + \
                                        str(lllon)      + ',' + str(lllat) + ',' + \
                                        str(urlon)      + ',' + str(urlat) + ',' + \
                                        str(lrlon)      + ',' + str(lrlat) + ','

                        outline     +=  str(catID)      + ',' + \
                                        str(meanSatEl)  + ',' + str(meanSatAz) + ',' + \
                                        str(meanSunEl)  + ',' + str(meanSunAz) + ',' + \
                                        str(meanCTVA)   + ',' + str(meanONVA) + ',' + \
                                        str(ephemX)     + ',' + str(ephemY) + ',' + str(ephemZ) + ',' + \
                                        str(ullon)      + ',' + str(ullat) + ',' + \
                                        str(lllon)      + ',' + str(lllat) + ',' + \
                                        str(urlon)      + ',' + str(urlat) + ',' + \
                                        str(lrlon)      + ',' + str(lrlat) + ',' + \
                                        str(centLon)    + ',' + str(centLat) + ','

                    # Calc stereo angles
                    stereoAngs = stereoAngles(meanSatEl_1,meanSatAz_1,meanSatEl,meanSatAz,ephemX_1,ephemY_1,ephemZ_1,ephemX,ephemY,ephemZ,centLat,centLon)

                    # Write line
                    csvfile.write(Names + SSGangles + centCoords + ephemeris + cornerCoords + str(stereoAngs[0]) + "," + str(stereoAngs[1]) + "," + str(stereoAngs[2])+'\n')
                    ##csvfile.write(outline + str(stereoAngs[0]) + "," + str(stereoAngs[1]) + "," + str(stereoAngs[2]))
                    print("\tOuput CSV file: %s" %(outCSV))
                    print("\tConvergence Angle = " + str(stereoAngs[0]))
                    print("\tBisector Elevation Angle = " + str(stereoAngs[1]))
                    print("\tAsymetry Angle = " + str(stereoAngs[2]))
                    return(str(stereoAngs[0]),str(stereoAngs[1]), str(stereoAngs[2]))


                    ##writer.writerow(hdr)





##"""
##Finds and footprints stereo pair coverage within an image directory
##
##After running the HiRes footprinting code, this script reads the *_metadata.csv file
##Untar the data's .tar files and read the XML data
##Identify stereo pairs based on name,time, date, and center lat/lons
##
##Create a stereo pair record listing both members of stereo pairs, as well as satellite ephemeris
##info and camera info for each image pair.
##    MEANSATEL, MEANSATAZ, MEANOFVA, EPHEMx, EPHEMy, EPHEMz
##Create a stereo overlap box used to generate a shapefile of stereo coverage (approx)
##Gets cloud cover info from the XML (from one of the 2 in the pair - they should be generally close)
##
##Output: shapefile and corresponding csv. Use csv to run 'calc_stereo_pair_angles.py'
##"""
##
##def stereopairs_old(
##    directRaw,
##    outFileNameEnd='_STEREO_info',
##    matchRC = False, # do RC tiles need to match?
##    maxDeltaT = 30, # maximum allowable time difference in seconds between stereo imagery; 0: no time limit
##    maxLatDelta = 90, # max allowable difference in lat; default: 0.05
##    maxLonDelta = 90, # max allowable difference in lon; default: 0.17
##    maxCloudCov = 100, # max allowable CC in either image
##    minConvAng = 0
##    ):
##
##    direct = directRaw + "/"
##    # Read csv of stereo metadata
##    if direct.split('/')[1] == '':                                              # script called at drive root
##        fileName = direct.strip('/').strip(':')
##    else:                                                                       # script called at subdirectory
##        fileName = directRaw.split('/')[len(directRaw.split('/'))-1]
##
##    # if CSV exists in dir with imagery
##    if os.path.isfile(direct+fileName+'_metadata.csv'):
##        csvStereo = open(direct+fileName+'_metadata.csv', 'r')                  # named for folder containing imagery
##
##    # if CSV exists in dir above imagery
##    csvFile = os.path.join(direct.rsplit('/',2)[0],fileName + '_metadata.csv')
##    ##print(csvFile)
##    if os.path.isfile(csvFile):
##        csvStereo = open(csvFile, 'r')                                          # named for folder containing imagery
##
##    # Get the header
##    header = csvStereo.readline().strip('\n')
##
##    # Get the position of the cols of interest
##    nameIdx     =   header.split(',').index('name')
##    timeIdx     =   header.split(',').index('acqtime')
##    LatIdx      =   header.split(',').index('centLat')
##    LonIdx      =   header.split(',').index('centLon')
##    sensBands   =   header.split(',').index('spec')
##    LLIdx       =   header.split(',').index('LL')
##    LRIdx       =   header.split(',').index('LR')
##    URIdx       =   header.split(',').index('UR')
##    ULIdx       =   header.split(',').index('UL')
##
##    stereoDict = {}                                                             # indexed by image name; will hold metadata
##
##    # Loop through lines of stereo csv
##    for line in csvStereo.readlines():
##
##        # Get the attributes from an image
##        curImageName = line.split(',')[nameIdx]
##        curImageTime = line.split(',')[timeIdx]
##        ##print('curImageTime: %s' %(curImageTime))
##
##        if curImageTime != '':
##            t_cur = datetime.strptime(curImageTime,"%Y-%m-%dT%H:%M:%S.%fZ")
##
##        if line.split(',')[LatIdx] != '':
##            curImageLat = float(line.split(',')[LatIdx])
##
##        if line.split(',')[LonIdx] != '':
##            curImageLon = float(line.split(',')[LonIdx])
##
##        curImageSB = line.split(',')[sensBands]
##
##        curMeanSatEl    = float(line.split(',')[-10])
##        curMeanSatAz    = float(line.split(',')[-9])
##        curMeanONVA     = float(line.split(',')[-8])
##        curEphemX       = float(line.split(',')[-7])
##        curEphemY       = float(line.split(',')[-6])
##        curEphemZ       = float(line.split(',')[-5])
##        curImageCC      = float(line.split(',')[-2])
##
##        # Get corner coords for current image
##        curLL = line.split(',')[LLIdx].split()
##        curLL = [float(curLL[0]),float(curLL[1])] # format --> [lat,lon]
##
##        curLR = line.split(',')[LRIdx].split()
##        curLR = [float(curLR[0]),float(curLR[1])]
##
##        curUR = line.split(',')[URIdx].split()
##        curUR = [float(curUR[0]),float(curUR[1])]
##
##        curUL = line.split(',')[ULIdx].split()
##        curUL = [float(curUL[0]),float(curUL[1])]
##
##        # Set up a dictionary to hold attributes
##        ## stereoDict[image] = [sensor, date/time, cloud cover, center lat, center lon, LL, LR, UR, UL,
##        ##                       sat el, sat az, ONVA, ephemX, ephemY, ephemZ]
##        stereoDict[curImageName] = [curImageSB,t_cur,curImageCC,curImageLat,curImageLon,curLL,curLR,curUR,curUL,
##                                    curMeanSatEl,curMeanSatAz,curMeanONVA,curEphemX,curEphemY,curEphemZ]
##
##    stereoNameList = stereoDict.keys()
##
##    # Prepare a csv to hold stereo pair info
##    csvSTEREOFOOT = open(direct+fileName+outFileNameEnd+'.csv', 'w') # named for folder containing imagery
##    csvSTEREOFOOT.write('Image_1,Image_2,Sensor,Date_1,Date_2,Time_1,Time_2,'+ # header
##                        'deltaYear,deltaMonth,deltaDay,deltaHour,CloudCov_1,CloudCov_2,'+
##                        'ovlapCentLat,ovlapCentLon,ovlapLLx,ovlapLLy,ovlapLRx,ovlapLRy,ovlapURx,ovlapURy,ovlapULx,ovlapULy,'+
##                        'MeanSatEl_1,MeanSatAz_1,MeanONVA_1,ephemX_1,ephemY_1,ephemZ_1,'+
##                        'MeanSatEl_2,MeanSatAz_2,MeanONVA_2,ephemX_2,ephemY_2,ephemZ_2,'+
##                        'convAngle,asymAngle,bieAngle\n')
##
##    stereoPairsDict={}
##    image1Idx = 0
##    numPairs = 0
##    errorConvAng = 0
##
##    while image1Idx < len(stereoNameList)-1:
##        image2Idx = image1Idx+1
##        while image2Idx < len(stereoNameList):
##            print '1: '+str(image1Idx)+', 2: '+str(image2Idx)
##            image1Name = stereoNameList[image1Idx]
##            image2Name = stereoNameList[image2Idx]
##
##            image1SB = stereoDict[image1Name][0]
##            image2SB = stereoDict[image2Name][0]
##
##            image1Time = stereoDict[image1Name][1]
##            image2Time = stereoDict[image2Name][1]
##            t_dif = abs(image1Time-image2Time)
##
##            image1CC = stereoDict[image1Name][2]
##            image2CC = stereoDict[image2Name][2]
##            if image1CC == -99900.0: image1CC = 100
##            if image2CC == -99900.0: image2CC = 100
##
##            image1Lat = stereoDict[image1Name][3]
##            image1Lon = stereoDict[image1Name][4]
##            image2Lat = stereoDict[image2Name][3]
##            image2Lon = stereoDict[image2Name][4]
##            lat_dif = abs(image1Lat-image2Lat)
##            lon_dif = abs(image1Lon-image2Lon)
##
##            # Now for a potential stereo pair, get the polygon of overlap
##            # Get corner coords for match image
##            #   max UL x        min UR x
##            #   min UL y        min UR y
##            #
##            #   max LL x        min LR x
##            #   max LL y        max LR y
##
##            image1LL = stereoDict[image1Name][5] # format --> [lat,lon]
##            image1LR = stereoDict[image1Name][6]
##            image1UR = stereoDict[image1Name][7]
##            image1UL = stereoDict[image1Name][8]
##
##            image2LL = stereoDict[image2Name][5]
##            image2LR = stereoDict[image2Name][6]
##            image2UR = stereoDict[image2Name][7]
##            image2UL = stereoDict[image2Name][8]
##
##            # these variables need to be updated following calculation
##            # of the actual geometry of the overlap
##            ovlapLL = [float(max(image1LL[0],image2LL[0])),float(max(image1LL[1],image2LL[1]))]
##            ovlapLR = [float(max(image1LR[0],image2LR[0])),float(max(image1LR[1],image2LR[1]))]
##            ovlapUR = [float(max(image1UR[0],image2UR[0])),float(max(image1UR[1],image2UR[1]))]
##            ovlapUL = [float(max(image1UL[0],image2UL[0])),float(max(image1UL[1],image2UL[1]))]
##
##            maxLat = max(ovlapUL[0],ovlapUR[0])
##            minLat = min(ovlapLL[0],ovlapLR[0])
##            maxLon = max(ovlapUL[1],ovlapUR[1])
##            minLon = min(ovlapLL[1],ovlapLR[1])
##            ovlapCentLat = minLat + (maxLat - minLat)/2
##            ovlapCentLon = minLon + (maxLon - minLon)/2
##
##            meanSatEl1 = stereoDict[image1Name][9]
##            meanSatAz1 = stereoDict[image1Name][10]
##            meanSatONVA1 = stereoDict[image1Name][11]
##            ephemX1 = stereoDict[image1Name][12]
##            ephemY1 = stereoDict[image1Name][13]
##            ephemZ1 = stereoDict[image1Name][14]
##
##            meanSatEl2 = stereoDict[image2Name][9]
##            meanSatAz2 = stereoDict[image2Name][10]
##            meanSatONVA2 = stereoDict[image2Name][11]
##            ephemX2 = stereoDict[image2Name][12]
##            ephemY2 = stereoDict[image2Name][13]
##            ephemZ2 = stereoDict[image2Name][14]
##
##            # returns (con_ang,asym_ang,bie_ang)
##            # calcConvergenceAngle(alpha1,theta1,alpha2,theta2,x1,y1,z1,x2,y2,z2,lat,lon)
##            convAng = calcConvergenceAngle(meanSatEl1,meanSatAz1,meanSatEl2,meanSatAz2,ephemX1,ephemY1,ephemZ1,ephemX2,ephemY2,ephemZ2,ovlapCentLat,ovlapCentLon)
##            if convAng == (-99999,-99999,-99999): errorConvAng+=1
##
##            # Make sure a given pair:
##            #   1. wont be identical (do not grab the same record when reiterating through the csv)
##            #   2. will be either both PAN or both MS
##            #   3. were acquired within appropriate time window
##            #   4. center points were close enough in space
##            #   5. have suitable cloud cover
##            #   optional >>
##            #   6. won't be different 'RC' tiles that for some reason dont get removed with if lines below
##            if matchRC:
##                if image1Name.split('_')[1] != image2Name.split('_')[1]: break
##            if maxDeltaT !=0:
##                if int(t_dif.seconds) <= maxDeltaT: break
##            if image1Name != '' and image2Name != '' and image1Name != image2Name and\
##                image1SB == image2SB and \
##                lat_dif <= maxLatDelta and lon_dif <= maxLonDelta and \
##                image1CC <= maxCloudCov and image2CC <= maxCloudCov and convAng[0]>=minConvAng:
##
##                #print "Found a stereo pair: Time dif= " + str(abs(t_dif.total_seconds()))
##                #print "     Current Image Name: " + line2.split(',')[0]+' '+ curImageName
##                #print "     Paired Image Name: " + line2.split(',')[0]+' '+ matchImageName
##
##                # Write out the csv file
##                csvSTEREOFOOT.write(
##                    image1Name+','+image2Name+','+image1SB.strip('\n')+','+                                                             # Image_1,Image_2,Sensor,
##                    image1Time.strftime("%Y-%m-%d")+','+image2Time.strftime("%Y-%m-%d")+','+                                            # Date_1, Date_2,
##                    image1Time.strftime("%H:%M:%S.%f")+','+image2Time.strftime("%H:%M:%S.%f")+','+                                      # Time_1,Time_2,
##                    str(abs(image1Time.year-image2Time.year))+','+str(abs(image1Time.month-image2Time.month))+','+                      # deltaYear,deltaMonth,
##                    str(abs(image1Time.day-image2Time.day))+','+str(abs(image1Time.hour-image2Time.hour))+','+                          # deltaDay,deltaHour,
##                    str(stereoDict[image1Name][2])+','+str(stereoDict[image2Name][2])+','+                                              # CloudCov_1,CloudCov_2,
##                    str(ovlapCentLat)+','+str(ovlapCentLon)+','+                                                                        # ovlapCentLat,ovlapCentLon
##                    str(ovlapLL[0])+','+str(ovlapLL[1])+','+str(ovlapLR[0])+','+str(ovlapLR[1])+','+                                    # ovlapLLx,ovlapLLy,ovlapLRx,ovlapLRy,
##                    str(ovlapUR[0])+','+str(ovlapUR[1])+','+str(ovlapUL[0])+','+str(ovlapUL[1])+','+                                    # ovlapURx,ovlapURy,ovlapULx,ovlapULy,
##                    str(meanSatEl1)+','+str(meanSatAz1)+','+str(meanSatONVA1)+','+str(ephemX1)+','+str(ephemY1)+','+str(ephemZ1)+','+   # MeanSatEl_1,MeanSatAz_1,MeanONVA_1,ephemX_1,ephemY_1,ephemZ_1,
##                    str(meanSatEl2)+','+str(meanSatAz2)+','+str(meanSatONVA2)+','+str(ephemX2)+','+str(ephemY2)+','+str(ephemZ2)+','+   # MeanSatEl_2,MeanSatAz_2,MeanONVA_2,ephemX_2,ephemY_2,ephemZ_2
##                    str(convAng[0])+','+str(convAng[1])+','+str(convAng[2])+'\n')                                                       # convAngle,asymAngle,bieAngle
##                numPairs+=1
##            image2Idx+=1
##        image1Idx+=1
##
##
##    print "     Total num stereo pairs: " + str(numPairs)
##    csvStereo.close()
##    csvSTEREOFOOT.close()
##
##    """
##    Create a SHP of the STEREO_info.csv (for testing)
##    """
##        ###############################################
##    # Prep to export csv contents to shp
##    csvFile2shp = direct+fileName+outFileNameEnd+'.csv'
##    csvStereoInfo = open(csvFile2shp, 'r')
##    shpOut = shp.Writer(shp.POLYGON)
##
##        ###############################################
##    # Copy csv header into shp header
##
##    # Write shp header
##    # Note: all fields are cast as strings ('C') with length, 80, by default
##    shpHeader = csvStereoInfo.readline()
##    i=0
##    while i < len(shpHeader.split(',')):
##        if i < 3 or i == 5 or i == 6: # image names, sensors
##            shpOut.field(shpHeader.split(',')[i],'C','80')
##        elif i == 3 or i == 4: # date fields
##            shpOut.field(shpHeader.split(',')[i],'D','80')
##        elif i <= 10: #deltaTime fields
##            shpOut.field(shpHeader.split(',')[i],'I','80')
##        elif i > 10: # including cloud cover and following fields
##            shpOut.field(shpHeader.split(',')[i],'F','80')
##        i+=1
##
##    # Create shp features
##    for line in csvStereoInfo.readlines():
##        # Extract corner coords
##        ##print('Line %s' %(line))
##        polyLL = [float(line.split(',')[15]),float(line.split(',')[16])]
##        polyLR = [float(line.split(',')[17]),float(line.split(',')[18])]
##        polyUR = [float(line.split(',')[19]),float(line.split(',')[20])]
##        polyUL = [float(line.split(',')[21]),float(line.split(',')[22])]
##        shpOut.poly(parts=[[polyLL, polyLR, polyUR, polyUL, polyLL]]) # record geometry
##
##        # create shp DBF that parallels csv records
##        lineList = line.split(',')
##        shpOut.record(lineList[0],lineList[1],lineList[2],lineList[3],lineList[4],lineList[5],\
##        lineList[6],lineList[7],lineList[8],lineList[9],lineList[10],\
##        lineList[11],lineList[12],lineList[13],lineList[14],lineList[15],\
##        lineList[16],lineList[17],lineList[18],lineList[19],lineList[20],\
##        lineList[21],lineList[22],lineList[23],lineList[24],lineList[25],\
##        lineList[26],lineList[27],lineList[28],lineList[29],lineList[30],\
##        lineList[31],lineList[32],lineList[33],lineList[34],lineList[35],\
##        lineList[36],lineList[37].strip('\n'))
##
##    shpName = csvFile2shp.strip('.csv')
##    shpOut.save(shpName) #shp name is directory's name
##    csvStereoInfo.close()
##    prj = open(shpName+'.prj', "w")
##    epsg = 'GEOGCS["WGS 84",'
##    epsg += 'DATUM["WGS_1984",'
##    epsg += 'SPHEROID["WGS 84",6378137,298.257223563]]'
##    epsg += ',PRIMEM["Greenwich",0],'
##    epsg += 'UNIT["degree",0.0174532925199433]]'
##    prj.write(epsg)
##    prj.close()
##
##    ###############################################
##
##    print '     pairs with errored convergence angle calc: '+str(int(math.floor(errorConvAng/2)))
##    end_time = time.time()
##    duration = (end_time-start_time)/3600
##    print("     elapsed time was %g seconds" % duration)

if __name__ == "__main__":
    import sys
    stereopairs(str(sys.argv[1]))