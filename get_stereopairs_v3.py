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
##    hdr = "catID,SatEl,SatAz,SunEl,SunAz,CTVA,ONVA,ephemX,ephemY,ephemZ,ullon,ullat,lllon,lllat,urlon,urlat,lrlon,lrlat,centLon,centLat"##+\
##            ##"catID,meanSatEl,meanSatAz,meanSunEl,meanSunAz,ephemX,ephemY,ephemZ,ullon,ullat,lllon,lllat,urlon,urlat,lrlon,lrlat,centLon,centLat"
    hdr =   "left_scene,right_scene,"+\
            "SatEl,SatAz,SunEl,SunAz,CTVA,ONVA,SatEl,SatAz,SunEl,SunAz,CTVA,ONVA,"+\
            "centLon,centLat,centLon,centLat,"+\
            "ephemX,ephemY,ephemZ,ephemX,ephemY,ephemZ,"+\
            "ullon,ullat,lllon,lllat,urlon,urlat,lrlon,lrlat,ullon,ullat,lllon,lllat,urlon,urlat,lrlon,lrlat" + ",ang_conv,ang_bie,ang_asym\n"

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
        csvfile.write(hdr)

        # Get all combos of scenes from each catid strip:
        for leftXML in cat1list:
            for rightXML in cat2list:
                with open(os.path.realpath(os.path.join(imageDir,leftXML)), 'r') as file1, open(os.path.realpath(os.path.join(imageDir,rightXML)),'r') as file2:
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
                    outCSVline = Names + SSGangles + centCoords + ephemeris + cornerCoords + str(stereoAngs[0]) + "," + str(stereoAngs[1]) + "," + str(stereoAngs[2])+'\n'

                    # Write line
                    csvfile.write(outCSVline)
                    ##csvfile.write(outline + str(stereoAngs[0]) + "," + str(stereoAngs[1]) + "," + str(stereoAngs[2]))

                    print("\tOuput CSV file: %s" %(outCSV))
                    print("\tConvergence Angle = " + str(stereoAngs[0]))
                    print("\tBisector Elevation Angle = " + str(stereoAngs[1]))
                    print("\tAsymetry Angle = " + str(stereoAngs[2]))

                    return(str(stereoAngs[0]),str(stereoAngs[1]), str(stereoAngs[2]), hdr, outCSVline)


if __name__ == "__main__":
    import sys
    stereopairs(str(sys.argv[1]))