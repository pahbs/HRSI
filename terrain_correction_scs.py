"""
Script created based on Javascript / Google Earth Engine code provided by Jamon 

"""

import math

def SCSTC(img):
    #SLOPE, ASPECT  (radians)
    #from DEM file?
    SLP = 1; #placeholder
    ASP = 1; #placeholder

    #Get Solar Azimuth Angle and Solar Zenith Angle from IMAGE METADATA
    az = 149 #placeholder; solar azimuth angle
    ze = 52 #placeholder; solar zenith angle
    #Convert to radians
    az_r = math.radians(az)
    ze_r = math.radians(ze)
    print(az_r, ze_r)

    #CALCULATE LOCAL ILLUMINATION, COS OF THE ZENITH ANGLE, AND COS OF SLOPE
    #var IL = AZ_R.subtract(ASP).cos().multiply(SLP.sin()).multiply(ZE_R.sin()).add(ZE_R.cos().multiply(SLP.cos()));
    #deconstructing this:
    #start with the solar azimuth angle in radians.
    #subtract the aspect
    #calculate the cosine of this result.
    #multiply that by the sine of the slope
    #then multiply by the sine of the solar zenith angle, in radians
    #add the cosine of the solar zenith agnel, in radians
    #finally, multiply by the cosine of the slope

    #in Python:

    IL = math.cos(SLP) * (   (math.cos(az_r - ASP)) * math.sin(SLP) * math.sin(ze_r) + math.cos(ze_r)   )

    #var cos_ZE_SLP = (ZE_R.cos()).multiply((SLP).cos());
    #calculate the cosine of the zenith angle, multiply by the cosine of the slope
    #in python:

    cos_SZ_SLP = math.cos(ze_r) * math.cos(SLP)

    # SUN-CANOPY-SENSOR CORRECTION
    # var img_TC = img.multiply(0.0001).multiply(cos_ZE_SLP).divide(IL);   
    img_TC = img * (0.0001) * cos_ZE_SLP / IL  #this is probably for each element in the array if applied via numpy

    # ADJUST VALUE RANGE (REFLECTANCE BETWEEN 0 AND 1)
    # var img_TC1 = img_TC.where(img_TC.lte(0),0);
    # if the corrected image is less than or equal to 0, just assign that location 0
    # var img_TC2 = img_TC1.where(img_TC1.gte(1),1);
    # if the corrected image is greater than or equal to 1, just assign that location 1
    # ask why!

    #write out the resulting image


SCSTC("test")


