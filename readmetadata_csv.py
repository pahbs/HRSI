#! usr/bin/python
'''
Created by: Stephanie Miller
2017-02-09
Usage:  

Open shapefile in GIS-compatible system (e.g. QGIS) and save as csv format
The createjson function converts specific fields in the CSV file into JSON format
For easier access/later processing.
'''

import os, csv, json

def createjson(filename):
    '''
    The metadata we need for SCS correction are: solar azimuth and solar zenith.
    We also need the pairname.  
    Solar azimuth = SunAz (left image)
    Solar zenith is complement of solar elevation angle, so 90 - SunEl (left image)
    (or cosine of either one of them equals the sine of the other)
    '''
    with open(filename, 'r') as fhand:
        header = fhand.readline().strip().split(',')
        metadatalist = []
        for row in fhand:
            metadatalist.append(row.strip().split(','))

    pairnames = [i[1] for i in metadatalist]  #could use to enumerate over this later; leaving in for now

    metadatakeys = {}
    acqdict = {}

    for i, j in enumerate(header):
        for k in ['pairname', 'SunEl', 'SunAz']:
            #In case the attributes are in different columns, find the index # for these fields
            # http://stackoverflow.com/questions/176918/finding-the-index-of-an-item-given-a-list-containing-it-in-python
            if j == k:
                metadatakeys[k] = i  

    for row in metadatalist:
        pair = row[metadatakeys['pairname']]
        sunaz = int(row[metadatakeys['SunAz']])
        sunel = int(row[metadatakeys['SunEl']])
        sunzen = 90 - sunel
        acqdict[pair] = (sunaz, sunel, sunzen)  # I may remove one of the complementary angles once confirm this works
        #may want to add additional fields or include field names as subkeys

    fout = 'metadata_' + filename.split('.')[0] + '.txt'
    with open(fout, 'w') as outfile:
         json.dump(acqdict, outfile, sort_keys = True, indent = 4, ensure_ascii=False)
         #http://stackoverflow.com/questions/12309269/how-do-i-write-json-data-to-a-file-in-python

    print('***** JSON file created for basic metadata')


# fname = "outASP_DSM_foot_20170130.csv"
# createjson(fname)


# with open('metadata.txt', 'r') as infile: 
#     d = json.load(infile)

#     print d


