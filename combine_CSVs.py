# Created: 5/7/2019
# Purpose: To combine 2 or more CSVs together into one output database/csv
#   Will only work if headers are the same
#   Will use the header from the first csv and all others should match

import pandas as pd
import numpy as np
import os, sys

# Inputs:
csvList = ['/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal/Stacks_20190328/outputs/20190328_Stacks__zonalStats_15m.csv',
            '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal/Stacks_20190416/outputs/20190416_Stacks__zonalStats_15m.csv']

outCsv = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal/GLAS_Stacks_zonalStats_15m-2m.csv'
if os.path.isfile(outCsv): # if output file exists, we want to append to it
##    csvList.insert(0, outCsv) # this was causing problems
    #print
    sys.exit("Output CSV {} already exists. Please delete and try again".format(outCsv))

first = True
concatList = []
for c in csvList:
    #import pdb; pdb.set_trace()

    df = pd.read_csv(c)

    if first:
        hdr = list(df.columns)
        firstC = c
        first = False


    # check hdf first
    if hdr == list(df.columns):
        concatList.append(df)
    else:
        print "CSV {} has a different header from {} and cannot be added to the output CSV".format(c, firstC)

#print concatList

outdf = pd.concat(concatList, ignore_index=True).drop_duplicates().reset_index(drop=True)
##print 'ya'
outdf.to_csv(outCsv, index=False)
print "Wrote to {}".format(outCsv)
##    if first:
##        outdf = df
##        hdr = df.columns
##        first = False
##
##    #print df
##    #print dir(df)
##
##    print hdr


