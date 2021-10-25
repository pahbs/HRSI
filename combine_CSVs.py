# Modified 10/19/2021

#Inputs: Text list with inputs, output file name
#Outputs: Output .gdb and .txt file with the inputs used

# New process:
#   For each .csv in input list:
#       Add contents to output .csv
#       Add filename to output .txt file

# Created: 5/7/2019
# Purpose: To combine 2 or more CSVs together into one output database/csv
#   Will only work if headers are the same
#   Will use the header from the first csv and all others should match

import pandas as pd
import os, sys
import glob

# Inputs:
#inDir =  '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/ZonalStats/ATL08/Landsat/h*'
#outCsv = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/ZonalStats/ATL08__Landsat__ZonalStats__rewrite.csv'
inTxt = sys.argv[1]
outCsv = sys.argv[2]
outTxt = outCsv.replace('.csv', '__inputFiles.txt')

if not inTxt.endswith('.txt'):
    sys.exit("First argument must be input .txt file. Try again")
if not outCsv.endswith('.csv'):
    sys.exit("Second argument must be output .csv file. Try again")    
if os.path.isfile(outCsv): # if output file exists, we want to append to it
##    csvList.insert(0, outCsv) # this was causing problems
    #print
    sys.exit("Output CSV {} already exists. Please delete and try again".format(outCsv))
    
with open(inTxt, 'r') as it:
    csvList = [f.strip() for f in it.readlines()]
   
print "Combining {} .csv files...\n".format(len(csvList))

first = True
concatList = []
cnt = 0
for c in csvList:
    #import pdb; pdb.set_trace()

    if not os.path.isfile(c):
        print "{} does not exist. Skipping".format(c)
        continue
    
    cnt += 1
    print cnt

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
        continue

    # If file exists and is being added to output .csv, write to out txt:
    with open(outTxt, 'a') as ot:
        ot.write('{}\n'.format(c))

#print concatList

outdf = pd.concat(concatList, ignore_index=True).drop_duplicates().reset_index(drop=True)
print "\n{} rows in final output df".format(len(outdf))

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


