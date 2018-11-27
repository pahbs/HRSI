# one time script to combine results from individual stack output csv's
# Stacks_0713 contains the 'correct' stacks, all but one is a duplicate stack from Stacks_0717
# So we want to combine stacks from 0713, and then any that are in 0717 but not 0713
# Process:
# for csv's in dir:
  # read output csv into list
import os, sys
import glob
from time import strftime
import GLAS_zonalStats_to_database as zsd
import shutil

def add_to_db(outDbCsv, inCsv): # given an input csv we wanna add, add to the output Csv, then write contents to output Shp
    import csv

    # First write the database csv
    if not os.path.isfile(outDbCsv): # if csv does not already exist...
        shutil.copy(inCsv, outDbCsv) # make a copy of the single csv to the output db
    else: # if the csv does exist, add unique lines to it
        with open(outDbCsv, 'r') as odc: existingDb = list(csv.reader(odc)) # read exising db into a list
        with open(inCsv, 'r') as ic: addDb = list(csv.reader(ic)) # read csv to be added into list

##        print existingDb
##        print addDb

##        existingDbSubset = ['{},{}'.format(r[X], r[Y]) for r in existingDb] # where X is the index to get to shotN and Y is index to get to stackName

        for line in addDb: # for each line, if line does not already exist in db, append it to csv
##            print line
##            # HERE DO NOT ADD LINE IF PAIR/SHOTN COMBO ALREADY EXISTS if NOT ADD ALL -- maybe this way
##            # if not addAll:
##            # subset = '{},{}'.format(line[X],line[Y])
####            if subset in existingDbSubset:
####                print '{} already in db'.format(line)
####                continue
##            print 'adding: {}'.format(line)
            if line in existingDb: # this will enable us to skip hdr as well
##                print '{} already in db'.format(line)
                continue
##            print 'adding: {}'.format(line)
            with open(outDbCsv, 'a') as odc: odc.write('{}\n'.format(','.join(line)))

##    # lastly, write the accumulated output csv db to shp
##    if os.path.exists(outDbShp): os.rename(outDbShp, outDbShp.replace('.shp', '__old.shp')) # first rename the existing shp db if it exists
##    zsd.database_to_shp(outDbCsv, '4326') # then create shp, outDbShp will be same name/path as .csv but with .shp extension

# For logging on the fly
start_time = strftime("%Y%m%d-%H%M%S")
lfile = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/combine_0713_0717_stacks_toDb_Log__{}.txt'.format(start_time)

so = se = open(lfile, 'w', 0)                       # open our log file
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0) # re-open stdout without buffering
os.dup2(so.fileno(), sys.stdout.fileno())           # redirect stdout and stderr to the log file opened above
os.dup2(se.fileno(), sys.stderr.fileno())

dir1 = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal/Stacks_20180713/outputs/' # Stacks_20180713
dir2 = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal/Stacks_20180717/outputs'

outDbShp = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal/3DSI_GLAS_stats_database__15m.shp'
outDbCsv = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal/3DSI_GLAS_stats_database__15m.csv'

print "Output Csv: {}".format(outDbCsv)
print "Output Shp: {}\n".format(outDbShp)



# loop through 0713 dir and add all to db
print"Adding stack results from {}:".format(dir1)
addedStacks = [] # list to store stacks when we add them. skip same stacks later
for stackCsv in glob.glob(os.path.join(dir1, '*csv')):

    bname = os.path.basename(stackCsv)
    print '\nStack: {}'.format(bname)

    print " Adding {} to {}".format(bname, outDbCsv)
    add_to_db(outDbCsv, stackCsv)

    addedStacks.append(bname)

print '---------------------------------------------'
# 9/5 remake db without 717 results
print "Adding stack results from {}:".format(dir2)
# loop through 0717 dir and add to db ONLY IF stack has not been added yet
for stackCsv in glob.glob(os.path.join(dir2, '*csv')):
    bname = os.path.basename(stackCsv)
    print '\nStack: {}'.format(bname)

    if bname in addedStacks:
        print " {} already in db".format(bname, outDbCsv)
        continue

    print " Adding {} to {}".format(bname, outDbCsv)
    add_to_db(outDbCsv, stackCsv)

# and convert final csv to db:
print "\nConverting {} to {}".format(outDbCsv, outDbShp)
if os.path.exists(outDbShp): os.rename(outDbShp, outDbShp.replace('.shp', '__old.shp')) # first rename the existing shp db if it exists
zsd.database_to_shp(outDbCsv, '4326') # then create shp, outDbShp will be same name/path as .csv but with .shp extension

print ''
print strftime("%Y%m%d-%H%M%S")

