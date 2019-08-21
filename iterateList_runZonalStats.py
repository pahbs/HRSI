"""
 3/21/2019: copied from iterateDir_runZonalStats
 instead of iterating through a list of directories, this one iterates through a text list
 can combine these later somehow if need be but for now easiest to do them separate

 process is similar except will split the text list like we do with scene lists and others to get them running on multiple nodes
"""

import os, sys

lineS = sys.argv[1] # line start
lineE = sys.argv[2] # enter like 1 20   21 30, etc

# set up some parameters
runScript = '/att/home/mwooten3/code/HRSI/run_GLAS_zonal_database.py'
outdir_base = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal/Stacks_20190815'
outDir = os.path.join(outdir_base, 'outputs')
shpDir = os.path.join(outdir_base, 'shp')
logDir = os.path.join(outdir_base, 'logs')
for d in [outDir, shpDir, logDir]: # for extra measure
    os.system('mkdir -p {}'.format(d))

# where all outputs for the run will go
mainDb = os.path.join(outdir_base, 'Stacks_20190815__zonalStats_15m.csv')

# get list of stacks to run based on input
#inList = '/att/gpfsfs/briskfs01/ppl/wcwagne1/_share/completed_for_maggie.txt'
inList = '/att/gpfsfs/briskfs01/ppl/wcwagne1/_share/maggie_stacks'
with open (inList, 'r') as il:
    inStacks_all = [x.strip('\r\n') for x in il.readlines()] # _all stacks in list

inStacks = inStacks_all[int(lineS)-1:int(lineE)] # subset the scenes we are interested in based on inputs

print "\nProcessing {} stacks...".format(len(inStacks))
# now iterate through stacks and run
c = 0
for stack in inStacks:
    c+=1
    comm = 'python {} {} -shpDir {} -outDir {} -logDir {}'.format(runScript, stack, shpDir, outDir, logDir)
    if mainDb:
        comm += ' -mainDatabasePrefix {}'.format(mainDb)

    print "\n{}/{}:".format(c, len(inStacks))
    print comm
    os.system(comm)

##    # new 3/28/2019: make a copy of the output csv to go back into input dir
##    bname = os.path.basename(os.path.splitext(stack)[0]).replace('_stack', '__stats.csv')
##    outCsv = os.path.join(outDir, bname)
##    toDir = os.path.dirname(stack)
##
##    if os.path.isfile(outCsv):
##        os.system('cp {} {}'.format(outCsv, toDir))
##    else: print '{} DNE'.format(outCsv)

"""
KEEP THIS FOR NOW IN CASE WE HAVE TO COPY STUFF ABOUT SKIPPING STACKS THAT ARE FINISHED
for area in areas:

    runScript = '/att/home/mwooten3/code/HRSI/run_GLAS_zonal_database.py'
    indir = '/att/gpfsfs/briskfs01/ppl/wcwagne1/3DSI/hrsi_chms/{}/'.format(area) #*

    # indir is set up like this: indir/<pairname>/stack.tif'
    area = area.strip('Stacks_') # 11/27 just in case it already has Stacks_ in the name, remove it

    globDir = glob.glob(os.path.join(indir, '*', '*stack.vrt')) # some stack dirs still use tif's not vrt
    if len(globDir) == 0: globDir = glob.glob(os.path.join(indir, '*', '*stack.tif'))

    outdir_base = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal/Stacks_{}'.format(area) #*
    outDir = os.path.join(outdir_base, 'outputs')
    shpDir = os.path.join(outdir_base, 'shp')
    logDir = os.path.join(outdir_base, 'logs')

    # set mainDb = '' if you want to use the default -- 11/27 use separate for now
    mainDb = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal/Stacks_{0}/{0}_Stacks__zonalStats_15m.csv'.format(area)

    for d in [outDir, shpDir, logDir]:
        os.system('mkdir -p {}'.format(d))

    # default buffer size (15m); default zstats
    for pairStack in globDir:

    ##    # temporarily skip list of pairs that were run - stopped after due to system err
    ##    if os.path.basename(pairStack) in skipFiles:
    ##        print "{} already in db. skipping".format(os.path.basename(pairStack))
    ##        continue
        comm = 'python {} {} -shpDir {} -outDir {} -logDir {}'.format(runScript, pairStack, shpDir, outDir, logDir)
    ##    comm = 'python {} {} -shpDir {} -outDir {} -logDir {} -mainDatabasePrefix /att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/3DSI_GLAS_stats_database_15m__p2.csv'.format(runScript, pairStack, shpDir, outDir, logDir) # TEMP
        if mainDb:
            comm += ' -mainDatabasePrefix {}'.format(mainDb)

        print comm
        os.system(comm)

"""