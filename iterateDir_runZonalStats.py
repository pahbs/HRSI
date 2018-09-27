# purpose: to iterate over a given directory and run zonal stats on all stacks
# change variables according to directory setup
import os, sys
import glob

runScript = '/att/home/mwooten3/code/HRSI/run_GLAS_zonal_database.py'
indir = '/att/gpfsfs/briskfs01/ppl/wcwagne1/3DSI/hrsi_chms/Ontario/'
# indir is set up like this: indir/<pairname>/stack.tif'
globDir = os.path.join(indir, '*', '*stack.vrt')

outdir_base = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal/Stacks_Ontario'
outDir = os.path.join(outdir_base, 'outputs')
shpDir = os.path.join(outdir_base, 'shp')
logDir = os.path.join(outdir_base, 'logs')

# set mainDb = '' if you want to use the default
mainDb = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_zonal/Stacks_Ontario/Ontario_Stacks__zonalStats_15m.csv'

for d in [outDir, shpDir, logDir]:
    os.system('mkdir -p {}'.format(d))

# default buffer size (15m); default zstats

for pairStack in glob.glob(globDir):

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
