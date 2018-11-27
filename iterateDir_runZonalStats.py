# purpose: to iterate over a given directory and run zonal stats on all stacks
# change variables according to directory setup
import os, sys
import glob

# 11/27/2018: Adding loop to run stacks for multiple areas. write to separate csv for each area

areas_str = raw_input("Enter stack names (i.e. 'name1,name2,name3'): ")
areas = areas_str.split(",")

for area in areas:

    runScript = '/att/home/mwooten3/code/HRSI/run_GLAS_zonal_database.py'
    indir = '/att/gpfsfs/briskfs01/ppl/wcwagne1/3DSI/hrsi_chms/{}/'.format(area) #*
    print indir
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
