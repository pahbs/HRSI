# purpose: to iterate over a given directory and run zonal stats on all stacks
# change variables according to directory setup
import os, sys
import glob

runScript = '/att/home/mwooten3/code/HRSI/run_GLAS_zonal_database.py'
#indir = '/att/gpfsfs/briskfs01/ppl/wcwagne1/3DSI/hrsi_chms/Stacks_20180717/'
indir = '/att/gpfsfs/briskfs01/ppl/wcwagne1/3DSI/hrsi_chms/Stacks_20180713/' #TEMP
# indir is set up like this: indir/<pairname>/stack.tif'
globDir = os.path.join(indir, '*', '*stack.tif')

#outdir_base = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/Stacks_20180717'
outdir_base = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/Stacks_20180713' # TEMP
outDir = os.path.join(outdir_base, 'outputs')
shpDir = os.path.join(outdir_base, 'shp')
logDir = os.path.join(outdir_base, 'logs')

for d in [outDir, shpDir, logDir]:
    os.system('mkdir -p {}'.format(d))

# default buffer size (15m); default zstats

skipFiles = ['WV03_20150802_104001000F59BD00_104001000F4EA200_stack.tif', 'WV01_20150801_1020010042405000_1020010042EAB400_stack.tif', 'WV02_20150722_1030010044800200_10300100457E6900_stack.tif',
'WV03_20150615_104001000D22A300_104001000D1DE500_stack.tif', 'WV02_20160419_1030010054423000_1030010055975200_stack.tif', 'WV01_20160708_10200100516C7900_102001005082D500_stack.tif', 'WV01_20150701_102001004115D400_102001003E51E000_stack.tif',
'WV01_20140821_10200100320F4B00_1020010031CCA900_stack.tif', 'WV02_20140717_103001003597FC00_1030010034891C00_stack.tif', 'WV01_20110804_1020010015973400_10200100152A5F00_stack.tif', 'WV01_20140923_1020010035E05F00_10200100324D0700_stack.tif'] # temporary


for pairStack in glob.glob(globDir):

    # temporarily skip list of pairs that were run - stopped after due to system err
    if os.path.basename(pairStack) in skipFiles:
        print "{} already in db. skipping".format(os.path.basename(pairStack))
        continue
    #comm = 'python {} {} -shpDir {} -outDir {} -logDir {}'.format(runScript, pairStack, shpDir, outDir, logDir)
    comm = 'python {} {} -shpDir {} -outDir {} -logDir {} -mainDatabasePrefix /att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/3DSI_GLAS_stats_database_15m__p2.csv'.format(runScript, pairStack, shpDir, outDir, logDir) # TEMP
    print comm
    os.system(comm)