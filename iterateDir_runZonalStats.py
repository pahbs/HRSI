# purpose: to iterate over a given directory and run zonal stats on all stacks
# change variables according to directory setup
import os, sys
import glob

runScript = '/att/home/mwooten3/code/HRSI/run_GLAS_zonal_database.py'
indir = '/att/gpfsfs/briskfs01/ppl/wcwagne1/3DSI/hrsi_chms/Stacks_20180717/'
# indir is set up like this: indir/<pairname>/stack.tif'
globDir = os.path.join(indir, '*', '*stack.tif')

outdir = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/Stacks_20180717'
csvDir = os.path.join(outdir, 'csv')
shpDir = os.path.join(outdir, 'shp')
logDir = os.path.join(outdir, 'logs')

for d in [csvDir, shpDir, logDir]:
    os.system('mkdir -p {}'.format(d))

# default buffer size (15m); default zstats

for pairStack in glob.glob(globDir):
    comm = 'python {} {} -shpDir {} -outDir {} -logDir {}'.format(runScript, pairStack, shpDir, csvDir, logDir)
    print comm
    os.system(comm)