# -*- coding: utf-8 -*-
"""
Modified from 3DSI_createOutGdb.py 10/19/2021

Inputs: Text list with input .gdb/.gpkg, output file name
Outputs: Output .gdb and .txt file with the inputs used

Process:
    for each .gdb in text list: run zs.updateOutputGdb --> outDir/outputGDB
    add .gdb to output text file"""

import os
import glob
import argparse

#import 3DSI_zonalStats as zs #updateOutputGdb
#zs = __import__('3DSI_zonalStats') # the above does not work because it starts w number
from FeatureClass import FeatureClass

# Set up directories - these are kinda hardcoded according to current set-up in other code
inDir = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/ZonalStats/_zonalStatsGdb'
outDir = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/ZonalStats'
    
def main(args):
    
    # Unpack args
    stackType  = args['stackType']
    zonalType  = args['zonalType']

    bname = '{}__{}__ZonalStats'.format(zonalType, stackType)    
    outGdb = os.path.join(outDir, '{}.gdb'.format(bname))
    
    globDir = glob.glob(os.path.join(inDir, '{}*gpkg'.format(bname)))
    print "\nCreating {} from {} input files...\n".format(outGdb, len(globDir))

    for f in globDir:

        fc = FeatureClass(f)
        #zs.updateOutputGdb(outGdb, f)
        fc.addToFeatureClass(outGdb)
        
        
    # Lastly, move the csv to its final directory
    mvCsv = os.path.join(inDir, '{}.csv'.format(bname))
    os.system('mv {} {}'.format(mvCsv, outDir))

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("zonalType", type=str,
                                help="Zonal type (ATL08 or GLAS)")    
    parser.add_argument("stackType", type=str, 
                                help="Stack type (SGM, LVIS, or GLiHT)")


    args = vars(parser.parse_args())

    main(args)
    
