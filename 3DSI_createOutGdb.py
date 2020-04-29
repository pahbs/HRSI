# -*- coding: utf-8 -*-
"""
Created on Thu Apr 23 15:44:57 2020; @author: mwooten3

For temporary fix to OGR locking issue

To merge the node-specific GPKG's to one GDB

Inputs: stackType, zonalType

Process:
    for each gumbyX.gpkg: run zs.updateOutputGdb --> outDir/outputGDB
    move the csv to outDir
"""

import os
import glob
import argparse

#import 3DSI_zonalStats as zs #updateOutputGdb
zs = __import__('3DSI_zonalStats') # the above does not work because it starts w number

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
    import pdb; pdb.set_trace()
    for f in globDir:
        print f
        zs.updateOutputGdb(outGdb, f, outDrv = 'GPKG')#outDrv = "FileGDB")
        
        
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
    