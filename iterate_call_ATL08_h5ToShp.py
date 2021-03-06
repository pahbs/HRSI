# -*- coding: utf-8 -*-
"""
iterate_call_ATL08_h5toShp.py

Created on Wed Mar 18 22:06:12 2020

Ierate through directory and call
ATL08_h5toShp.py

    Added 5/11 - ACTUALLY this is happening in ATL08_h5ToShp.py: 
        Use update GDB function in 3DSI_zonalStats.py to create big GDB
        If GDB does not work, write to GPKG then convert to GDB
    
    NEW 5/15 -
        Adding argparse, and adding option to run in parallel (default False)
        utilizing all of the CPUs on nodes by running ATL08_h5ToShp.py
            with GNU parallel
        Also moving logging to h5ToShp.py so overwriting not an issue
        
        Adding parameter in h5ToShp.py to update an output GDB or not
            If not running in parallel, supply output GDB
            If running in parallel, do not supply output GDB
                Will instead build list of output shapefiles after parallel is 
                done running here and update gdb using list

    NEW 5/27 -
        Adding option to specify na or eu. The former is default, and will be
        used in output naming, and sent to ATL08 h5 to shp code
        
"""
import os, sys
import platform
import time
import argparse
#import glob

from FeatureClass import FeatureClass

# Set variables that should more or less stay the same (do not depend on input):
runScript = '/home/mwooten3/code/HRSI/ATL08_h5ToShp.py'

def main(args):
    
    # Unpack arguments   
    listRange  = args['range']
    runPar = args['parallel']
    cont = args['continent'] # eu or na (default na)

    # Set variables that should more or less stay the same (but depend on input)     
    fileList = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/ATL08/ls_ATL08_{}_v3.txt'.format(cont)
    flightShpDir = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/ATL08/{}/flight_shps'.format(cont)
    outGdb = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/ATL08/{}/ATL08_{}_v3__{}.gdb'.format(cont, cont, platform.node())
    
    print "\nBEGIN: {}".format(time.strftime("%m-%d-%y %I:%M:%S"))
     
    with open (fileList, 'r') as l:
        h5Files = [x.strip('\r\n') for x in l.readlines()]

    # Check inputs: 
    # 1. Check that continent is na or eu
    if cont != 'na' and cont != 'eu':
        raise RuntimeError("Continent must be 'na' or 'eu'")  
    # 2. Check that range is supplied correctly
    try:
        listRange = listRange.split('-')
        S, E = listRange
    except:
        raise RuntimeError("Range must be supplied like: 1-20")
        
    # Get list using range
    h5Files = h5Files[ int(S)-1 : int(E) ]    
    
    # Get list of output shapefiles using input h5 files  
    shps = [os.path.join(flightShpDir, 
            os.path.basename(i).replace('.h5', '.shp')) for i in h5Files]
    
    """ Exploring possible duplicates issue
    import collection
    print [item for item, count in collections.Counter(h5Files).items() if count > 1]
    sys.exit()            
    """
    
    # Run in parallel
    if runPar:    
        # Prepare inputs for parallel call:
        call = "lscpu | awk '/^Socket.s.:/ {sockets=$NF} END {print sockets}'"
        ncpu = os.popen(call).read().strip()
        ncpu = int(ncpu) - 1 # all CPUs minus 1
    
        parList = ' '.join(h5Files)

        print "\nProcessing {} .h5 files in parallel...\n".format(len(h5Files))

        # Do not supply output GDB, do supply continent
        parCall = '{} -i '.format(runScript) + '{1} -continent {2}'
        cmd = "parallel --progress -j {} --delay 1 '{}' ::: {} ::: {}".format(ncpu, parCall, parList, cont)
        os.system(cmd)       

        # And update node-specific GDB 
        print "\n\nCreating {} with completed shapefiles ({})...".format(outGdb, time.strftime("%m-%d-%y %I:%M:%S"))   
        for shp in shps:
            if os.path.isfile(shp):
                fc = FeatureClass(shp)
                fc.addToFeatureClass(outGdb)

    # Do not run in parallel
    else:        
        c = 0
        for h5 in h5Files:
            
            c += 1
            print "\nProcessing {} of {}...".format(c, len(h5Files))
            
            # Call script one at a time and supply node-specific output GDB and continent
            cmd = 'python {} -i {} -gdb {} -continent {}'.format(runScript, h5, outGdb, cont)
            os.system(cmd)
            
            #o = os.popen(call).read()
        
    print "\nEND: {}\n".format(time.strftime("%m-%d-%y %I:%M:%S"))    


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()

    parser.add_argument("-r", "--range", type=str, required=True,
                                help="Range for stack iteration (i.e. 1-20)")
    parser.add_argument("-par", "--parallel", action='store_true', help="Run in parallel")
    parser.add_argument("-continent", "--continent", type=str, required=False,
                                default = 'na', help="Continent (na or eu)")
    
    args = vars(parser.parse_args())

    main(args)