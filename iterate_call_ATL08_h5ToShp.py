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

        
        
"""
import os, sys
#import glob
#import platform
import time
import argparse

listRange = sys.argv[1]

def main(args):
    
    # Unpack arguments   
    listRange  = args['range']
    runPar = args['parallel']
    
    print "BEGIN: {}".format(time.strftime("%m-%d-%y %I:%M:%S"))

    runScript = '/home/mwooten3/code/HRSI/ATL08_h5ToShp.py'       
    fileList = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/ATL08/ls_ATL08_na_v3.txt'
    
    with open (fileList, 'r') as l:
        h5Files = [x.strip('\r\n') for x in l.readlines()]
    
    # Check that range is supplied correctly
    try:
        listRange = listRange.split('-')
        S, E = listRange
    except:
        raise RuntimeError("Range must be supplied like: 1-20")
        
    # Get list using range
    h5Files = h5Files[ int(S)-1 : int(E) ]    
    
    """ Exploring duplicates thing"
    import collections
    print [item for item, count in collections.Counter(h5Files).items() if count > 1]
    sys.exit()            
    """
    import pdb; pdb.set_trace()
    
    # Run in parallel
    if runPar:    
        # Prepare inputs for parallel call:
        call = "lscpu | awk '/^Socket.s.:/ {sockets=$NF} END {print sockets}'"
        ncpu = os.popen(call).read().strip()
    
        parList = ' '.join(h5Files)

        print "Processing {} .h5 files in parallel\n".format(len(h5Files))

        parCall = '{} -i '.format(runScript) + '{1}'
        cmd = "parallel --progress -j {} --delay 1 '{}' ::: {}".format(ncpu, parCall, parList)
    
        os.system(cmd)

    # Do not run in parallel
    else:        
        c = 0
        for h5 in h5Files:
            
            c += 1
            print "\nProcessing {} of {}...".format(c, len(h5Files))
            
            call = 'python {} -i {}'.format(runScript, h5)
            os.system(call)
            
            #o = os.popen(call).read()
        
    print "\nEND: {}\n".format(time.strftime("%m-%d-%y %I:%M:%S"))    


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()

    parser.add_argument("-r", "--range", type=str, required=True,
                                help="Range for stack iteration (i.e. 1-20)")
    parser.add_argument("-par", "--parallel", action='store_true', help="Run in parallel")

    args = vars(parser.parse_args())

    main(args)