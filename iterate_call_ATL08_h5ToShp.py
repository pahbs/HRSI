# -*- coding: utf-8 -*-
"""
iterate_call_ATL08_h5toShp.py

Created on Wed Mar 18 22:06:12 2020

Ierate through directory and call
ATL08_h5toShp.py

    Added 5/11 - ACTUALLY this is happening in ATL08_h5ToShp.py: 
        Use update GDB function in 3DSI_zonalStats.py to create big GDB
        If GDB does not work, write to GPKG then convert to GDB
        
"""
import os, sys
#import glob
#import platform

fileList = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/ATL08/ls_ATL08_na_v3.txt'
logFile = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/ATL08/ATL08toGdb_v3__Log.txt'
listRange = sys.argv[1]

# Log output
print "See {} for log".format(logFile)
so = se = open(logFile, 'a', 0) # open our log file
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0) # re-open stdout without buffering
os.dup2(so.fileno(), sys.stdout.fileno()) # redirect stdout and stderr to the log file opened above
os.dup2(se.fileno(), sys.stderr.fileno())

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
            
            
#loopDir = '/att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/data/icesat2/atl08/h5_na/*h5'
print "Processing {} .h5 files".format(len(h5Files))

for h5 in h5Files:

    print "\nProcessing {}...".format(h5)
    call = 'python /home/mwooten3/code/HRSI/ATL08_h5ToShp.py -i {}'.format(h5)
    os.system(call)
    
    #o = os.popen(call).read()
    
