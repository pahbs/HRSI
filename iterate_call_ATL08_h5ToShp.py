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
        utilizing all of the CPUs on nodes by running ATL08_h5ToShp.py
            with GNU parallel
        Also moving logging to h5ToShp.py so overwriting not an issue
"""
import os, sys
#import glob
#import platform
import time

listRange = sys.argv[1]

print "BEGIN: {}".format(time.strftime("%m-%d-%y %I:%M:%S"))
    
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
        
#loopDir = '/att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/data/icesat2/atl08/h5_na/*h5'
print "Processing {} .h5 files in parallel\n".format(len(h5Files))
import pdb; pdb.set_trace()
c = 0
for h5 in h5Files:
    
    c += 1
    print "\nProcessing {} of {}...".format(c, len(h5Files))
    
    call = 'python /home/mwooten3/code/HRSI/ATL08_h5ToShp.py -i {}'.format(h5)
    os.system(call)
    

    
    #o = os.popen(call).read()
print "\nEND: {}\n".format(time.strftime("%m-%d-%y %I:%M:%S"))    