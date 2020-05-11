# -*- coding: utf-8 -*-
"""
iterate_call_ATL08_h5toShp.py

Created on Wed Mar 18 22:06:12 2020

Ierate through directory and call
ATL08_h5toShp.py

    Added 5/11: 
        Use update GDB function in 3DSI_zonalStats.py to create big GDB
        If GDB does not work, write to GPKG then convert to GDB
        
"""
import os
import glob

loopDir = '/att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/data/icesat2/atl08/h5_na/*h5'

for h5 in glob.glob(loopDir):

    print "\nProcessing {}...".format(h5)
    call = 'python /home/mwooten3/code/HRSI/ATL08_h5ToShp.py -i {}'.format(h5)
    os.system(call)
    
    #o = os.popen(call).read()
    
