# -*- coding: utf-8 -*-
"""
iterate_call_ATL08_h5toShp.py

Created on Wed Mar 18 22:06:12 2020

Ierate through directory and call
ATL08_h5toShp.py
"""
import os
import glob

loopDir = '/att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/data/icesat2/atl08/h5_na'

for h5 in glob.glob(os.path.join(loopDir, '*h5')):
    
    print "\nProcessing {}...".format(h5)
    call = 'python ATL08_h5ToShp.py -i {}'.format(h5)
    os.system(call)
    
