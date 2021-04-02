"""
Created 4/15/2020

Iterate through a list of stacks for a given stack/zone type combo,
     and run Zonal Stats

Inputs: 
    stackType [SGM, LVIS, or GLiHT for now]
    zoneType  [ATL08?, GLAS for now]
    
6/5 
    Adding parameter in 3DSI_zonalStats.py to update an output GDB or not
    If not running in parallel, supply output GDB/GPKG
    If running in parallel, do not supply output GDB
        Will instead build list of output shapefiles after parallel is 
        done running here and update gdb using list
"""

import os
import argparse
import platform
import time

from RasterStack import RasterStack
from FeatureClass import FeatureClass

overwrite = False

mainDir = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/ZonalStats'
runScript = '/home/mwooten3/code/HRSI/3DSI_zonalStats.py'

validStackTypes = ['SGM', 'LVIS', 'GLiHT', 'Landsat', 'Tandemx']
validZonalTypes = ['ATL08', 'GLAS']

""" Dirs as of 10/13/2020
    SGM: /att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/projects/3dsi/stacks/Out_SGM/
    LVIS: /att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/projects/3dsi/stacks/out_lvis/ # these were updated to 15m stacks but location is same
    GLiHT: /att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/projects/3dsi/stacks/out_gliht/
    Landsat: /att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/data/standage/boreal_na/tile_stacks
    Tandemx: /att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/Tandemx_stacks
"""

def getVarsDict(stackType, zonalType):

    inputList = os.path.join(mainDir, '_lists', 'ls_{}.txt'.format(stackType))
    
    """
    if stackType == 'SGM':
        inputList = os.path.join(mainDir, '_lists', 'ls_SGM.txt')
    elif stackType == 'LVIS':
        inputList = os.path.join(mainDir, '_lists', 'ls_LVIS.txt')
    elif stackType == 'GLiHT':
        inputList = os.path.join(mainDir, '_lists', 'ls_GLiHT.txt')
    """
    
    if zonalType == 'ATL08':
        #inputZonal = os.path.join(mainDir, '_zonalGdb', 'ATL08_na.gdb')
        inputZonal = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/ATL08_na_v003.gdb'
    elif zonalType == 'GLAS':
        inputZonal = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/GLAS_naBoreal.gdb'        
        
    outCsv = os.path.join(mainDir, '_zonalStatsGdb',
                         '{}__{}__ZonalStats.csv'.format(zonalType, stackType))
    
    varsDict = {'inList': inputList, 'inZonal': inputZonal, 'outCsv': outCsv}
    
    return varsDict

def getStackList(inList, stackRange):

    with open (inList, 'r') as l:
        stacks = [x.strip('\r\n') for x in l.readlines()]

    # TO-DO try statement here and reject stackRange input
    if stackRange: 
        S, E = stackRange
        stackList = stacks[ int(S)-1 : int(E) ]
        
    else: # If range is None, run all stacks
        stackList = stacks
        
    return stackList

# Unpack and validate input arguments
def unpackValidateArgs(args):
    
    # Unpack args
    stackType  = args['stackType']
    zonalType  = args['zonalType']
    stackRange = args['range']
    runPar     = args['parallel']

    # Validate inputs   
    if stackType not in validStackTypes:
        err = "Stack type must be one of: " + \
                        "{}".format(", ".join(validStackTypes).strip(', ')) 
        raise RuntimeError(err)
    
    if zonalType not in validZonalTypes:        
        err = "Zonal type must be one of: " + \
                        "{}".format(", ".join(validZonalTypes).strip(', ')) 
        raise RuntimeError(err)
        
    if stackRange:        
        try:
            stackRange = stackRange.split('-')
            S, E = stackRange
        except:
            raise RuntimeError("Range must be supplied like: 1-20")
            
    return stackType, zonalType, stackRange, runPar

def main(args):

    # Unpack and validate arguments
    stackType, zonalType, stackRange, runPar = unpackValidateArgs(args)
    
    # Get varsDict --> {inList; inZonal; outCsv}
    varsDict = getVarsDict(stackType, zonalType) 
    
    # Get list of stacks to iterate
    stackList = getStackList(varsDict['inList'], stackRange)
    
    # Get node-specific output .gdb
    outGdb = varsDict['outCsv'].replace('.csv', '-{}.gpkg'.format(platform.node()))

    if runPar: # If running in parallel
        
        # Get list of output shp's that we are expecting based off stackList
        shps = [os.path.join(mainDir, zonalType, stackType, RasterStack(stack).stackName, 
                '{}__{}__zonalStats.shp'.format(zonalType, RasterStack(stack).stackName)) 
                                                        for stack in stackList]

        # Prepare inputs for parallel call:
        call = "lscpu | awk '/^Socket.s.:/ {sockets=$NF} END {print sockets}'"
        ncpu = os.popen(call).read().strip()
        ncpu = int(ncpu) - 1 # all CPUs minus 1
    
        parList = ' '.join(stackList)
        
        print "\nProcessing {} stack files in parallel...\n".format(len(stackList))

        # Do not supply output GDB, just supply .csv
        parCall = '{} -rs '.format(runScript) + '{1} -z {2} -o {3} -log'
        cmd = "parallel --progress -j {} --delay 1 '{}' ::: {} ::: {} ::: {}". \
                format(ncpu, parCall, parList, varsDict['inZonal'], 
                                                           varsDict['outCsv'])

        os.system(cmd)       

        # And update node-specific GDB if shp exists
        print "\n\nCreating {} with completed shapefiles ({})...".format(outGdb, 
                                            time.strftime("%m-%d-%y %I:%M:%S"))   
        for shp in shps:
            if os.path.isfile(shp):
                fc = FeatureClass(shp)
                if fc.nFeatures > 0: fc.addToFeatureClass(outGdb)        
                
    # Do not run in parallel
    else:   
        
        # Iterate through stacks and call
        print "\nProcessing {} stacks...".format(len(stackList))
        
        c = 0
        for stack in stackList:
            
            c+=1
            print "\n{}/{}:".format(c, len(stackList))
            
            # Check stack's output csv's, and skip if it exists and overwrite is False
            rs = RasterStack(stack)
            check = os.path.join(mainDir, zonalType, stackType, rs.stackName,
                        '{}__{}__zonalStats.csv'.format(zonalType, rs.stackName))
            
            if not overwrite:
                if os.path.isfile(check):
                    print "\nOutputs for {} already exist\n".format(rs.stackName)
                    continue
            
            # Not running in parallel, send the node-specific ouput .gdb and both should get written
            cmd = 'python {} -rs {} -z {} -o {} -log'.format(runScript, stack,  \
                                          varsDict['inZonal'], outGdb)        
            print cmd
            os.system(cmd) 
        
        
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("zonalType", type=str,
                                help="Zonal type (ATL08 or GLAS)")    
    parser.add_argument("stackType", type=str, 
                                help="Stack type (SGM, LVIS, GLiHT, Landsat, Tandemx)")
    parser.add_argument("-r", "--range", type=str,
                                help="Range for stack iteration (i.e. 1-20)")
    parser.add_argument("-par", "--parallel", action='store_true', help="Run in parallel")
    
    args = vars(parser.parse_args())

    main(args)
