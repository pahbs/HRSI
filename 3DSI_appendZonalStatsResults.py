"""
Created 4/15/2020

Iterate through a list of output zonal stats for a given stack/zone type combo,
     and append zonal stats results to .csv and .gdb

Inputs: 
    stackType [SGM, LVIS, or GLiHT for now]
    zoneType  [ATL08, GLAS]
    
Process:
    A) Append zonalStats.csv to .csv
    B) Get list of zonalStats.shp for stack/zone combo;
	 Write those without 0 features to .gdb
     
      If running with a range (i.e. running by distributing among nodes),
        write to node-specific .gpkg.
      If running with no range (only on one node), write to main output .GDB
      
    REMOVING parallel functionality because:
      We still would not be able to write to one .gdb in parallel
      And writing to the big .csv is risky, more so here where there is much 
      less to do vs in ZS, writes were more spaced out
        
        
"""

import os
import argparse
import platform
#import time
import pandas as pd

#from RasterStack import RasterStack
from FeatureClass import FeatureClass

overwrite = False

mainDir = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/ZonalStats'
#runScript = '/home/mwooten3/code/HRSI/3DSI_zonalStats.py'

validStackTypes = ['SGM', 'LVIS', 'GLiHT']
validZonalTypes = ['ATL08', 'GLAS']


def getVarsDict(stackType, zonalType):

    inputList = os.path.join(mainDir, '_lists', 'ls-zsOut_{}.txt'.format(stackType))
#    
#    """
#    if stackType == 'SGM':
#        inputList = os.path.join(mainDir, '_lists', 'ls_SGM.txt')
#    elif stackType == 'LVIS':
#        inputList = os.path.join(mainDir, '_lists', 'ls_LVIS.txt')
#    elif stackType == 'GLiHT':
#        inputList = os.path.join(mainDir, '_lists', 'ls_GLiHT.txt')
#    """
    # if inputList does not exist, make your own
    if not os.path.isfile(inputList):
        searchDir = os.path.join(mainDir, '{}/{}/*/*zonalStats.shp'.format(zonalType, stackType))
        os.system('ls {} > {}'.format(searchDir, inputList))
       
    # For now right to main ZS dir
    outCsv = os.path.join(mainDir,
            '{}__{}__ZonalStats.csv'.format(zonalType, stackType))
    
    varsDict = {'inList': inputList, 'outCsv': outCsv}
    
    return varsDict

def getShpList(inList, stackRange):
    
    # Get List of shapefiles from text list [ls *zonalStats.shp > list.txt]

    with open (inList, 'r') as l:
        shps = [x.strip('\r\n') for x in l.readlines()]

    # TO-DO try statement here and reject stackRange input
    if stackRange: 
        S, E = stackRange
        shpList = shps[ int(S)-1 : int(E) ]
        
    else: # If range is None, run all stacks
        shpList = shps
        
    return shpList

# Unpack and validate input arguments
def unpackValidateArgs(args):
    
    # Unpack args
    stackType  = args['stackType']
    zonalType  = args['zonalType']
    stackRange = args['range']
    #runPar     = args['parallel']

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
            
    return stackType, zonalType, stackRange#, runPar

def updateOutputCsv(outCsv, df):
    # Append a dataframe to an output CSV - assumes columns are the same

    #print "\nUpdating the big output csv {}".format(outCsv)
    
    hdr = False # Only add the header if the file does not exist
    if not os.path.isfile(outCsv):
        hdr = True
    
    df.to_csv(outCsv, sep=',', mode='a', index=False, header=hdr)
    
    return None

def main(args):

    # Unpack and validate arguments
    stackType, zonalType, stackRange = unpackValidateArgs(args)
    
    # Get varsDict --> {inList; inZonal; outCsv}
    varsDict = getVarsDict(stackType, zonalType) 
    
    # Get list of zonalStats.shp to iterate
    shpList = getShpList(varsDict['inList'], stackRange)
    
    # If running distributively (i.e. with a range): Get node-specific output .gdb/.gpkg
    if stackRange:
        outGdb = varsDict['outCsv'].replace('.csv', '-{}.gpkg'.format(platform.node()))
    # If running on one node, write to one .gdb
    else:
        outGdb = varsDict['outCsv'].replace('.csv', '.gdb')

    # To record feature count
    featureCount = os.path.join(mainDir, '_timing', 
                '{}_{}__appendFeatureCount.csv'.format(zonalType, stackType))
    if not os.path.isfile(featureCount):
        with open(featureCount, 'w') as bc:
            bc.write('zonalStatShp,basename,nFeatures\n')

    # PART A: Iterate through csv's and write to big output .csv
    print "\nProcessing {} zonalStats outputs to write .csv {}...". \
                            format(len(shpList), varsDict['outCsv'])
    #c = 0
    for inShp in shpList:
        
        #c+=1
        #print "\n{}/{}:".format(c, len(shpList))
        
        # A: Update to large output .csv if one exists:
        inCsv = inShp.replace('.shp', '.csv')
        
        if not os.path.isfile(inCsv): continue # Skip if no .csv (ie zero features in shp)
    
        df = pd.DataFrame.from_csv(inCsv)
        updateOutputCsv(varsDict['outCsv'], df)
        
    print "Finished writing .csv\n========================================"
    
    """    
    # PART B: Iterate through shp's and write to big output gdb/.gpkg
    print "\nProcessing {} zonalStats outputs to write .gdb {}...". \
                                        format(len(shpList), outGdb)
    c = 0
    for inShp in shpList:
        
        c+=1
        print "\n{}/{}:".format(c, len(shpList))
              
        # B. Append to large output .gdb
        zs = FeatureClass(inShp)
        nFeatures = zs.nFeatures
        
        if nFeatures > 0: # Only bother if shp has features
            zs.addToFeatureClass(outGdb)
        
        # C. Write number of features for shapefile zone / 
        #       stackName combo to csv since timing failed
        bname = os.path.basename(inShp).strip('__zonalStats.shp')
        with open(featureCount, 'a') as bc:
            bc.write('{},{},{}\n'.format(inShp, bname, nFeatures))
    """



    # removing parallel functionality here
    """
    if runPar: # If running in parallel
        
        # we already have the list of shp now that 
        # Get list of output stacks that we are expecting based off stackList
        shps = [os.path.join(mainDir, zonalType, stackType, RasterStack(stack).stackName, 
                '{}__{}__zonalStats.shp'.format(zonalType, RasterStack(stack).stackName)) for stack in stackList]

        # Prepare inputs for parallel call:
        call = "lscpu | awk '/^Socket.s.:/ {sockets=$NF} END {print sockets}'"
        ncpu = os.popen(call).read().strip()
        ncpu = int(ncpu) - 1 # all CPUs minus 1
    
        parList = ' '.join(stackList)
        
        print "\nProcessing {} stack files in parallel...\n".format(len(stackList))

        # Do not supply output GDB, just supply .csv
        parCall = '{} -rs '.format(runScript) + '{1} -z {2} -o {3} -log'
        cmd = "parallel --progress -j {} --delay 1 '{}' ::: {} ::: {} ::: {}". \
                format(ncpu, parCall, parList, varsDict['inZonal'], varsDict['outCsv'])

        os.system(cmd)       

        # And update node-specific GDB 
        print "\n\nCreating {} with completed shapefiles ({})...".format(outGdb, time.strftime("%m-%d-%y %I:%M:%S"))   
        for shp in shps:
            if os.path.isfile(shp):
                fc = FeatureClass(shp)
                fc.addToFeatureClass(outGdb)        
                
    # Do not run in parallel
    else:   
       
        # Iterate through stacks and call
        print "\nProcessing {} stacks...".format(len(stackList))
        
        c = 0
        for stack in stackList:
            
            c+=1
            print "\n{}/{}:".format(c, len(stackList))
            
            # Check stack's outputs, and skip if it exists and overwrite is False
            rs = RasterStack(stack)
            check = os.path.join(mainDir, zonalType, stackType, rs.stackName,
                        '{}__{}__zonalStats.shp'.format(zonalType, rs.stackName))
            
            if not overwrite:
                if os.path.isfile(check):
                    print "\nOutputs for {} already exist\n".format(rs.stackName)
                    continue
            
            # Not running in parallel, send the node-specific ouput .gdb and both should get written
            cmd = 'python {} -rs {} -z {} -o {} -log'.format(runScript, stack,  \
                                          varsDict['inZonal'], outGdb)        
            print cmd
            os.system(cmd) 
    """         
        
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("zonalType", type=str,
                                help="Zonal type (ATL08 or GLAS)")    
    parser.add_argument("stackType", type=str, 
                                help="Stack type (SGM, LVIS, or GLiHT)")
    parser.add_argument("-r", "--range", type=str,
                                help="Range for stack iteration (i.e. 1-20)")
    #parser.add_argument("-par", "--parallel", action='store_true', help="Run in parallel")
    
    args = vars(parser.parse_args())

    main(args)
