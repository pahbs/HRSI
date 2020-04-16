"""
Created 4/15/2020

Iterate through a list of stacks for a given stack/zone type combo,
     and run Zonal Stats

Inputs: 
    stackType [SGM, LVIS, or GLiHT for now]
    zoneType  [ATL08?, GLAS for now]
"""

import os
import argparse

overwrite = False

mainDir = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/ZonalStats'
runScript = '/att/home/mwooten3/code/HRSI/3DSI_zonalStats.py'

validStackTypes = ['SGM', 'LVIS', 'GLiHT']
validZonalTypes = ['ATL08', 'GLAS']

""" Dirs as of 4/15/2020
    DSM: /att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/projects/3dsi/stacks/Out_SGM/
    LVIS: /att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/projects/3dsi/stacks/out_lvis/
    GLiHT: /att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/projects/3dsi/stacks/out_gliht/
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
        inputZonal = os.path.join(mainDir, '_zonalGdb', 'ATL08_na.gdb')
    elif zonalType == 'GLAS':
        inputZonal = '' # ?????        
        
    outCsv = os.path.join(mainDir, '{}__{}__ZonalStats.csv'.format(zonalType, stackType))
    
    varsDict = {'inList': inputList, 'inZonal': inputZonal, 'outCsv': outCsv}
    
    return varsDict

def getStackList(inList, stackRange):

    with open (inList, 'r') as l:
        stacks = [x.strip('\r\n') for x in l.readlines()]

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
        except:
            raise RuntimeError("Range must be supplied like: 1-20")
            
    return stackType, zonalType, stackRange

def main(args):
    import pdb; pdb.set_trace()
    # Unpack and validate arguments
    stackType, zonalType, stackRange = unpackValidateArgs(args)
    
    # Get varsDict --> {inList; inZonal; outCsv}
    varsDict = getVarsDict(stackType, zonalType) 
    
    # Get list of stacks to iterate
    stackList = getStackList(varsDict['inList'], stackRange)

    # Iterate through stacks and call
    print "\nProcessing {} stacks...".format(len(stackList))
    
    c = 0
    for stack in stackList:
        
        c+=1
        print "\n{}/{}:".format(c, len(stackList))
        
        # COME BACK TO THIS - can we easily forumlate the output .shx or .csv from RasterStack class here?
        """
        bname = os.path.basename(stack).strip(os.path.splitext(stack)[1]).strip('_stack')
        # skip files whose .shx file (last file to be created) already exists
        if not overwrite:
            if os.path.isfile(os.path.join(outDir, '{}__stats.shx'.format(bname))):
                print "\nOutputs for {} already exist\n".format(bname)
                continue
        """
        
        cmd = 'python {} -rs {} -z {} -o {} -log'.format(runScript, stack, varsDict['inZonal'], varsDict['outCsv'])        
        print cmd
        os.system(cmd) 
        
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("stackType", type=str, 
                                help="Stack type (SGM, LVIS, or GLiHT)")
    parser.add_argument("zonalType", type=str,
                                help="Zonal type (ATL08 or GLAS)")
    parser.add_argument("-r", "--range", type=str,
                                help="Range for stack iteration (i.e. 1-20)")

    args = vars(parser.parse_args())

    main(args)
