#-------------------------------------------------------------------------------
# Goal:  Create the LVIS (and GLiHT) stacks so that they can be run through
#        Zonal Stats process
# Notes: Naming conventions should match Will's DSM stacks
#        LVIS: /att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/data/LVIS/2017/
#        output/metrics_macander/LVIS2_ABoVE2017_0630_R1803_076394_RH100_mean_30m.tif
#
# LVIS:  For each flightline:
#           Create a NoData layer
#           Using the layer dictionary, get the corresponding geoTIFFS
#           Using build .vrt of geoTIFFs
#
#-------------------------------------------------------------------------------
import os, sys, glob

# for fun, maybe later if it makes sense
#class stack(self, sensor, flightline): # sensor is LVIS or GLiHT
    #self.basename = # depends on sensor. If LVIS: bname = LVIS2_ABoVE2017_0630_{flightline}_

    #def layerList(self, ddir):
        # for layer names in dict, and ddir, and ?, get list of layers
    #def createNoDataLayer(self, etc.)
        # create noData layer

#def layerDict = {} # shorthand name
    # dictionary of layers
    # this assumes layers correspond lke <basename_layerName.tif>
    # where basename = the flightline/file name and layerName is in layerDict
layerDict = {
    'LVIS':['retge_1p37_30m', 'RH010_mean_30m', 'RH015_mean_30m', 'RH020_mean_30m',
            'RH025_mean_30m', 'RH030_mean_30m', 'RH035_mean_30m', 'RH040_mean_30m',
            'RH045_mean_30m', 'RH050_mean_30m', 'RH055_mean_30m', 'RH060_mean_30m',
            'RH065_mean_30m', 'RH070_mean_30m', 'RH075_mean_30m', 'RH080_mean_30m',
            'RH085_mean_30m', 'RH090_mean_30m', 'RH095_mean_30m', 'RH096_mean_30m',
            'RH097_mean_30m', 'RH098_mean_30m', 'RH099_mean_30m', 'RH100_mean_30m',
            'ZG_mean_30m'],
    'GLiHT': []
    }

def buildLayerList(ddir, flight): # longhand path to geoTIFF
    # build list of geoTIFFs for stack
    # find using the layer dict, find corresponding geoTIFFs
    layerList = []
    for layer in layerDict['LVIS']:
        layerTif = os.path.join(ddir, '{}_{}.tif'.format(flight, layer))
        if os.path.isfile(layerTif):
            layerList.append(layerTif)
        else: print '{} does not exist'.format(layerTif)

    return layerList

def buildStack(ddir, flight, stackList):
    # take list of geoTIFFs and build a .vrt

    outVrt = os.path.join(ddir, '{}_stack.vrt'.format(flight))
    cmd = 'gdalbuildvrt -separate -o {} {}'.format(outVrt, ' '.join(stackList))

    # not sure if we wanna overwrite yet:
    os.system(cmd)

def createLvisNoDataLayer(ddir, flight, useTif):
    # useTif is the raster we want to use to make the no data layer
    # no data layer should be: 0 where data, 1 otherwise

    noDataLayer = os.path.join(ddir, '{}_mask_proj.tif'.format(flight))

    cmd = 'gdal_calc.py -A {} --outfile={} --calc "(A==255)*1" \
            --co "COMPRESS=LZW" --NoDataValue=1 --type Byte'.format(useTif, noDataLayer)
    print 'Creating noDataLayer {}'.format(noDataLayer)
    os.system(cmd)

    return noDataLayer

def createStackLog(ddir, flight):
    # create the Log.txt file, to match Will's
    # use the layer dict, but remove _mean and _30m; add nodata mask
    stackLog = os.path.join(ddir, '{}_stack_Log.txt'.format(flight))

    with open(stackLog, 'w') as sl:
        sl.write('header\n')
        for i, l in enumerate(layerDict['LVIS']):
            sl.write('{},{}\n'.format(i+1, l.strip('_30m').strip('_mean')))
        sl.write('{},mask_proj'.format(i+2))


def processLvis(ddir):

    # ddir being where the stacks live

    #for flight in flightlines:
    flight = 'LVIS2_ABoVE2017_0630_R1803_076394'

    #import pdb; pdb.set_trace()
    layerList = buildLayerList(ddir, flight)

    # create no data layer, add it to the layerList
    noDataLayer = createLvisNoDataLayer(ddir, flight, layerList[0])
    layerList.append(noDataLayer)

    buildStack(ddir, flight, layerList)

    createStackLog(ddir, flight)





def main():

    # log no matter what

    # if LVIS:
    #indir = '/att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/data/LVIS/2017/output/metrics_macander/'
    indir = '/att/gpfsfs/briskfs01/ppl/mwooten3/3DSI/LVIS/data/LVIS2_ABoVE2017_0630_R1803_076394'
    processLvis(indir)
    # get list of flightlines from dir
    # for each flightline, build stack
    # --> get list of layers from dict
    # --> create NoData layer. add to list
    # --> build VRT
    # --> Create stack "Log" aka the layer list we use in zonalStats

if __name__ == '__main__':
    main()




