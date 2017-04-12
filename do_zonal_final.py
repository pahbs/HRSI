#-------------------------------------------------------------------------------
# Name:        do_zonal.py
# Purpose:      Get stats from raster within zones defined by a shapefile
#
# Author:      pmontesa
#
# Created:     11/01/2016
# Copyright:   (c) pmontesa 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import os, sys
import workflow_functions as wf
import platform
from time import gmtime, strftime
import hi_res_filter_v5
import LLtoUTM as convert

# Get zonal stats at PALS, GLAS, etc
outDir = '/att/nobackup/pmontesa/DSM_ssg/glas_zonal_25m/results/do_zonal_final'

par = True
pointBuf = 25   # add & subtract this to a centroid's x,y to determine a box around the centroid from which to gather DSM pixels
#inZone = '/att/gpfsfs/userfs02/ppl/pmontesa/arc/GLAS_sib_test4.shp'

# Get zonal stats at GLAS shots for Siberia DSMs
#   output --> CSV files in /att/gpfsfs/userfs02/ppl/pmontesa/arc/
#               that look like 'GLAS_siberia_north_WV02_20130331_1030010020AA0B00_103001002167AC00.csv'
# These are the northern Siberia DSMs available in my pmontesa/outASP as of 1/12/2015
# Need to add the DSMs from cneigh/outASP when gpfs copy completes

# pg1..
RasList = [\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV01_20140307_102001002A803800_102001002ADD4300/out-strip-holes-fill-DEM.tif',\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV01_20130604_1020010023E3DB00_1020010024C5D300/out-strip-holes-fill-DEM.tif',\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV01_20130604_102001002138EC00_1020010021AA3000/out-strip-holes-fill-DEM.tif',\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV01_20120616_102001001B6B7800_102001001A4BEC00/out-strip-holes-fill-DEM.tif']

# l1
RasList = [\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV01_20120710_102001001CE5F900_102001001CE3A400/out-strip-holes-fill-DEM.tif',\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV01_20121007_102001001DA1E400_102001001F138E00/out-strip-holes-fill-DEM.tif',\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV01_20120711_102001001C88E300_102001001CDB0B00/out-strip-holes-fill-DEM.tif']

# pg2
RasList = [\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV02_20130915_1030010026161400_1030010026B60000/out-strip-holes-fill-DEM.tif',\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV01_20120602_102001001A7B6F00_102001001C395800/out-strip-holes-fill-DEM.tif',\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV02_20131025_1030010027457800_1030010028BBC900/out-strip-holes-fill-DEM.tif',\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV01_20130724_10200100246B6B00_1020010022D9CD00/out-strip-holes-fill-DEM.tif']

#pg3
RasList = [\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV01_20130226_102001002163B600_102001001F57B800/out-strip-holes-fill-DEM.tif',\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV02_20130309_1030010020632700_103001002065A800/out-strip-holes-fill-DEM.tif',\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV01_20120808_102001001C790F00_102001001CE43800/out-strip-holes-fill-DEM.tif',\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV02_20130218_103001001F5AEF00_103001001F288800/out-strip-holes-fill-DEM.tif',\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV01_20130216_102001001FC1D000_10200100212F9F00/out-strip-holes-fill-DEM.tif']

#l2
RasList = [\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV02_20130719_1030010025AD6800_10300100250F1100/out-strip-holes-fill-DEM.tif',\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV02_20130729_103001002575BC00_1030010024955700/out-strip-holes-fill-DEM.tif']

# luk
RasList = [\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV01_20140604_102001002EEB0200_102001002FCBB000/out-strip-holes-fill-DEM.tif',\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV02_20130616_103001002334B100_1030010024467000/out-strip-holes-fill-DEM.tif']

# luk2
RasList = [\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV01_20130602_1020010023B80B00_1020010021191100/out-strip-holes-fill-DEM.tif',\
'/att/briskfs/pmontesa/outASP/WV01_20130708_1020010022283300_10200100239A2100/out-strip-holes-fill-DEM.tif']

# Ak
# LiDAR file
inZone = '/att/gpfsfs/userfs02/ppl/pmontesa/glas/tiles_5deg/csv_files/gla14_N60E215.shp'
RasList = [\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV01_20130714_1020010021C68100_1020010024867400/out-strip-holes-fill-DEM.tif',\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV01_20130714_1020010024090600_10200100237D5D00/out-strip-holes-fill-DEM.tif']

# Ak2
# LiDAR file
inZone = '/att/gpfsfs/userfs02/ppl/pmontesa/glas/tiles_5deg/csv_files/gla14_N60E210.shp'
RasList = [\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV01_20130617_1020010022BB6400_1020010022894400/out-strip-holes-fill-DEM.tif',\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV01_20130730_10200100230D0800_10200100236BAE00/out-strip-holes-fill-DEM.tif']

# Kheta2
# LiDAR file
inZone = '/att/gpfsfs/userfs02/ppl/pmontesa/glas/tiles_5deg/csv_files/gla14_N70E090.shp'
RasList = [\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV01_20130718_1020010025309700_1020010022B29900/out-strip-holes-fill-DEM.tif',\
'/att/briskfs/pmontesa/outASP/WV01_20140621_102001002FA10A00_102001002F730B00/out-strip-holes-fill-DEM.tif']

# Lena
# LiDAR file
inZone = '/att/gpfsfs/userfs02/ppl/pmontesa/glas/tiles_5deg/csv_files/gla14_N70E125.shp'
RasList = [\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV01_20120716_102001001C973C00_102001001B01F400/out-strip-holes-fill-DEM.tif',\
'/att/gpfsfs/userfs02/ppl/pmontesa/outASP/WV02_20130718_103001002528B700_1030010024B25E00/out-strip-holes-fill-DEM.tif',\
'/att/briskfs/pmontesa/outASP/WV02_20140729_10300100349D0F00_1030010034AF4B00/out-strip-holes-fill-DEM.tif']

# luk2b
#g,c
# LiDAR file
inZone = '/att/gpfsfs/userfs02/ppl/pmontesa/glas/tiles_5deg/csv_files/gla14_N70E100.shp'
RasList = [\
'/att/briskfs/pmontesa/outASP/WV02_20130724_1030010026964100_10300100251D9400/out-strip-holes-fill-DEM.tif',\
'/att/briskfs/pmontesa/outASP/WV01_20130708_1020010022283300_10200100239A2100/out-strip-holes-fill-DEM.tif']

# aleppo
#outDir = '/att/gpfsfs/userfs02/ppl/pmontesa/projects/misc/aleppo/do_zonal_final'
#g,c
# LiDAR file
inZone = '/att/gpfsfs/userfs02/ppl/pmontesa/glas/tiles_5deg/csv_files/gla14_N35E035.shp'
RasList = [\
'/att/gpfsfs/userfs02/ppl/cneigh/nga_veg/data/mix/aleppo/WV02_2010_0615_0607/out-holes-fill-DEM.tif',\
'/att/gpfsfs/userfs02/ppl/cneigh/nga_veg/data/mix/aleppo/WV02_2010_0615_0424/out-holes-fill-DEM.tif',\
'/att/gpfsfs/userfs02/ppl/cneigh/nga_veg/data/mix/aleppo/WV02_2010_0607_0505/out-holes-fill-DEM.tif',\
'/att/gpfsfs/userfs02/ppl/cneigh/nga_veg/data/mix/aleppo/WV02_2010_0629_0618/out-holes-fill-DEM.tif']

# l3
#g,c
# LiDAR file
inZone = '/att/nobackup/pmontesa/glas/tiles_5deg/csv_files/gla14_N70E100.shp'
RasList = [\
'/att/briskfs/pmontesa/outASP/WV01_20130606_1020010022B6A500_1020010021CF0600/out-strip-holes-fill-DEM.tif',\
'/att/briskfs/pmontesa/outASP/WV01_20130916_10200100240DA200_1020010022E2E100/out-strip-holes-fill-DEM.tif']

# luk2c
#g,c
# LiDAR file
inZone = '/att/gpfsfs/userfs02/ppl/pmontesa/glas/tiles_5deg/csv_files/gla14_N70E100.shp'
RasList = [\
'/att/briskfs/pmontesa/outASP/WV02_20130724_1030010026964100_10300100251D9400/out-strip-holes-fill-DEM.tif',\
'/att/briskfs/pmontesa/outASP/WV01_20130616_1020010021113E00_1020010022687600_mode2_subp9_/out-strip-holes-fill-DEM.tif']

for num, Ras in enumerate(RasList):
    print(Ras)
    """
    This code parallelizes code that runs by looping over a RasList
        required: len(RasList) =< # nodes available from the pupsh cmd

    When par=True AND this script is launched with:
            pupsh "hostname ~ 'ecotone'" "python ~/code/do_zonal_final.py"
        This loop will run on each 'ecotone' VM, but only execute the gRas
        that coincides with the 'num' that creates a VM var that matches
        the node on which this script is launched

        e.g.
        1. the above cmd launches this python script across all 'cneigh' VMs
        2. on cneigh101, the script enters into this loop, sets num to '1' and makes enumVM = cneigh101
        3. this 'enumVM' var matches 'platform.node' and so it runs the cmd in the 'if' statement.
        4. the loop continues, and 'enumVM' will change and never match again the platfor.node() var, but since the same code is running on 'cneigh102', 'cneigh103', etc,
            then each index in the list gets processed AS LONG AS len(gRasList) =< # nodes available from the pupsh cmd.
    """
    if par:
        preLogtxt = []
        # Set enumVM according to enumeration var 'num'
        num = num + 1
        if int(num) < 10:
            num = "0" + str(num)
        enumVM = 'ecotone' + str(num)
        preLogtxt.append('\n\tEnumerated VM: ' + enumVM + "\n")
        print '\n\tEnumerated VM: ' + enumVM + " on node " + platform.node()

        # Excute loop if...
        #       ..the node on which this script was launched matches the enumVM identified with the enumerated 'num' var
        if platform.node() == enumVM:
            print(platform.node())
            # ---------
            # For logging on the fly: main log  (used to be MAIN_LOG. Became the only LOG)
            path, file = os.path.split(inZone)
            pathR, fileR = os.path.split(Ras)

            # Setup log file
            lfile = os.path.join(outDir,'logs','do_zonal_LOG_' + file.split('.shp')[0] +'_' + pathR.split('/')[-1] + '_' + platform.node() + '_' + strftime("%Y%m%d_%H%M%S") + '.txt')
            preLogtxt.append("\t"+lfile)
            so = se = open(lfile, 'w', 0) # open our log file
            sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0) # re-open stdout without buffering
            os.dup2(so.fileno(), sys.stdout.fileno()) # redirect stdout and stderr to the log file opened above
            os.dup2(se.fileno(), sys.stderr.fileno())

            for row in preLogtxt:
                print row

            print '\n\tRun hi_res_filter to get slopes of input DSMs'
            dtm, slp, asp = hi_res_filter_v5.main(Ras,25,1)

            # DSMs
            rasteridtail = 'dsm'
            print '\n\tZonal stats processing for zones of: ' + Ras
            wf.loop_zonal_stats(inZone,Ras,pointBuf,rasteridtail,outDir)

            # SLOPES
            rasteridtail = 'slp'
            print '\n\tZonal stats processing for zones of: ' + Ras
            wf.loop_zonal_stats(inZone,slp,pointBuf,rasteridtail,outDir)

            # ASPECTS
            rasteridtail = 'asp'
            print '\n\tZonal stats processing for zones of: ' + Ras
            wf.loop_zonal_stats(inZone,asp,pointBuf,rasteridtail,outDir)

    else:
        print '\n\tZonal stats processing for zones of: ' + Ras
        rasteridtail = ''
        wf.loop_zonal_stats(inZone,Ras,pointBuf,rasteridtail,outDir)


