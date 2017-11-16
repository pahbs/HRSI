#!/bin/bash
#
# Create Multispectral HRSI data that is QGIS-ready
#
# MOSAIC each single band of the M1BS (multispectral) HRSI scenes into strips
# STACK the single band strips into a multiband strip 
# MAPPROJECT the HRSI multiband strips using ASTER GDEM
# produces a mapprojected multiband GeoTiff with overviews 

# DATA dir

# these dirs are just examples...change as needed
dir='/att/gpfsfs/userfs02/ppl/cneigh/nga_veg/data/mix/do_rn_out/WV02_103001000C29E000_M1BS_052510443010_02'
catID='103001000C29E000'
date='20110701'
sensor='WV02'
ext='ntf'

outMAPdir=/att/nobackup/pmontesa/outMAP

cd $dir

# Here is the ASTER GDEM dir on ADAPT: /att/gpfsfs/atrepo01/data/DEM/ASTERGDEM
inDEM=/att/nobackup/cneigh/nga_veg/in_DEM/aster_gdem2_siberia_N60N76.tif

# Set up the filename you want 
mosMerge=${sensor}_${date}_${catID}.r100.M1BS.tif

# MOSAIC
# For each band of MS, mosaic all scenes associated with the strip of data (denoted with the catID)
# NOTE the '&' at the end of dg_mosaic, which launches the process in the background

for bandNum in `seq 1 4`;
do
	dg_mosaic --band=$bandNum *${catID}_*M1BS*${ext} --output-prefix ${sensor}_${date}_${catID} --reduce-percent=100 &
done

# STACK
# Stack the mosaiced bands
gdal_merge.py -separate -o $mosMerge `ls ${sensor}_${date}_${catID}*tif`

# Now, COPY XML from a band's XML to the merged dataset
cp ${sensor}_${date}_${catID}.r100.b1.xml ${mosMerge%.tif}.xml

# MAPPROJECT
# to Geographic WGS84 (EPSG:4326) -- you can choose another prj if you want.
mapproject --nodata-value=-99 --threads=20 -t rpc --t_srs 'EPSG:4326' $inDEM $mosMerge ${mosMerge%.tif}.xml ${mosMerge%.tif}_prj.tif

# you are crazy if you don't build pyramid layers...
gdaladdo -r average ${mosMerge%.tif}_prj.tif 2 4 8 16

