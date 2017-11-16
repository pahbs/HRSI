#!/bin/bash
#
# Footprint HRSI DSMs

cd $HOME/code

now="$(date +'%Y%m%d')"

dsmDir=/att/nobackup/pmontesa/outASP
inputDir=/att/nobackup/pmontesa/inASP

dsmDir=/att/briskfs/pmontesa/outASP
inputDir=/att/briskfs/pmontesa/inASP

dsmDir=$1
outSHP=${2}_$now.shp
foot_file_str=$3

#foot_file_str=in-Band3N_proj.tif
echo $foot_file_str

# Shapefile name with current date
# hi-res
#outSHP=HRSI_DSM_footprints_$now.shp
# ASTER
#outSHP=HMA_AST_L1A_DSM_footprints_$now.shp


#python -c "import workflow_HRSI_v18 as w; w.footprint_dsm('$dsmDir', '$inputDir', '$myDir', '$outSHP')"

#find $dsmDir -type f -name "VALID*" -exec rm -rf {} \;

# Make GUnion work in SpatiaLite lib of SQLite
source /att/gpfsfs/home/pmontesa/code/sqlite_fix.env

# Remove tmp files from previous bad runs
find $dsmDir -type f -name VALID* -exec rm -rf {} \;

# Run the footprinting script, which adds to current daily file
python footprint_dsm.py ${dsmDir} ${outSHP} -kml -str_fp ${foot_file_str}

# Move to _footprints dir
mv $dsmDir/${2}_$now* $dsmDir/_footprints/