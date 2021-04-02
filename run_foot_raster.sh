#!/bin/bash
#
# Footprint Raster

t_start=$(date +%s)

now="$(date +'%Y%m%d')"

# Types:
# HRSI_DSM will be footprinted with stereo acquisition attributes
# CHM, ARCTICDEM and other types will be done with just path and filename attributes

TYPE=$1

if [ "$TYPE" = HRSIDSM ] ; then

    RAS_DIR=$2 #/att/pubrepo/DEM/hrsi_dsm #/att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/outASP #/att/gpfsfs/briskfs01/ppl/pmontesa/outASP/3DSI_pairset_01 #
    RAS_EXT=${3:-'out-DEM_1m.tif'}
    COARSEN_PCT=${4:-'0.25'} # 1pct at 1m will be a 100m res ; 0.25 is good value for speed and accuracy
    
    OUT_SHP=HRSI_DSM_footprints_$(basename $RAS_DIR) #_${RAS_EXT%.*}        #HRSI_DSM_footprints_pct${COARSEN_PCT}

    OUT_DIR=${RAS_DIR}_footprints

    NB_OUT=${5:-'false'}
    NB_OUT_DIR='/att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/arc'

    if [ "$NB_OUT" = true ] ; then
        OUT_DIR=${NB_OUT_DIR}/_footprints
    fi

    TMP_DIR=${OUT_DIR}/tmp_${RAS_EXT%.*}
    mkdir -p $OUT_DIR
    mkdir -p $TMP_DIR
    opts="-dsm"
fi
if [ "$TYPE" = CHM ] ; then

    RAS_DIR=$2  #/att/gpfsfs/briskfs01/ppl/pmontesa/hrsi_chm_work/hrsi_chm_multres
    RAS_EXT=${3:-'chm.tif'}

    COARSEN_PCT=${5:-'0.25'} # 0.5 pct at 2m will be a 100m res
    OUT_SHP=HRSI_CHM_footprints_${RAS_EXT%.*}

    TMP_DIR=$NOBACKUP/tmp/tmp_${RAS_EXT%.*}
    OUT_DIR=${RAS_DIR}_footprints
fi

if [ "$TYPE" = ARCTICDEM ] ; then

    RAS_DIR=/att/pubrepo/DEM/ArcticDEM/strips/v2/2m
    RAS_EXT=dem.tif
    
    COARSEN_PCT=0.5 # 0.5 pct at 2m will be a 100m res
    OUT_SHP=ARCTICDEM_footprints_pct${COARSEN_PCT}

    TMP_DIR=$NOBACKUP/tmp/tmp_${RAS_EXT%.*}
    OUT_DIR=/att/pubrepo/DEM/hrsi_dsm/_footprints
fi

if [ "$TYPE" = ARCTICDEMSUB ] ; then

    RAS_DIR=/att/gpfsfs/briskfs01/ppl/pmontesa/outASP_TEST/test_get_pairname_xmls
    RAS_EXT=dem.tif

    COARSEN_PCT=0.25 # 0.5 pct at 2m will be a 100m res
    OUT_SHP=ARCTICDEM_footprints_gliht_pct${COARSEN_PCT}

    TMP_DIR=$NOBACKUP/tmp/tmp_${RAS_EXT%.*}
    OUT_DIR=$RAS_DIR
    opts="-dsm"
fi

if [ "$TYPE" = TDX ] ; then

    RAS_DIR=/att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/data/tandemx/TDM90
    RAS_EXT=DEM.tif
    
    COARSEN_PCT=10 # 0.5 pct at 2m will be a 100m res
    OUT_SHP=TANDEMXDEM_footprints_pct${COARSEN_PCT}

    TMP_DIR=$NOBACKUP/tmp/tmp_${RAS_EXT%.*}
    OUT_DIR=`dirname ${RAS_DIR}`/_footprints
    mkdir -p $OUT_DIR
    mkdir -p $TMP_DIR

fi

if [ "$TYPE" = OTHER ] ; then

    RAS_DIR=$2
    RAS_EXT=$3
    COARSEN_PCT=${4:-'0.25'}
    COARSEN_PCT_STR=$(echo $COARSEN_PCT | tr . p)

    OUT_SHP=footprints_pct_${COARSEN_PCT_STR}_dir_$(basename ${RAS_DIR})_file_${RAS_EXT%.*}
    # Get RAS_DIR, append '_footprints' to it so you dont have to open up massive RAS_DIR to get to footprints 
    OUT_DIR=`dirname $(dirname ${RAS_DIR})`/_footprints
    TMP_DIR=${OUT_DIR}/tmp_${RAS_EXT%.*}
    mkdir -p $OUT_DIR
    mkdir -p $TMP_DIR
fi

if [ "$TYPE" = ASTER ] ; then

    RAS_DIR=${2:-'/att/gpfsfs/atrepo01/data/hma_data/ASTER/L1A_out/cr/dsm'}
    RAS_EXT=${3:-'DEM_cr.tif'}
    COARSEN_PCT=${4:-'100'}

    OUT_SHP=${TYPE}_footprints_${RAS_EXT%.*}_pct${COARSEN_PCT}

    TMP_DIR=$NOBACKUP/tmp/tmp_${RAS_EXT%.*}
    OUT_DIR=${RAS_DIR}_footprints
fi

# -tmp_dir is optional. If None, python sets it to out_dir

# Make GUnion work in SpatiaLite lib of SQLite
source $HOME/code/sqlite_fix_new.env

echo; echo "Run the footprinting script."; echo

mkdir -p $TMP_DIR
mkdir -p $OUT_DIR

opts+=" -kml"
opts+=" -link"
opts+=" -csv"
opts+=" -ras_ext $RAS_EXT"
opts+=" -out_shp ${OUT_SHP}"
opts+=" -c_pct $COARSEN_PCT"
opts+=" -tmp_dir $TMP_DIR"
args="$RAS_DIR $OUT_DIR"

rm -f ${OUT_DIR}/${OUT_SHP%.*}.csv

cmd="footprint_rasters.py $opts $args -dir_exc_list _ v d c l o"
echo $cmd ; eval $cmd

cmd="ogr2ogr -f 'GPKG' ${OUT_DIR}/${OUT_SHP}.gpkg ${OUT_DIR}/${OUT_SHP}.shp"
#echo "Convert shapefile to Geopackage" ; echo $cmd ; eval $cmd

zip ${OUT_DIR}/${OUT_SHP%.*}.zip ${OUT_DIR}/${OUT_SHP%.*}*

#rsync -avs ${OUT_DIR}/HRSI_DSM_footprints.* $NOBACKUP/tmp

rm ${TMP_DIR}/*

t_end=$(date +%s)
t_diff=$(expr $t_end - $t_start)
t_diff_hr=$(printf '%0.4f' $(echo "$t_diff/3600" | bc -l))
echo; date ; echo; echo "Total processing time (hr): $t_diff_hr"