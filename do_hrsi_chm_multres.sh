#!/bin/bash
#
# Individual runs of multi-res CHM generation approach

pairname=$1
res=$2

MAIN_DIR=$NOBACKUP/hrsi_chm_work/hrsi_chm_multres
DSM_DIR=/att/pubrepo/DEM/hrsi_dsm/${pairname}
TMP_DIR=${MAIN_DIR}/${pairname}
OUT_DIR=${MAIN_DIR}/${res}m

GROUND=out-DEM_24m.tif
CANOPY=out-DEM_1m.tif
echo; echo "Working on pairname: ${pairname}"; echo


if [ -e ${OUT_DIR}/${pairname}_chm.tif ] ; then
    echo; echo "Already finished: ${pairname}"; echo
else

    echo "Tmp output dir: $TMP_DIR "; echo

    mkdir -p $TMP_DIR

    cd $DSM_DIR
    echo; echo "Warping to ${res}m..."; echo
    warptool.py -tr $res -outdir ${TMP_DIR} ${GROUND} ${CANOPY}

    cd $TMP_DIR
    echo; echo "Computing difference map..."; echo

    if [ ! -e ${TMP_DIR}/${CANOPY}_warp.tif ] ; then
        cp ${DSM_DIR}/${CANOPY} ${CANOPY%.*}_warp.tif
    fi
    cmd="compute_dz.py ${GROUND%.*}_warp.tif ${CANOPY%.*}_warp.tif -outdir $TMP_DIR"
    echo $cmd
    eval $cmd
    DIF_MAP=${GROUND%.*}_warp_${CANOPY%.*}_warp_dz_eul.tif

    mkdir -p $OUT_DIR
    mv ${DIF_MAP} ${OUT_DIR}/${pairname}_chm.tif
    gdaladdo -r nearest -ro ${OUT_DIR}/${pairname}_chm.tif 2 4 8 16 32 64

    rm -rf ${TMP_DIR}
fi
# pairsnames to do:
# WV01_20150801_102001004385C300_102001004364FF00
# WV01_20120710_102001001CE5F900_102001001CE3A400
# WV01_20150801_10200100407BFB00_10200100424E3A00
# WV01_20130626_1020010022490400_102001002442FE00
# WV01_20150801_10200100407BFB00_10200100424E3A00