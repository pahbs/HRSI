#! /bin/bash

#David Shean
#dshean@gmail.com

#This will co-register a single DEM
#See dem_coreg_all.sh for batch processing

#Input should be highest res version of DEM (i.e., DEM_2m.tif)
dem=$1

if [ ! -e $dem ] ; then
    echo "Unable to find source DEM: $dem"
    exit
fi

#Define the reference DEM
#Need to create vrt with 1 arcsec over areas where 1/3 is not avail
#1-arcsec NED (30 m) for CONUS
#ref=/nobackup/deshean/rpcdem/ned1/ned1_tiles_glac24k_115kmbuff.vrt
#1/3-arcsec NED (10 m) for CONUS
#ref=/nobackup/deshean/rpcdem/ned13/ned13_tiles_glac24k_115kmbuff.vrt
#1-arcsec SRTM (30 m) for HMA
ref=/nobackup/deshean/rpcdem/hma/srtm1/hma_srtm_gl1.vrt
#1-arcsec (30m) ASTER GDEM v2 for northern Siberia
ref=/att/gpfsfs/userfs02/ppl/pmontesa/refdem/siberia/SIB_ASTGTM2_pct100.vrt

if [ ! -e $ref ] ; then
    echo "Unable to find ref DEM: $ref"
    exit
fi

demdir=$(dirname $dem)
dembase=$(basename $dem)
#This will be pc_align output directory
outdir=${dembase%.*}_grid_align
dembase=$(echo $dembase | awk -F'-' '{print $1}')

#PAUL ##This is DEM_32m reference mask output by dem_mask.py
#PAUL #dem_mask=$demdir/${dembase}-DEM_32m_ref.tif

#PAUL #if [ ! -e $dem_mask ] ; then
#PAUL #    echo "Unable to find reference DEM mask, need to run dem_mask.py"
#PAUL #    exit
#PAUL #fi

#Clip the reference DEM to the DEM_32m extent
warptool.py -te $dem -tr $ref -t_srs $dem -outdir $demdir $ref

#PAUL ##Not using a masked dem---
#PAUL #refdem=$demdir/$(basename $ref)
#PAUL #refdem=${refdem%.*}_warp.tif
#PAUL ##Mask the ref using valid pixels in DEM_32m_ref.tif product
#PAUL #apply_mask.py -extent intersection $refdem $dem_mask
#PAUL #refdem_masked=${refdem%.*}_masked.tif

#point-to-point
#PAUL #pc_align_wrapper.sh $refdem_masked $dem
pc_align_wrapper.sh $ref $dem

cd $demdir
log=$(ls -t $outdir/*.log | head -1)
if [ -e $log ] ; then
    if grep -q 'Translation vector' $log ; then
        #PAUL #apply_dem_translation.py ${dembase}-DEM_32m.tif $log
        #PAUL #apply_dem_translation.py ${dembase}-DEM_8m.tif $log
        apply_dem_translation.py ${dembase}-DEM.tif $log
        ln -sf $outdir/*DEM.tif ${dembase}-DEM_2m_trans.tif
        #PAUL #compute_dh.py $(basename $refdem) ${dembase}-DEM_8m_trans.tif
    fi
fi
