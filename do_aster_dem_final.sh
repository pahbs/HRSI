#!/bin/bash
#
# TEST by SCENE starting with a sceneName (eg; AST_L1A_00306232003135434_20170125151451_24922)
# Process ASTER L1A data from earthdata.nasa.gov polygon searches
#

################################
#_____Function Definitions_____
################################

run_asp_fine () {

    SGM=$1
    inDEM=$2
    tileSize=$3
    sceneName=$4
    now=$5
    L1Adir=$6

    cd $L1Adir

    corrKern=7
    subpixKern=7

    #runDir=corr${corrKern}_subpix${subpixKern}

    echo "[4] Working on: $sceneName -- Checking for PC.tif ..."
    echo "Input coarse DEM = ${inDEM}"

    outPrefix=$sceneName/outASP/out

    # Stereo Run Options
    # NOTE:  running MGM (--stereo-algorithm 2 instead of SGM)
    #
    par_opts="--corr-tile-size $tileSize --job-size-w $tileSize --job-size-h $tileSize --processes 18 --threads-multiprocess 10 --threads-singleprocess 32 --nodes-list=/att/gpfsfs/home/pmontesa/code/nodes_ecotone07"
    sgm_opts="-t aster --threads=10 --xcorr-threshold -1 --corr-kernel $corrKern $corrKern --cost-mode 4 --subpixel-mode 0 --median-filter-size 3 --texture-smooth-size 13 --texture-smooth-scale 0.13"
    reg_opts="-t aster --subpixel-mode 2 --corr-kernel $corrKern $corrKern --subpixel-kernel $subpixKern $subpixKern"
    stereo_opts="$sceneName/in-Band3N_proj.tif $sceneName/in-Band3B_proj.tif $sceneName/in-Band3N.xml $sceneName/in-Band3B.xml $outPrefix $inDEM"

    # Run stereo with the map-projected images
    #
    if [ ! -f $outPrefix-PC.tif ]; then

        echo "No PC.tif file -- determine which stereo algorithm to run ..."
        echo "Tile size = ${tileSize}"

        if $SGM; then

            echo "Stereo mode SGM or MGM = ${SGM}"

            find $sceneName/outASP -type f -name out* -exec rm -rf {} \;

            echo "   Running stereo with SGM mode ..."
            echo "   Running stereo with SGM mode ..."$'\r' >> ${sceneName}_${now}.log
            #parallel_stereo  --stereo-algorithm 1 $par_opts $sgm_opts $stereo_opts
            stereo  --stereo-algorithm 1 $sgm_opts $stereo_opts
            echo "   Finished stereo from SGM mode."

            if [ ! -f $outPrefix-PC.tif ]; then

                find $sceneName/outASP -type f -name out* -exec rm -rf {} \;

                echo "   Running stereo with MGM mode b/c SGM failed to create a PC.tif ..."
                echo "   Running stereo with MGM mode b/c SGM failed to create a PC.tif ..."$'\r' >> ${sceneName}_${now}.log
                stereo  --stereo-algorithm 2 $sgm_opts $stereo_opts
                echo "   Finished stereo from MGM mode."
            else
                echo "   Stereo successful from SGM mode."
            fi

        else
            echo "   Running stereo ..."
            echo "   Running stereo ..."$'\r' >> ${sceneName}_${now}.log
            #parallel_stereo $par_opts $reg_opts $stereo_opts
            stereo $reg_opts $stereo_opts
        fi
    fi


    # Create the final DEM and ortho'd Pan
    if [ -f $outPrefix-PC.tif ]; then

        echo "  PC.tif exists..."

        if [ ! -f $outPrefix-DEM-hlshd-e25.tif ]; then

            echo "  No hillshade, running point2dem..."

            echo "[5] Running fine point2dem on $sceneName/outASP/$runDir ..."$'\r' >> ${sceneName}_${now}.log

            point2dem --threads=6 -r earth $outPrefix-PC.tif -o $outPrefix --orthoimage $outPrefix-L.tif

            # Final Viewing GeoTiffs
            echo "[6] Running final viewing GeoTiffs on $sceneName ..."$'\r' >> ${sceneName}_${now}.log
            hillshade $outPrefix-DEM.tif -o $outPrefix-DEM-hlshd-e25.tif -e 25
            colormap $outPrefix-DEM.tif -s $outPrefix-DEM-hlshd-e25.tif -o $outPrefix-DEM-clr-shd.tif --colormap-style /att/gpfsfs/home/pmontesa/code/color_lut_hma.txt

            gdal_translate -of VRT ${L1Adir}/$outPrefix-DEM-clr-shd.tif ${L1Adir}_out/clr/${sceneName}-CLR.vrt &
            gdal_translate -of VRT ${L1Adir}/$outPrefix-DRG.tif ${L1Adir}_out/drg/${sceneName}-DRG.vrt &
            gdal_translate -of VRT ${L1Adir}/$outPrefix-DRG.tif ${L1Adir}_out/dsm/${sceneName}-DEM.vrt &

            gdaladdo -r average ${L1Adir}/$outPrefix-DEM-clr-shd.tif 2 4 8 16 &
            gdaladdo -r average ${L1Adir}/$outPrefix-DRG.tif 2 4 8 16 &
            gdaladdo -r average ${L1Adir}/$outPrefix-DEM.tif 2 4 8 16 &
            echo "[7] Finished processing ${sceneName}."$'\r' >> ${sceneName}_${now}.log
            echo "----------}."$'\r' >> ${sceneName}_${now}.log
        else
            if [ ! -f $outPrefix-DEM-clr-shd.tif ]; then

                echo "  Hillshade exists -- CLR-SHD needed ..."
                colormap $outPrefix-DEM.tif -s $outPrefix-DEM-hlshd-e25.tif -o $outPrefix-DEM-clr-shd.tif --colormap-style /att/gpfsfs/home/pmontesa/code/color_lut_hma.txt
            else
                echo "   CLR-SHD exists."
                echo "   CLR-SHD exists."$'\r' >> ${sceneName}_${now}.log
            fi

            echo "  Create output VRT files: CLR, DEM, DRG..."
            gdal_translate -of VRT ${L1Adir}/$outPrefix-DEM-clr-shd.tif ${L1Adir}_out/clr/${sceneName}-CLR.vrt &
            gdal_translate -of VRT ${L1Adir}/$outPrefix-DRG.tif ${L1Adir}_out/drg/${sceneName}-DRG.vrt &
            gdal_translate -of VRT ${L1Adir}/$outPrefix-DRG.tif ${L1Adir}_out/dsm/${sceneName}-DEM.vrt &
        fi
    else
        echo "   No PC.tif file !!! DEM not created."
        echo "   No PC.tif file !!! DEM not created."$'\r' >> ${sceneName}_${now}.log
    fi

}
##############################################

#
# Hard coded stuff
#

batch=$1

topDir=/att/gpfsfs/atrepo01/data/hma_data/ASTER

inDEM=/att/gpfsfs/userfs02/ppl/pmontesa/HiMAT/hma_dem/HMA_ASTGTM2_pct100.tif
#inDEM=/att/nobackup/pmontesa/projects/siberia/aster_dem/SIB_ASTGTM2_pct100.tif
now="$(date +'%Y%m%d%T')"

#
# Process the AST_L1A dir indicated with the sceneName
#
cd $topDir

hostN=`/bin/hostname -s`

#find . -type d -name "AST_*" > scenes4stereo.list
#python ~/code/gen_csv_chunks.py scenes4stereo.list ~/code/nodes_tmp


# Read in sceneList of AST L1A scenes from an orderZip
while read -r sceneName; do
    sceneName=`echo $(basename $sceneName)`

    # ---- Pixel sizes
    # 10m
    pixResFineSize=0.0000898315284119

    # Stereo corr & job tile size for SGM run
    # tilesize^2 * 300 / 1e9 = RAM needed per thread
    tileSize=3200
    SGM=true

    #_____Function calls_____

    #run_mapprj $inDEM $sceneName $now $pixResFineSize
    run_asp_fine $SGM $inDEM $tileSize $sceneName $now $topDir/L1A


done < scenes.list_${batch}_${hostN}



