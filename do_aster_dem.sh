#!/bin/bash
#
# TEST by SCENE starting with a sceneName (eg; AST_L1A_00306232003135434_20170125151451_24922)
# Process ASTER L1A data from earthdata.nasa.gov polygon searches
#

################################
#_____Function Definitions_____
################################

run_mapprj_coarse () {

    sceneName=$1
    now=$2
    pixResFineSize=$3
    outCoarse=$4

    if [ -f "${sceneName}/in-Band3N_proj.tif" ]
        then
            echo "ASP mapprojected input exist."
        else
            ##### ASP: Workflow from manual

            #echo "[1] Running initial stereo on $sceneName/outASPcoarse..."$'\r' >> ${sceneName}_${now}.log
            # Initial stereo with defaults
            parallel_stereo --nodes-list=/att/gpfsfs/home/pmontesa/code/nodes_ecotone07 --processes 18 --threads-multiprocess 16 --threads-singleprocess 32 -t aster --subpixel-mode 3 $sceneName/in-Band3N.tif $sceneName/in-Band3B.tif $sceneName/in-Band3N.xml $sceneName/in-Band3B.xml $sceneName/outASPcoarse/out
            ###stereo -t aster --subpixel-mode 3 $sceneName/in-Band3N.tif $sceneName/in-Band3B.tif $sceneName/in-Band3N.xml $sceneName/in-Band3B.xml $sceneName/outASPcoarse/out

            #echo "[2] Running coarse point2dem on $sceneName/outASPcoarse ..."$'\r' >> ${sceneName}_${now}.log
            # Create a coarse and smooth DEM at 300 meters/pixel
            point2dem --threads=18 -r earth --tr $pixRes300m $sceneName/outASPcoarse/out-PC.tif -o $outCoarse

    		  echo "Mapprojecting..."
    		  echo "......--------------------------------------"
            echo "[3] Running mapproject of fine input onto coarse DSM ..."$'\r' >> ${sceneName}_${now}.log
            # Map-project onto this DEM at 10 meters/pixel
            mapproject --threads=8 --tr $pixResFineSize $outCoarse-DEM.tif $sceneName/in-Band3N.tif $sceneName/in-Band3N.xml $sceneName/in-Band3N_proj.tif &
            mapproject --threads=8 --tr $pixResFineSize $outCoarse-DEM.tif $sceneName/in-Band3B.tif $sceneName/in-Band3B.xml $sceneName/in-Band3B_proj.tif

    fi
    echo "<><><><><>"$'\r' >> ${sceneName}_${now}.log
}

run_mapprj () {
    inDEM=$1
    sceneName=$2
    now=$3
    pixResFineSize=$4

    if [ -f "${sceneName}/in-Band3N_proj.tif" ]
        then
            echo "ASP mapprojected input exist."
        else
            ##### ASP: Workflow from manual
    		echo "Mapprojecting..."
		    echo "......--------------------------------------"
            echo "[3] Running mapproject of fine input onto coarse DSM ..."$'\r' >> ${sceneName}_${now}.log
            # Map-project onto this DEM
            mapproject --threads=8 --tr $pixResFineSize $inDEM $sceneName/in-Band3N.tif $sceneName/in-Band3N.xml $sceneName/in-Band3N_proj.tif &
            mapproject --threads=8 --tr $pixResFineSize $inDEM $sceneName/in-Band3B.tif $sceneName/in-Band3B.xml $sceneName/in-Band3B_proj.tif 
    fi
    echo "<><><><><>"$'\r' >> ${sceneName}_${now}.log
}

run_asp_fine () {
    corrKern=$1
    subpixKern=$2
    SGM=$3
    inDEM=$4
    tileSize=$5
    sceneName=$6
    now=$7

    #runDir=corr${corrKern}_subpix${subpixKern}

    echo "[4] Running stereo on $sceneName ..."$'\r' >> ${sceneName}_${now}.log
    echo "Stereo mode SGM = ${SGM}"
    echo "Input coarse DEM = ${inDEM}"
    echo "Tile size = ${tileSize}"

	 outPrefix=$sceneName/outASP/out

	 # Stereo Run Options
	 par_opts="--corr-tile-size $tileSize --job-size-w $tileSize --job-size-h $tileSize --processes 18 --threads-multiprocess 10 --threads-singleprocess 32 --nodes-list=/att/gpfsfs/home/pmontesa/code/nodes_ecotone07"
	 sgm_opts="-t aster --stereo-algorithm 1 --threads=10 --xcorr-threshold -1 --corr-kernel $corrKern $corrKern --cost-mode 4 --subpixel-mode 0 --median-filter-size 3 --texture-smooth-size 13 --texture-smooth-scale 0.13"
	 reg_opts="-t aster --subpixel-mode 2 --corr-kernel $corrKern $corrKern --subpixel-kernel $subpixKern $subpixKern"
	 stereo_opts="$sceneName/in-Band3N_proj.tif $sceneName/in-Band3B_proj.tif $sceneName/in-Band3N.xml $sceneName/in-Band3B.xml $outPrefix $inDEM"

    # Run stereo with the map-projected images
    #
    if $SGM
        then
            echo "   Running stereo with SGM mode for Slopes and Ice/Snow ..."
            echo "   Running stereo with SGM mode for Slopes and Ice/Snow ..."$'\r' >> ${sceneName}_${now}.log
				#parallel_stereo $par_opts $sgm_opts $stereo_opts
            stereo $sgm_opts $stereo_opts
            
        else
            echo "   Running stereo ..."
            echo "   Running stereo ..."$'\r' >> ${sceneName}_${now}.log
            #parallel_stereo $par_opts $reg_opts $stereo_opts
            stereo $reg_opts $stereo_opts 
    fi

    echo "[5] Running fine point2dem on $sceneName/outASP/$runDir ..."$'\r' >> ${sceneName}_${now}.log
    # Create the final DEM and ortho'd Pan
    #
    #outFine=$sceneName/outASP/$runDir/out
    point2dem --threads=6 -r earth $outPrefix-PC.tif -o $outPrefix --orthoimage $outPrefix-L.tif

    # Final Viewing GeoTiffs
    #
    echo "[6] Running final viewing GeoTiffs on $sceneName ..."$'\r' >> ${sceneName}_${now}.log
    hillshade $outPrefix-DEM.tif -o $outPrefix-DEM-hlshd-e25.tif -e 25
    colormap $outPrefix-DEM.tif -s $outPrefix-DEM-hlshd-e25.tif -o $outPrefix-DEM-clr-shd.tif --colormap-style /att/gpfsfs/home/pmontesa/code/color_lut_hma.txt

    gdal_translate -of VRT ${topDir}/$outPrefix-DEM-clr-shd.tif ${topDir}/clr/${sceneName}_out-DEM-clr-shd.vrt
    gdal_translate -of VRT ${topDir}/$outPrefix-DRG.tif ${topDir}/drg/${sceneName}_out-DRG.vrt
	 gdal_translate -of VRT ${topDir}/$outPrefix-DRG.tif ${topDir}/dsm/${sceneName}_out-DEM.vrt
    gdaladdo -r average ${topDir}/$outPrefix-DEM-clr-shd.tif 2 4 8 16 &
    gdaladdo -r average ${topDir}/$outPrefix-DRG.tif 2 4 8 16 &
    echo "[7] Finished processing ${sceneName}."$'\r' >> ${sceneName}_${now}.log
    echo "----------}."$'\r' >> ${sceneName}_${now}.log
}
##############################################

#
# Hard coded stuff
#
#sceneName=$1

topDir=/att/nobackup/pmontesa/ASTER/L1A
inDEM=/att/gpfsfs/userfs02/ppl/pmontesa/HiMAT/hma_dem/HMA_ASTGTM2_pct100.tif
inDEM=/att/nobackup/pmontesa/projects/siberia/aster_dem/SIB_ASTGTM2_pct100.tif
now="$(date +'%Y%m%d%T')"

#
# Process the AST_L1A dir indicated with the sceneName
#
cd $topDir

hostN=`/bin/hostname -s` 

# Read in sceneList of AST L1A scenes from an orderZip
#
while read -r sceneName; do
    echo $sceneName
    if [ -f "${sceneName}/in-Band3N.tif" ]
        then
            echo "ASP input files exists already."
        else
            echo "Running aster2asp on $sceneName ..."$'\r' >> ${sceneName}_${now}.log
            aster2asp --threads=18 ${sceneName} -o ${sceneName}/in
    fi

    ## Kernel Size Lists
    declare -a corrKernList=("9")
    declare -a subpixKernList=("9")

    # ---- Pixel sizes
    ## 7m
    #pixResFineSize=0.0000628820698883

    ## 10m
    pixResFineSize=0.0000898315284119

    ## Stereo corr & job tile size for SGM run
    # tilesize^2 * 300 / 1e9 * numThreads
    tileSize=3200
    SGM=true

    ################################
    #_____Function calls_____
    ################################

    run_mapprj $inDEM $sceneName $now $pixResFineSize

    for corrKern in "${corrKernList[@]}"
        do
            for subpixKern in "${subpixKernList[@]}"
                do
                    run_asp_fine $corrKern $subpixKern $SGM $inDEM $tileSize $sceneName $now &
                done
        done

done < ${1%.csv}_${hostN}.csv



