#!/bin/bash
#
# TEST by SCENE starting with a sceneName (eg; AST_L1A_00306232003135434_20170125151451_24922)
# Process ASTER L1A data from earthdata.nasa.gov polygon searches
# Filters
# June 1 - Sept 15 for each year?

topDir=/att/gpfsfs/userfs02/ppl/pmontesa/projects/misc/aleppo/ASTER/L1A
cd $topDir
#HMA_GDEM=/att/gpfsfs/userfs02/ppl/pmontesa/HiMAT/hma_dem/HMA_ASTGTM2_pct100.tif
sceneName=$1
now="$(date +'%Y%m%d%T')"


# Process the AST_L1A dir indicated with the sceneName

dataDir=`echo $sceneName`

echo "Running aster2asp on $dataDir ..."$'\r' >> ${sceneName}_${now}.log

if [ -f "${dataDir}/in-Band3N.tif" ]
    then
        echo "ASP input files exists already."
    else
        aster2asp --threads=18 ${dataDir} -o ${dataDir}/in
fi

## Kernel Size Lists
#declare -a corrKernList=("35" "25" "15")
#declare -a subpixKernList=("25" "15" I"11")
declare -a corrKernList=("7")
declare -a subpixKernList=("7")

# ---- Pixel sizes
## 7m
#pixResFineSize=0.0000628820698883
#pixResFine=7

## 10m
pixResFineSize=0.0000898315284119
pixResFine=10

## 300m
pixRes300m=0.0026949458523585
outCoarse=$dataDir/outASPcoarse/out-300m

## Stereo corr & job tile size for SGM run
# tilesize^2 * 300 / 1e9 * numThreads
tileSize=3200
SGM=true

################################
#_____Function Definitions_____

run_mapprj () {
if [ -f "${dataDir}/in-Band3N_proj.tif" ]
    then
        echo "ASP mapprojected input exist."
    else
        ##### ASP: Workflow from manual

        #echo "[1] Running initial stereo on $dataDir/outASPcoarse..."$'\r' >> ${sceneName}_${now}.log
        # Initial stereo with defaults
        parallel_stereo --nodes-list=/att/gpfsfs/home/pmontesa/code/nodes_ecotone07 --processes 18 --threads-multiprocess 16 --threads-singleprocess 32 -t aster --subpixel-mode 3 $dataDir/in-Band3N.tif $dataDir/in-Band3B.tif $dataDir/in-Band3N.xml $dataDir/in-Band3B.xml $dataDir/outASPcoarse/out
        ###stereo -t aster --subpixel-mode 3 $dataDir/in-Band3N.tif $dataDir/in-Band3B.tif $dataDir/in-Band3N.xml $dataDir/in-Band3B.xml $dataDir/outASPcoarse/out

        #echo "[2] Running coarse point2dem on $dataDir/outASPcoarse ..."$'\r' >> ${sceneName}_${now}.log
        # Create a coarse and smooth DEM at 300 meters/pixel
        point2dem --threads=18 -r earth --tr $pixRes300m $dataDir/outASPcoarse/out-PC.tif -o $outCoarse

		  echo "Mapprojecting..."
		  echo "......--------------------------------------"
        echo "[3] Running mapproject of fine input onto coarse DSM ..."$'\r' >> ${sceneName}_${now}.log
        # Map-project onto this DEM at 7 meters/pixel
        mapproject --tr $pixResFineSize $outCoarse-DEM.tif $dataDir/in-Band3N.tif $dataDir/in-Band3N.xml $dataDir/in-Band3N_proj.tif
        mapproject --tr $pixResFineSize $outCoarse-DEM.tif $dataDir/in-Band3B.tif $dataDir/in-Band3B.xml $dataDir/in-Band3B_proj.tif

fi
echo "<><><><><>"$'\r' >> ${sceneName}_${now}.log
}



run_asp_fine () {
    corrKern=$1
    subpixKern=$2
	 SGM=$3
	 runDir=corr${corrKern}_subpix${subpixKern}

    echo "[4] Running stereo on $dataDir/outASP/$runDir ..."$'\r' >> ${sceneName}_${now}.log
    # Run stereo with the map-projected images
	 #
	 if [ "$SGM"=true ]
	 	 then
	 	 	  #<><><> Stereo with SGM for Slopes and Ice/Snow
	 	 	  parallel_stereo -t aster --stereo-algorithm 1 --nodes-list=/att/gpfsfs/home/pmontesa/code/nodes_ecotone07 \
	 	 	  --threads 1 --xcorr-threshold -1 --corr-kernel $corrKern $corrKern --corr-tile-size $tileSize --job-size-w $tileSize --job-size-h $tileSize \
	 	 	  --processes 18 --threads-multiprocess 1 --threads-singleprocess 32\
	 	 	  --cost-mode 4 --subpixel-mode 0 --median-filter-size 3 --texture-smooth-size 13 --texture-smooth-scale 0.13 \
	 	 	  $dataDir/in-Band3N_proj.tif $dataDir/in-Band3B_proj.tif $dataDir/in-Band3N.xml $dataDir/in-Band3B.xml $dataDir/outASP/$runDir/out-proj $HMA_GDEM
	 	 else
			  #parallel_stereo --nodes-list=/att/gpfsfs/home/pmontesa/code/nodes_ecotone07 \
	 		  #--processes 18 --threads-multiprocess 16 --threads-singleprocess 32 -t aster --subpixel-mode 2 --corr-kernel $corrKern $corrKern --subpixel-kernel $subpixKern $subpixKern \
	 		  #$dataDir/in-Band3N_proj.tif $dataDir/in-Band3B_proj.tif $dataDir/in-Band3N.xml $dataDir/in-Band3B.xml $dataDir/outASP/$runDir/out-proj $HMA_GDEM
			  stereo -t aster --subpixel-mode 2 --corr-kernel $corrKern $corrKern --subpixel-kernel $subpixKern $subpixKern $dataDir/in-Band3N_proj.tif \
			  $dataDir/in-Band3B_proj.tif $dataDir/in-Band3N.xml $dataDir/in-Band3B.xml $dataDir/outASP/$runDir/out-proj $HMA_GDEM
	 fi

	 echo "[5] Running fine point2dem on $dataDir/outASP/$runDir ..."$'\r' >> ${sceneName}_${now}.log
    # Create the final DEM and ortho'd Pan
	 #
    outFine=$dataDir/outASP/$runDir/out-${pixResFine}m
    point2dem --threads=6 -r earth --tr $pixResFineSize $dataDir/outASP/$runDir/out-proj-PC.tif -o $outFine --orthoimage $dataDir/outASP/$runDir/out-proj-L.tif

    # Final Viewing GeoTiffs
	 #
    echo "[6] Running final viewing GeoTiffs on $dataDir/outASP/$runDir ..."$'\r' >> ${sceneName}_${now}.log
    hillshade $outFine-DEM.tif -o $outFine-DEM-hlshd-e25.tif -e 25
    colormap $outFine-DEM.tif -s $outFine-DEM-hlshd-e25.tif -o $outFine-DEM-clr-shd.tif --colormap-style /att/gpfsfs/home/pmontesa/code/color_lut_hma.txt

    gdal_translate -of VRT ${topDir}/$outFine-DEM-clr-shd.tif ${topDir}/vrt_clr/${dataDir}_$runDir_out-${pixResFine}m-DEM-clr-shd.vrt
    gdal_translate -of VRT ${topDir}/$outFine-DRG.tif ${topDir}/vrt_clr/${dataDir}_$runDir_out-${pixResFine}m-DRG.vrt
    gdaladdo -r average ${topDir}/$outFine-DEM-clr-shd.tif 2 4 8 16 &
    gdaladdo -r average ${topDir}/$outFine-DRG.tif 2 4 8 16 &
    echo "[7] Finished processing $dataDir/outASP/$runDir."$'\r' >> ${sceneName}_${now}.log
    echo "----------}."$'\r' >> ${sceneName}_${now}.log
}

################################
#_____Function calls_____

run_mapprj

for corrKern in "${corrKernList[@]}"
    do
        for subpixKern in "${subpixKernList[@]}"
            do
                run_asp_fine $corrKern $subpixKern &
            done
done



