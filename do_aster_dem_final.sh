#!/bin/bash
#
# TEST by SCENE starting with a sceneName (eg; AST_L1A_00306232003135434_20170125151451_24922)
# Process ASTER L1A data from earthdata.nasa.gov polygon searches
#

################################
#_____Function Definitions_____
################################

run_mapprj() {
    inDEM=$1
    sceneName=$2
    pixResFineSize=$3
	 logFile=$4
    
    echo "[3] Running mapproject ..." | tee -a $logFile
    mapproject --threads=5 --tr $pixResFineSize $inDEM $sceneName/in-Band3N.tif $sceneName/in-Band3N.xml $sceneName/in-Band3N_proj.tif
    mapproject --threads=5 --tr $pixResFineSize $inDEM $sceneName/in-Band3B.tif $sceneName/in-Band3B.xml $sceneName/in-Band3B_proj.tif
    #echo "${sceneName}" >>               AST_L1A_mapprj.list

    echo "	Finished mapprojecting" | tee -a $logFile
}

run_asp() {

    SGM=$1
    inDEM=$2
    tileSize=$3
    sceneName=$4
    now=$5
    L1Adir=$6
	 logFile=$7

    cd $L1Adir

    corrKern=7
    subpixKern=7

    #runDir=corr${corrKern}_subpix${subpixKern}

    echo "[4] Working on: $sceneName " | tee -a $logFile
	 echo "Checking for PC.tif ..." | tee -a $logFile
    echo "Input coarse DEM = ${inDEM}" | tee -a $logFile

    outPrefix=$sceneName/outASP/out

    # Stereo Run Options
    # 
    #
    par_opts="--corr-tile-size $tileSize --job-size-w $tileSize --job-size-h $tileSize --processes 18 --threads-multiprocess 10 --threads-singleprocess 32 --nodes-list=/att/gpfsfs/home/pmontesa/code/nodes_ecotone07"
    sgm_opts="--corr-tile-size $tileSize -t aster --threads=5 --xcorr-threshold -1 --corr-kernel $corrKern $corrKern --cost-mode 4 --subpixel-mode 0 --median-filter-size 3 --texture-smooth-size 13 --texture-smooth-scale 0.13"
    reg_opts="-t aster --subpixel-mode 2 --corr-kernel $corrKern $corrKern --subpixel-kernel $subpixKern $subpixKern"
    stereo_opts="$sceneName/in-Band3N_proj.tif $sceneName/in-Band3B_proj.tif $sceneName/in-Band3N.xml $sceneName/in-Band3B.xml $outPrefix $inDEM"

	 echo "	Check for ASP input"
	 inPre=$sceneName/in-Band3

	 if [ -f ${inPre}N.tif ] && [ -f ${inPre}B.tif ] && [ -f ${inPre}N.xml ] && [ -f ${inPre}B.xml ]; then
		echo "	ASP input is exists." | tee -a $logfile
	 else
		echo " Find and unzip data" | tee -a $logfile
		orderDir=`echo $(dirname $L1Adir)`/L1A_orders
		sceneArr=(${sceneName//_/ })
		sceneSearch=${sceneArr[2]}

		echo "Unzip $sceneName ..." | tee -a $logFile
		find $orderDir -name *${sceneSearch}*.zip -exec unzip -oj -d $sceneName {} \;

		echo "Running aster2asp on $sceneName ..." | tee -a $logFile
		find $sceneName -type f -name in-Band3* -exec rm -rf {} \;
      aster2asp --threads=18 ${sceneName} -o ${sceneName}/in
	 fi

    if [ ! -f ${inPre}N_proj.tif ] || [ ! -f ${inPre}B_proj.tif ] ; then

        echo "[3] MAPPROJECT SCENE..." | tee -a $logfile
		  find $sceneName -type f -name in-Band3*_proj.tif -exec rm -rf {} \;
        run_mapprj $inDEM $sceneName $pixResFineSize $logFile

    fi
	 if gdalinfo ${inPre}N_proj.tif && gdalinfo ${inPre}B_proj.tif; then
       echo "[3] MAPPROJECT ALREADY COMPLETE." | tee -a $logfile
    else
       echo "[3] MAPPROJECT RE-DO on SCENE..." | tee -a $logFile
		 find $sceneName -type f -name in-Band3*_proj.tif -exec rm -rf {} \;
       run_mapprj $inDEM $sceneName $pixResFineSize $logFile
    fi
	 
    # Run stereo with the map-projected images
    #
    if [ ! -f $outPrefix-PC.tif ]; then

        echo "No PC.tif file -- determine which stereo algorithm to run ..." | tee -a $logFile
        echo "Tile size = ${tileSize}" | tee -a $logFile

        if $SGM; then

            echo "Stereo mode SGM or MGM = ${SGM}" | tee -a $logFile

            find $sceneName/outASP -type f -name out* -exec rm -rf {} \;

            echo "   Running stereo with SGM mode ..." | tee -a $logFile
            #parallel_stereo  --stereo-algorithm 1 $par_opts $sgm_opts $stereo_opts
            stereo  --stereo-algorithm 1 $sgm_opts $stereo_opts
            echo "   Finished stereo from SGM mode." | tee -a $logFile

            if [ ! -f $outPrefix-PC.tif ]; then

					 echo "SGM didnt work, now stereo mode is MGM" | tee -a $logFile

                find $sceneName/outASP -type f -name out* -exec rm -rf {} \;

                echo "   Running stereo with MGM mode b/c SGM failed to create a PC.tif ..." | tee -a $logFile
                stereo  --stereo-algorithm 2 $sgm_opts $stereo_opts
                echo "   Finished stereo from MGM mode." | tee -a $logFile
            fi

            if [ -f $outPrefix-PC.tif ]; then    
					echo "   Stereo successful from SGM or MGM mode." | tee -a $logFile
				else
					echo "   Stereo NOT successful from SGM or MGM mode." | tee -a $logFile
					SGM=false
            fi

        fi

		  if ! $SGM; then
            echo "   SGM and MGM failed to produce a PC.tif file. Running stereo ..." | tee -a $logFile
            #parallel_stereo $par_opts $reg_opts $stereo_opts
            stereo $reg_opts $stereo_opts
				echo "   Finished stereo." | tee -a $logFile
        fi
    fi


    echo " Try to create the final DEM and ortho'd Pan" | tee -a $logFile
    if [ -f $outPrefix-PC.tif ]; then

        echo "  PC.tif exists, now check for hillshade and colormap ..." | tee -a $logFile

        if [ ! -f $outPrefix-DEM-hlshd-e25.tif ]; then

            echo "  No hillshade, running point2dem..." | tee -a $logFile

            echo "[5] Running fine point2dem on $sceneName/outASP/$runDir ..."  | tee -a $logFile

            point2dem --threads=6 -r earth $outPrefix-PC.tif -o $outPrefix --orthoimage $outPrefix-L.tif
				echo "  Finished point2dem." | tee -a $logFile

            echo "[6] Running hillshade on $sceneName ..." | tee -a $logFile

            hillshade $outPrefix-DEM.tif -o $outPrefix-DEM-hlshd-e25.tif -e 25
		  else
            echo "   Hillshade exists." | tee -a $logFile
        fi

        if [ ! -f $outPrefix-DEM-clr-shd.tif ]; then
        		echo "  Now creating colormap ..." | tee -a $logFile
            colormap $outPrefix-DEM.tif -s $outPrefix-DEM-hlshd-e25.tif -o $outPrefix-DEM-clr-shd.tif --colormap-style /att/gpfsfs/home/pmontesa/code/color_lut_hma.txt
		  else
            echo "   Colormap exists." | tee -a $logFile
        fi

        echo "  Create output VRT files: CLR, DEM, DRG..." | tee -a $logFile
        gdal_translate -of VRT ${L1Adir}/$outPrefix-DEM-clr-shd.tif ${L1Adir}_out/clr/${sceneName}_CLR.vrt &
        gdal_translate -of VRT ${L1Adir}/$outPrefix-DRG.tif ${L1Adir}_out/drg/${sceneName}_DRG.vrt &
        gdal_translate -of VRT ${L1Adir}/$outPrefix-DRG.tif ${L1Adir}_out/dsm/${sceneName}_DEM.vrt &

        gdaladdo -r average ${L1Adir}/$outPrefix-DEM-clr-shd.tif 2 4 8 16 &
        gdaladdo -r average ${L1Adir}/$outPrefix-DRG.tif 2 4 8 16 &
        gdaladdo -r average ${L1Adir}/$outPrefix-DEM.tif 2 4 8 16 &
        echo "[7] Finished processing ${sceneName}." | tee -a $logFile

    else
        echo "   No PC.tif file !!! DEM not created." | tee -a $logFile
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

batchLogStem=${topDir}/logs/${batch}_${hostN}.log

# Read in sceneList of AST L1A scenes from an orderZip
while read -r sceneName; do
    sceneName=`echo $(basename $sceneName)`

	 sceneLog=${batchLogStem}_${sceneName}

    # ---- Pixel sizes
    # 10m
    pixResFineSize=0.0000898315284119

    # Stereo corr & job tile size for SGM run
    # tilesize^2 * 300 / 1e9 = RAM needed per thread
    tileSize=512
    SGM=true

    #_____Function calls_____

    #run_mapprj $inDEM $sceneName $now $pixResFineSize
    run_asp $SGM $inDEM $tileSize $sceneName $now $topDir/L1A $sceneLog


done < ${batch}_${hostN}



