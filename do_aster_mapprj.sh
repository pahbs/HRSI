#!/bin/bash
#
# [1] Check for / create aster camera model
# [2] Check for / Mapproject
################################
#_____Function Definitions_____
################################
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
            mapproject --threads=5 --tr $pixResFineSize $inDEM $sceneName/in-Band3N.tif $sceneName/in-Band3N.xml $sceneName/in-Band3N_proj.tif &
            mapproject --threads=5 --tr $pixResFineSize $inDEM $sceneName/in-Band3B.tif $sceneName/in-Band3B.xml $sceneName/in-Band3B_proj.tif &
    fi
    echo "<><><><><>"$'\r' >> ${sceneName}_${now}.log
}
################################

# Check that other dirs from previous unzip have mapprojected files
#
topDir=/att/nobackup/pmontesa/ASTER
HMA_GDEM=/att/gpfsfs/userfs02/ppl/pmontesa/projects/HiMAT/hma_dem/HMA_ASTGTM2_pct100.tif
# 10m
pixResFineSize=0.0000898315284119
now="$(date +'%Y%m%d')"

cd $topDir/L1A

for sceneName in AST_L1A_003*
    do
        if [ -d $sceneName ]
            then
                sceneName=`echo ${sceneName%/}`
                if [ -f "${sceneName}/in-Band3N.tif" ]
                    then
                        echo "ASP input files exists already."
                    else
                        echo "Running aster2asp on $sceneName ..."
                        aster2asp --threads=18 ${sceneName} -o ${sceneName}/in
                fi
                        if [ -f "${sceneName}/in-Band3N_proj.tif" ]
                    then
                        echo "Mapprojected files exists already."
                    else
                        echo "Running mapproject on $sceneName ..."
                        run_mapprj $HMA_GDEM $sceneName $now $pixResFineSize &
                fi
        fi

done