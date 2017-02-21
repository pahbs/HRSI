#!/bin/bash
#
# [1] Download and unzip ASTER L1A data from earthdata.nasa.gov polygon searches
# [2] Create aster camera model
# [3] Mapproject
# [4] Footprint
## Check email for "LPDAAC ECS Order Notification Order ID: *****"
## ENTER THE PullDir NUMBER --> note, this is not the ORDERID or the REQUEST ID from the email
################################
#_____Function Definitions_____
################################

preprocess() {

    scenePath=$1
    topDir=$2
    L1Adir=$3
    pixResFineSize=$4
    inDEM=$5

    echo " ASTER scene is:"
    echo ${scenePath}
    now="$(date +'%Y%m%d')"

    # Get scene name of scene zip
    cd ${topDir}/${L1Adir}
    scene=$(basename $scenePath)

	 if [[ $scene == *"_L1A_"* ]]; then

		# Copy the AST_L1A zip to the L1A dir
		cp $scenePath ${topDir}/${L1Adir}

		# Remove last two elements of filename, creating filenames like this: 'AST_L1A_00308312000062611'
		# Make array of elements in the filename string; eg 'AST_L1A_00308312000062611_20170202145004_32249' ---> [AST L1A 00308312000062611 20170202145004 32249]
		IFS='_' read -ra sceneArr <<< "$scene"
		sceneName=`join_by _ ${sceneArr[@]:0:3}`

		# Format date like this YYYmmdd
		sceneDate=`echo ${sceneName:11:8} | sed -E 's/(.{2})(.{2})(.{4})/\3\1\2/'`
		# Rename scene to this: AST_20000831_00308312000062611
		sceneName=AST_${sceneDate}_`join_by _ ${sceneArr[@]:2:1}`

	 else
		 sceneName=`echo $scene`
	 fi

    if [ ! -d "${sceneName}" ]; then

        echo "[1] UNZIP SCENE..."
        unzip -oj -d "${sceneName}" $scene
        echo "$scene"$'\r' >>                ${sceneName}_${now}.log

        echo "[2] CREATE CAMERA MODEL ..."
        echo "${sceneName}"

        if [ -f "${sceneName}/in-Band3N.tif" ]; then
            echo "ASP input files exists already."
        else
            echo "Running aster2asp on $sceneName ..."$'\r' >> ${sceneName}_${now}.log
            aster2asp --threads=18 ${sceneName} -o ${sceneName}/in
        fi

    else
        echo "Dir exists:"
        echo "${sceneName}"
    fi

    if [ ! -f "${sceneName}/in-Band3N_proj.tif" ]; then

        echo "[3] MAPPROJECT SCENE..."
        run_mapprj $inDEM $sceneName $now $pixResFineSize &

    else
        if gdalinfo ${sceneName}/in-Band3N_proj.tif && gdalinfo ${sceneName}/in-Band3B_proj.tif; then
            echo "[3] MAPPROJECT ALREADY COMPLETE."
        else
            echo "[3] MAPPROJECT RE-DO on SCENE..."
            run_mapprj $inDEM $sceneName $now $pixResFineSize &
        fi
	 fi
    rm $scene
    rm *.met
    rm checksum_report

}


run_mapprj() {
    inDEM=$1
    sceneName=$2
    now=$3
    pixResFineSize=$4

    echo "Mapprojecting..."
    echo "......--------------------------------------"
    echo "[3] Running mapproject ..."$'\r' >> ${sceneName}_${now}.log
    # Map-project onto this DEM
    mapproject --threads=5 --tr $pixResFineSize $inDEM $sceneName/in-Band3N.tif $sceneName/in-Band3N.xml $sceneName/in-Band3N_proj.tif
    mapproject --threads=5 --tr $pixResFineSize $inDEM $sceneName/in-Band3B.tif $sceneName/in-Band3B.xml $sceneName/in-Band3B_proj.tif
    echo "${sceneName}" >>               AST_L1A_mapprj.list

    echo "<><><><><>"$'\r' >> ${sceneName}_${now}.log
}

join_by() { local IFS="$1"; shift; echo "$*"; }

################################
#
# VARIABLES FOR RUN
#
# Launch like this example:
# pupsh "hostname ~ 'ecotone16'" "/att/nobackup/pmontesa/ASTER/do_aster_unzip_2.sh 'scenes.list' 'L1A_orders/tmp4'"

# Cmd Line Args
sceneList=$1
orderDir=$2

#"L1A_orders/orders_2000_2007_cc0_25"


#topDir=/att/nobackup/pmontesa/ASTER
topDir=/att/gpfsfs/atrepo01/data/hma_data/ASTER
inDEM=/att/gpfsfs/userfs02/ppl/pmontesa/HiMAT/hma_dem/HMA_ASTGTM2_pct100.tif
#inDEM=/att/nobackup/pmontesa/projects/siberia/aster_dem/SIB_ASTGTM2_pct100.tif

# 10m
pixResFineSize=0.0000898315284119

# Get data to the L1A_orders dir
L1Adir=L1A

# Go to dir with the scene list
cd ${topDir}/${orderDir}

# Get hostname
hostN=`/bin/hostname -s`

echo ${sceneList}_${hostN}

# Loop over lines in List

while read -r line; do
    echo "Line in list file is:"
    echo $line
    # Use '&' to set function to run in background
    preprocess $line $topDir $L1Adir $pixResFineSize $inDEM

done < ${sceneList}_${hostN}




