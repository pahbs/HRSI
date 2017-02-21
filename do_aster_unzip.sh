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

    orderDir=$1
    orderLink=$2
    topDir=$3
    L1Adir=$4
    pixResFineSize=$5
    inDEM=$6

    # Change order link to order zip
    tmp=http://e4ftl01.cr.usgs.gov/PullDir/
    orderZip=`echo ${orderLink/${tmp}}`

    cd $topDir/$orderDir
	 echo "Order zip is:"
	 echo ${orderZip}
    now="$(date +'%Y%m%d')"

    #
    # Unzip the order to L1A dir
    #

    unzip -oj -d ${topDir}/${L1Adir} ${orderZip}
    cd ${topDir}/${L1Adir}

    # Unzip indiv scene zips to their own dirs in L1A dir

    echo "`date`"$'\r' >>                               $topDir/$orderDir/${orderZip%.zip}_${now}.log
    echo "User `whoami` started the script."$'\r' >>    $topDir/$orderDir/${orderZip%.zip}_${now}.log
    echo $'\r' >>                                       $topDir/$orderDir/${orderZip%.zip}_${now}.log
    echo "Order: " $orderZip$'\r' >>                    $topDir/$orderDir/${orderZip%.zip}_${now}.log


    for file in AST_L1A_003*.zip
        do
            # Remove last two elements of filename, creating filenames like this: 'AST_L1A_00308312000062611'
            # Make array of elements in the filename string; eg 'AST_L1A_00308312000062611_20170202145004_32249' ---> [AST L1A 00308312000062611 20170202145004 32249]
            IFS='_' read -ra fileArr <<< "$file"
            sceneName=`join_by _ ${fileArr[@]:0:3}`

				 # Format date like this YYYmmdd
				 sceneDate=`echo ${sceneName:11:8} | sed -E 's/(.{2})(.{2})(.{4})/\3\1\2/'`
				 # Rename scene to this: AST_20000831_00308312000062611
				 sceneName=AST_${sceneDate}_`join_by _ ${fileArr[@]:2:1}`

            if [ ! -f "${sceneName}" ]
                then

                    echo "[1] UNZIP SCENE..."
                    unzip -oj -d "${sceneName}" $file
                    echo "$file"$'\r' >>                $topDir/$orderDir/${orderZip%.zip}_${now}.log

                    echo "[2] CREATE CAMERA MODEL ..."
                    echo "${sceneName}"
                    if [ -f "${sceneName}/in-Band3N.tif" ]
                        then
                            echo "ASP input files exists already."
                        else
                            echo "Running aster2asp on $sceneName ..."$'\r' >> ${sceneName}_${now}.log
                            aster2asp --threads=18 ${sceneName} -o ${sceneName}/in
                    fi

                else
                    echo "Dir exists:"
                    echo "${sceneName}"
            fi
				 if [ ! -f "${sceneName}/in-Band3N_proj.tif" ]
					  then
							echo "[3] MAPPROJECT EACH SCENE..."
                    run_mapprj $inDEM $sceneName $now $pixResFineSize &
							echo "${sceneName}" >>               $topDir/$orderDir/${orderZip%.zip}_${now}.list
					  else
							echo "Mapprojected files exists already."
				 fi
        rm $file
        rm *.met
        rm checksum_report
    done

    cd $topDir/$orderDir
}


run_mapprj() {
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
            mapproject --threads=5 --tr $pixResFineSize $inDEM $sceneName/in-Band3N.tif $sceneName/in-Band3N.xml $sceneName/in-Band3N_proj.tif
            mapproject --threads=5 --tr $pixResFineSize $inDEM $sceneName/in-Band3B.tif $sceneName/in-Band3B.xml $sceneName/in-Band3B_proj.tif
    fi
    echo "<><><><><>"$'\r' >> ${sceneName}_${now}.log
}

join_by() { local IFS="$1"; shift; echo "$*"; }

################################
#
# VARIABLES FOR RUN
#
# Launch like this example:
# pupsh "hostname ~ 'himat101'" "/att/nobackup/pmontesa/ASTER/do_aster_unzip.sh 'order_2008_2016.list' 'L1A_orders/orders_2008_2016_cc0_25'"

# Cmd Line Args
orderList=$1
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

# Go to dir with VM chunk
cd ${topDir}/${orderDir}

# Get hostname
hostN=`/bin/hostname -s`

echo ${orderList}_${hostN}

# Loop over lines in List
#
while read -r line; do
	 echo "Line in orderList file is:"
	 echo $line
    # Use '&' to set function to run in background
    preprocess $orderDir $line $topDir $L1Adir $pixResFineSize $inDEM

done < ${orderList}_${hostN}


#
# Footprint the ASTER L1A projected data
#
dir=${topDir}/${L1Adir}
cd  ~/code
look='in-Band3N_proj.tif'
srs='geog'
rastExt='.tif'
rm *metadata.txt

python -c "import mos_v2 as mos_v2; mos_v2.foot_tiles('$dir', '$look', '$srs', '$rastExt')"

cd $dir


