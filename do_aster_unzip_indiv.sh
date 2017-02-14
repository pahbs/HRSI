#!/bin/bash
#
# Download and unzip ASTER L1A data from earthdata.nasa.gov polygon searches

topDir=/att/nobackup/pmontesa/ASTER

## Check email for "LPDAAC ECS Order Notification Order ID: *****"
## ENTER THE PullDir NUMBER --> note, this is not the ORDERID or the REQUEST ID from the email
#orderZip=030385335976111.zip

orderZip=$1
now="$(date +'%Y%m%d')"
orderDir=L1A_orders

#
# Get data to the L1A_orders dir
#
cd $topDir/$orderDir
# http://e4ftl01.cr.usgs.gov/PullDir/$orderZip

#
# Unzip the order to L1A dir
#
L1Adir=L1A
unzip -oj -d ${topDir}/${L1Adir} $orderZip
cd ${topDir}/${L1Adir}

# Unzip indiv scene zips to their own dirs in L1A dir
#

# ____FUNCTIONS_____
join_by() { local IFS="$1"; shift; echo "$*"; }

echo "`date`"$'\r' >> $topDir/$orderDir/${orderZip%.zip}_${now}.log
echo "User `whoami` started the script."$'\r' >> $topDir/$orderDir/${orderZip%.zip}_${now}.log
echo $'\r' >> $topDir/$orderDir/${orderZip%.zip}_${now}.log
echo "Order: " $orderZip$'\r' >> $topDir/$orderDir/${orderZip%.zip}_${now}.log

for file in AST_L1A_003*.zip
    do
        # Remove last two elements of filename, creating filenames like this: 'AST_L1A_00308312000062611'
        # Make array of elements in the filename string; eg 'AST_L1A_00308312000062611_20170202145004_32249' ---> [AST L1A 00308312000062611 20170202145004 32249]
        IFS='_' read -ra fileArr <<< "$file"
        fileName=`join_by _ ${fileArr[@]:0:3}`

        if [ ! -d "${fileName}" ]
            then
                unzip -d "${fileName}" $file
                echo "$file"$'\r' >> $topDir/$orderDir/${orderZip%.zip}_${now}.log
                echo "${fileName}" >> $topDir/$orderDir/${orderZip%.zip}_${now}.list
            else
                echo "Dir exists:"
                echo "${fileName}"
        fi
    rm $file
    rm *.met
    rm checksum_report
done