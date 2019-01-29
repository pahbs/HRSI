#!/bin/bash
#
# Read in a GDB of ADAPT's NGA footprints
# Select subset with an SQL statement, return a subset shpfile (<GDB name>_sqlsel/<GDB name>.shp)
# Dissolve the selection by:
# catid ---> return a shp of catids
# pairname ---> return a shp of pairnams
# This shp of catids can be used to feed into validpairs, or be joined with other DG footprint shps as needed.

# subset_mos_gdb.sh nga_inventory_02_20_2017.gdb "60 73 5 179" stereo

function footprints_adapt () {
    t_start=$(date +%s)

    # Read in a GDB
    # Return a shapefile dissolved to CATID strips that fit the SQL selection criteria

    # Subset and Dissolve the GDB
    main_dir=$1
    in_file=$2
    lat_min=$3
    lat_max=$4
    lon_min=$5
    lon_max=$6
    STEREO=$7    
    cc_thresh=0.5
    echo; echo "Type of data (stereo/mono): ${STEREO}"; echo 

    # SELECTION CRITERIA (SELECT from GDB)
    if [[ $in_file == *"nga_inventory"* ]] || [[ $in_file == *"redux"* ]] ; then

        if [[ $in_file == *"12_13_2017"* ]]; then
            dissolve_field1=CATALOG_ID
        else
            dissolve_field1=catalog_id
        fi

        to_dir=$main_dir

        pair_info_field=pairname
        sunelev_field=SUN_ELEV
        cloud_field=CLOUDCOVER
        sensor_field=SENSOR
        prodcode_field=PROD_CODE
        offnadir_field=OFF_NADIR
        centerY_field=CENT_LAT
        centerX_field=CENT_LONG
        satazimuth_field=''
        date_rcvd=RCVD_DATE
        date_added=ADDED_DATE
        acqdate_field=ACQ_DATE

        sql_sel="SELECT $dissolve_field1, $pair_info_field, $sunelev_field, $cloud_field, $date_added, "
        sql_sel+="$acqdate_field, "
        sql_sel+="$sensor_field, $prodcode_field, $offnadir_field, $centerY_field, $centerX_field "
        sql_sel+="FROM ${in_file%.*} "
        sql_sel+="WHERE "

        if [ "$STEREO" = stereo ] ; then
            sql_sel+="($pair_info_field IS NOT NULL OR $pair_info_field LIKE '') AND "
        else
            STEREO=mono
            sql_sel+="($pair_info_field IS NULL OR $pair_info_field LIKE '') AND "
        fi
        sql_sel+="($centerY_field > $lat_min AND $centerY_field < $lat_max) AND "
        sql_sel+="($centerX_field > $lon_min AND $centerX_field < $lon_max) AND "
        sql_sel+="$prodcode_field like '%P1BS%' AND "
        sql_sel+="$cloud_field < $cc_thresh"
        
        # SQLite doesnts support EXTRACT, MONTH or DATEPART
        #sql_sel+=" AND (MONTH($acqdate_field) > 5 AND MONTH($acqdate_field) < 10)"
        # https://www.sqltutorial.org/sql-date-functions/extract-month-from-date-sql/
        # syntax problem
        #sql_sel+=" AND ( strftime('%m', $acqdate_field) > 5 AND strftime('%m', $acqdate_field) < 10 )"
        
        sql_sel_lo=" AND $sql_sel AND $sunelev_field =< 25"
        sql_sel_hi=" AND $sql_sel AND $sunelev_field >= 30"

    fi


    # [1] SQL SELECTION
    cd $to_dir
    echo "Current dir:"; echo "$PWD" ; echo

    sqlsel_dir=${in_file%.*}_sqlsel
    sqlsel_file=${sqlsel_dir}/${in_file%.*}_sqlsel.shp
    mkdir -p $sqlsel_dir

  	    echo; echo "[1] Selecting from $in_file ..."; echo
        echo "SQL Selection:";
        #echo $sql_sel; echo
        echo $sql_sel | awk -F"WHERE" '{print $1}'
        echo "WHERE"
        echo $sql_sel | awk -F"WHERE" '{print $2}'

        echo; which gdalinfo; which ogr2ogr
        echo; echo "Version of ogr2ogr:"; ogr2ogr --version

        echo; echo "Creating: $sqlsel_file ..."
        cmd="ogr2ogr -overwrite $sqlsel_file $in_file -f 'ESRI Shapefile' -dialect FileGDB -sql \"$sql_sel\""
        echo $cmd ; eval $cmd ; echo

    # [2] DISSOLVE...

    cd $to_dir/${in_file%.*}_sqlsel
    in_file=${in_file%.*}_sqlsel.shp

    bbox_str=${lat_min}_${lat_max}_${lon_min}_${lon_max}
    if [[ "$bbox_str" = *"-"* ]] ; then
        bbox_str=$(echo $bbox_str | tr '-' 'n')
    fi

    dis_file=${in_file%.*}_${STEREO}_catid_${bbox_str}.shp
    dis_file_pair=${in_file%.*}_${STEREO}_pairname_${bbox_str}.shp
    #rm -v ${in_file%.*}_catid_dis.*

    if [ -e $in_file ]; then

        echo; echo "[2] Running ogr2ogr to dissolve..."; echo

        sql_sel_catid_dis="SELECT GUnion(geometry), Count(*), $dissolve_field1, $pair_info_field, $sunelev_field, $cloud_field, $date_added, "
        sql_sel_catid_dis+="$acqdate_field, "
        sql_sel_catid_dis+="$sensor_field, $prodcode_field, $offnadir_field
                FROM ${in_file%.*}
                GROUP BY $dissolve_field1"

        sql_sel_pairname_dis="SELECT GUnion(geometry), Count(*), $dissolve_field1, $pair_info_field, $sunelev_field, $cloud_field, $date_added, "
        sql_sel_pairname_dis+="$acqdate_field, "
        sql_sel_pairname_dis+="$sensor_field, $prodcode_field, $offnadir_field "
        sql_sel_pairname_dis+="FROM ${dis_file%.*} "
        ##sql_sel_pairname_dis+="WHERE (MONTH($acqdate_field) > 5 AND MONTH($acqdate_field) < 10) "
        sql_sel_pairname_dis+="GROUP BY $pair_info_field"

        echo "Current dir:"; echo "$PWD" ; echo

        echo "Sourcing default GDAL; To dissolve using ogr2ogr you need the default GDAL 2+ ..."
        #source $HOME/.bashrc
        #source $HOME/init_evars

        cmd="$HOME/code/sqlite_fix_new.env"; echo $cmd; eval $cmd
        echo; which gdalinfo; which ogr2ogr; echo ; echo "Version of ogr2ogr:"; ogr2ogr --version ; echo

        # ...on CATID
        echo "SQL Selection for CATID dissolve:"; echo $sql_sel_catid_dis; echo
        echo; echo "Creating: $dis_file ..."
        cmd="ogr2ogr -overwrite $dis_file $in_file -f 'ESRI Shapefile' -dialect sqlite -sql \"$sql_sel_catid_dis\""
        echo $cmd ; eval $cmd ; echo

        # ...on PAIRNAME
        if [ "$STEREO" = stereo ] ; then
            echo "SQL Selection for PAIRNAME dissolve:"; echo $sql_sel_pairname_dis; echo
            echo; echo "Creating: $dis_file_pair ..."
            cmd="ogr2ogr -overwrite $dis_file_pair $dis_file -f 'ESRI Shapefile' -dialect sqlite -sql \"$sql_sel_pairname_dis\""
            echo $cmd ; eval $cmd ; echo
        fi

        UPDATE_FIELDS=false

        if [ "$UPDATE_FIELDS" = true ] ; then
            echo; echo "[3] UPDATE fields..."; echo
            for field in AQC_MONTH AQC_DOY ; do
                cmd="ogrinfo $dis_file -dialect SQLite -sql \"ALTER TABLE ${dis_file%.*} ADD COLUMN $field integer(3)\""
                #ogrinfo $dis_file -dialect SQLite -sql "UPDATE ${dis_file%.*} SET $field = MONTH($acqtime_field)"
                echo $cmd ; eval $cmd
            done
        fi
        rm -v ${in_file%.*}.*
    else
        echo; echo "[2] Dissolve failed. Selection did not return a subset shapefile to dissolve. You're done."
    fi
    t_end=$(date +%s)
    t_diff=$(expr "$t_end" - "$t_start")
    t_diff_hr=$(printf "%0.4f" $(echo "$t_diff/3600" | bc -l ))

    echo; date
    echo "Total processing time in hrs: ${t_diff_hr}"
    echo "-------------------------------"

}


# Run the function

# Example of call:
# pupsh "hostname ~ 'ecotone08'" "subset_mos_gdb.sh 20181227 \"60 73 5 179\" stereo"
#
# 'canon' used from 06_04_2018 onward; previous one is 05_09_2018
# seems like YYYYMMDD format is now what works..

main_dir=$NOBACKUP/userfs02/arc/ASC_Footprints

gdb_date=${1}                  #10_04_2018

gdb_file=nga_inventory_canon${gdb_date}.gdb
zip_file=nga_inventory_canon${gdb_date}.zip

NGA_foot_dir=/att/pubrepo/NGA/INDEX/Footprints/current

mkdir -p $main_dir/logs
hostN=`/bin/hostname -s`
scriptN=`basename "$0"`
now="$(date +'%Y%m%d%H%M%S')"

s_n_w_e=$2    # "60 73 5 179"
STEREO=$3     # stereo or mono

logfile=$main_dir/logs/${scriptN}_${hostN}_${now}_nga_inventory_canon${gdb_date}.log
from_dir=${NGA_foot_dir}/${gdb_date}/geodatabase

# Copy the zip of the gdb to $NOBACKUP
if [ ! -d ${main_dir}/${gdb_file%.*}/${gdb_file} ] ; then
    echo; echo "Running rsync..."; echo
    mkdir -p ${main_dir}/${gdb_file%.*}
    rsync -avs ${from_dir}/${zip_file} ${main_dir}/${gdb_file%.*}  #_${gdb_date}
    
    echo; echo "Running unzip..."; echo
    unzip -u ${main_dir}/${gdb_file%.*}/${zip_file} -d ${main_dir}/${gdb_file%.*}
fi

echo; echo "Running ogr2ogr to query GDB and get ADAPT footprints..." ; echo
cmd="footprints_adapt ${main_dir}/${gdb_file%.*} $gdb_file ${2} ${3}"
echo $cmd
eval $cmd | tee -a $logfile

