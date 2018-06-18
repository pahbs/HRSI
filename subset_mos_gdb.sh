#!/bin/bash
#
# Read in a GDB of NGA footprints
# Select subset with an SQL statement, return a subset shpfile (<GDB name>_sqlsel/<GDB name>.shp)
# Dissolve the selection by:
# catid ---> return a shp of catids
# pairname ---> return a shp of pairnams
# This shp of catids can be used to feed into validpairs, or be joined with other DG footprint shps as needed.

# subset_mos_gdb.sh nga_inventory_02_20_2017.gdb "60 73 5 179" stereo

function get_dis_shp () {
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
    STEREO=$7    #true or false
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
        #acqtime_field=ACQ_TIME_Converted
        #acqtime_field=ACQ_TIME
        sensor_field=SENSOR
        prodcode_field=PROD_CODE
        offnadir_field=OFF_NADIR
        centerY_field=CENT_LAT
        centerX_field=CENT_LONG
        satazimuth_field=''
        date_rcvd=RCVD_DATE
        date_added=ADDED_DATE

        sql_sel="SELECT $dissolve_field1, $pair_info_field, $sunelev_field, $cloud_field, $date_added, "
        #sql_sel+="$acqtime_field, "
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
        #sql_sel+=" AND (MONTH($acqtime_field) > 4 AND MONTH($acqtime_field) < 10)"
        

    else

        to_dir=$main_dir/PublicMD

        dissolve_field1=catalogid
        pair_info_field=paircatid
        sunelev_field=sunelevation
        cloud_field=cloudcover
        acqtime_field=acquisitiondate
        sensor_field=sensor
        prodcode_field=productlevel
        offnadir_field=offnadir
        centerY_field=CenterY
        centerX_field=CenterX
        satazimuth_field=satazimuth

        sql_sel="SELECT
                $dissolve_field1, $pair_info_field, $sunelev_field, $cloud_field, $acqtime_field,
                $sensor_field, $prodcode_field, $offnadir_field, $centerY_field, $centerX_field
                FROM ${in_file%.*}
                WHERE ($sensor_field like '%WV%' OR $sensor_field like '%GE%') AND
                ($pair_info_field = '' OR $pair_info_field IS NULL OR $pair_info_field NOT like '%WV%' OR $pair_info_field NOT like '%GE%') AND
                ($centerY_field > $lat_min AND $centerY_field < $lat_max) AND
                ($centerX_field > $lon_min AND $centerX_field < $lon_max) AND
                $prodcode_field like '%1B%' AND
                $cloud_field < $cc_thresh"

    fi


    # [1] SQL SELECTION
    cd $to_dir
    sqlsel_dir=${in_file%.*}_sqlsel
    sqlsel_file=${sqlsel_dir}/${in_file%.*}_sqlsel.shp
    mkdir -p $sqlsel_dir

  	    echo; echo "[1] Selecting from $in_file ..."; echo

        echo "Current dir:"; echo "$PWD" ; echo
        echo "SQL Selection:"; echo $sql_sel; echo

        echo; which gdalinfo; which ogr2ogr
        echo; echo "Version of ogr2ogr:"; ogr2ogr --version

        echo; echo "ogr2ogr -overwrite $sqlsel_file $in_file -f 'ESRI Shapefile' -dialect FileGDB -sql $sql_sel"; echo
        echo; echo "Creating: $sqlsel_file ..."
        ogr2ogr -overwrite $sqlsel_file $in_file -f 'ESRI Shapefile' -dialect FileGDB -sql "$sql_sel"

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
        #sql_sel_catid_dis+="$acqtime_field, "
        sql_sel_catid_dis+="$sensor_field, $prodcode_field, $offnadir_field
                FROM ${in_file%.*}
                GROUP BY $dissolve_field1"

        sql_sel_pairname_dis="SELECT GUnion(geometry), Count(*), $dissolve_field1, $pair_info_field, $sunelev_field, $cloud_field, $date_added, "
        #sql_sel_pairname_dis+="$acqtime_field, "
        sql_sel_pairname_dis+="$sensor_field, $prodcode_field, $offnadir_field "
        sql_sel_pairname_dis+="FROM ${dis_file%.*} "
        #sql_sel_pairname_dis+="WHERE (MONTH($acqtime_field) > 4 AND MONTH($acqtime_field) < 10) "
        sql_sel_pairname_dis+="GROUP BY $pair_info_field"

        echo "Current dir:"; echo "$PWD" ; echo

        echo "Sourcing default GDAL; To dissolve using ogr2ogr you need the default gdal (2.0.2)..."
        echo "source sqlite_fix_new.env"
        #source $HOME/.bashrc
        #source $HOME/init_evars
        source $HOME/code/sqlite_fix_new.env
        echo; which gdalinfo; which ogr2ogr
        echo; echo "Version of ogr2ogr:"; ogr2ogr --version

        # ...on CATID
        echo "SQL Selection for CATID dissolve:"; echo $sql_sel_catid_dis; echo
        echo; echo "Creating: $dis_file ..."
        echo; echo "ogr2ogr -overwrite $dis_file ${in_file} -f 'ESRI Shapefile' -dialect sqlite -sql $sql_sel_catid_dis"; echo
        ogr2ogr -overwrite $dis_file $in_file -f 'ESRI Shapefile' -dialect sqlite -sql "$sql_sel_catid_dis"

        # ...on PAIRNAME
        if [ "$STEREO" = stereo ] ; then
            echo "SQL Selection for PAIRNAME dissolve:"; echo $sql_sel_pairname_dis; echo
            echo; echo "Creating: $dis_file_pair ..."
            echo; echo "ogr2ogr -overwrite $dis_file_pair $dis_file -f 'ESRI Shapefile' -dialect sqlite -sql $sql_sel_pairname_dis"; echo
            ogr2ogr -overwrite $dis_file_pair $dis_file -f 'ESRI Shapefile' -dialect sqlite -sql "$sql_sel_pairname_dis"
        fi

        UPDATE_FIELDS=false

        if [ "$UPDATE_FIELDS" = true ] ; then
            echo; echo "[3] UPDATE fields..."; echo
            for field in AQC_MONTH AQC_DOY ; do
                ogrinfo $dis_file -dialect SQLite -sql "ALTER TABLE ${dis_file%.*} ADD COLUMN $field integer(3)"
                #ogrinfo $dis_file -dialect SQLite -sql "UPDATE ${dis_file%.*} SET $field = MONTH($acqtime_field)"
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

# Arg 2: (gdb name) should look like this: nga_inventory_08_22_2017.gdb
# Args 3 4 5 6 should be lat_min lat_max lon_min lon_max
# Dan's MDS: PublicMDs_01Aug2017.gdb
#
# most recent: nga_inventory_02_20_2018.gdb
#

main_dir=$NOBACKUP/userfs02/arc/ASC_Footprints
###gdb=$1
gdb_date=${1} ###02_20_2018
NGA_foot_dir=/att/pubrepo/NGA/INDEX/Footprints/current

mkdir -p $main_dir/logs
hostN=`/bin/hostname -s`
scriptN=`basename "$0"`
now="$(date +'%Y%m%d%H%M%S')"

s_n_w_e=$2
STEREO=$3    #true or false

logfile=$main_dir/logs/${scriptN}_${hostN}_${now}_nga_inventory_${gdb_date}.log
zip_file=${NGA_foot_dir}/${gdb_date}/geodatabase/nga_inventory_${gdb_date}.zip

# Copy the zip of the gdb to $NOBACKUP
if [ ! -d ${main_dir}/$(basename ${zip_file}).gdb ] ; then
    rsync -avs ${zip_file} ${main_dir}
    # Extract
    unzip ${zip_file} -d ${main_dir}
fi

# Run the function to get the shps
get_dis_shp $main_dir nga_inventory_${gdb_date}.gdb $2 $3 | tee -a $logfile

