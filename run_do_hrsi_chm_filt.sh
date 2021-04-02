#!/bin/bash
#
# 3DSI Boreal Forest Canopy Height
# paul.m.montesano@nasa.gov
# Estimate canopy height by differencing DEMs from point clouds filtered to estimate ground (min) and canopy (max)
# Run do_hrsi_chm_filt.sh in a loop
#
# For 3DSI runs:
# pupsh "hostname ~ 'crane101'" "run_do_hrsi_chm_filt.sh v2 list_v2_bc"
# For Will's AGU work:
# pupsh "hostname ~ 'crane101'" "run_do_hrsi_chm_filt.sh test batch01"
# For validation with LVIS:
# pupsh "hostname ~ 'crane|ngaproc02'" "run_do_hrsi_chm_filt.sh v2 list_val"


script_name=do_hrsi_chm_filt.sh
INPUT=${1:-'v1'}

if [ "${INPUT}" = "v1" ] ; then
    main_dir=/att/pubrepo/DEM/hrsi_dsm
    work_dir=/att/gpfsfs/briskfs01/ppl/pmontesa/chm_work/hrsi_chm_filt                   #outASP_TEST/test2_do_dem_filt
elif [ "${INPUT}" = "v2" ] ; then
    main_dir=/att/pubrepo/DEM/hrsi_dsm/v2
    work_dir=/att/gpfsfs/briskfs01/ppl/pmontesa/chm_work/hrsi_chm_sgm_filt
elif [ "${INPUT}" = "v2_val" ] ; then
    main_dir=/att/pubrepo/DEM/hrsi_dsm/v2
    work_dir=/att/gpfsfs/briskfs01/ppl/pmontesa/chm_work/hrsi_chm_sgm_filt
elif [ "${INPUT}" = "test" ] ; then
    ##main_dir=/att/gpfsfs/briskfs01/ppl/pmontesa/outASP/veg_mode/SGM_5_999_0
    ##work_dir=/att/gpfsfs/briskfs01/ppl/pmontesa/chm_work/hrsi_chm_sgm_filt
    main_dir=/att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/projects/3dsi/DSM_ground/${2}
    work_dir=$main_dir
elif [ "${INPUT}" = "v2_projrepo" ] ; then
    # TMP location for v2 SGM output
    main_dir=/att/projrepo/hrsi_dsm/v2
    work_dir=/att/gpfsfs/briskfs01/ppl/pmontesa/chm_work/hrsi_chm_sgm_filt
else
    echo "Incorrect INPUT dir. Exiting."
    exit 1
fi

mkdir -p $work_dir

cd $work_dir

batch=${work_dir}/${2}
FCOV=${3:-'dense'}

# in lat/lon
xmin=$4
ymin=$5
xmax=$6
ymax=$7

#Use these for now
res_fine=1			# the fine pixel res for DEMs
res_coarse=4		# the coarse pixel res for DEMs
max_slope=10		# the max slope above which pixels will be masked out

echo; echo "Resolutions for filtering: ${res_fine}m (fine for canopy), ${res_coarse}m (coarse for ground)" ; echo
if [ "$FCOV" = "sparse" ] ; then
    echo "Sparse forest cover filtering."
    search_rad_list="2"
else
    echo "Dense forest cover filtering."
    search_rad_list="2 5"

    if [[ "$INPUT" = "test" ]] || [[ "$INPUT" = *"val"* ]] ; then
        echo; echo "Runs for which you want 2,5,10"
        echo "     1. Will's DSM_ground AGU work or"
        echo "     2. validation work with LVIS" ; echo
        search_rad_list+=" 10"
        ##batch=${work_dir}/list_${2} #<- I did this to run WIlls stuff, but i should instead reconfigure the input to have it conform to what the script wants..
    fi 
fi


hostN=`/bin/hostname -s`
echo "List:" ; echo ${batch}_${hostN} ; echo
length_list=$(cat ${batch}_${hostN} | wc -l) 
num_pairname=0

num_parjobs=2
if [[ "$hostN" == *"ecotone"* ]] ; then
   num_parjobs=2
fi

mkdir -p ${work_dir}/logs

while read -r pairname; do
    echo ; echo "Pairname: $pairname"
    echo "List: ${batch}_${hostN}"
    num_pairname=$[$num_pairname + 1]
    echo "Working on ${num_pairname} of ${length_list} total pairnames..."; echo

    if [ ! -d "${main_dir}/${pairname}"  ] ; then
        echo "Not found in $main_dir"

        if [ "$main_dir" == "/att/pubrepo/DEM/hrsi_dsm/v2" ] ; then
            INPUT="v2_projrepo"
            main_dir="/att/projrepo/hrsi_dsm/v2" 
        elif [ "$main_dir" == "/att/projrepo/hrsi_dsm/v2" ] ; then
            INPUT="v2"
            main_dir="/att/pubrepo/DEM/hrsi_dsm/v2"
        else
            echo; echo "Main dir not recognized: $main_dir . Moving to next pairname in list..." ; echo
            continue
        fi
        echo "Try another dir: $main_dir"
    fi
    if [[ ! -d "${main_dir}/${pairname}"  ]] || [[ ! -e "${main_dir}/${pairname}/out-PC.tif" ]] ; then
        echo; echo "Can't find $pairname or its PC file in either dir. Moving to next pairname in list..." ; echo
        continue
    fi

    #from QGIS   lx      ly       rx	uy
    #          "439903 7174636 446392 7179954"
    # t_projwin wants this: ulx, uly, lrx, lry
    # Bonanza Creek test site
    #p2d_extent="439903 7184954 446392 7174636"
    # Ary Mas
    # 101.7943 72.3898 102.4882 72.5515
    #rm -r ${main_dir}/${pairname}

    mkdir -p ${work_dir}/${pairname}

    if [ ! -z "$xmin" -a "$xmin" != " " ]; then
        prj_utm=$(utm_proj_select.py ${main_dir}/${pairname}/out-DEM_1m.tif)
    
        # Create bbox shp for a specific run
        cmd="ogr2ogr -clipdst ${xmin} ${ymin} ${xmax} ${ymax} -t_srs EPSG:4326 ${work_dir}/${pairname}/bbox_tmp.shp /att/briskfs/pmontesa/userfs02/arc/world_adm0.shp"
        echo $cmd ; eval $cmd

        # Reproject bbox to utm of input DEM
        cmd="ogr2ogr -t_srs \"${prj_utm}\" ${work_dir}/${pairname}/bbox_utm.shp ${work_dir}/${pairname}/bbox_tmp.shp"
        echo $cmd ; eval $cmd

        # Run a function in my bash_profile to convert utm bbox to the format needed for -t_prjwin
        source ~/.bash_profile
        p2d_extent=$(ogr_extent ${work_dir}/${pairname}/bbox_utm.shp)
        
        echo $p2d_extent
        rm ${work_dir}/${pairname}/bbox*
    else
        echo "No bbox specified."
        p2d_extent=""
        #p2d_extent="439903 7184954 446392 7174636"
    fi

    cmd_list=''
    
    # Loop over sr list and fill a cmd_list
    for search_rad in ${search_rad_list} ; do
        out_dz=${work_dir}/${pairname}/${pairname}_sr0${search_rad}_${res_coarse}m-sr0${search_rad}-min_${res_fine}m-sr0${search_rad}-max_dz_eul.tif
        echo; echo $out_dz ; echo

        if [ ! -e ${out_dz} ] ; then        

            cmd=''
            cmd="${script_name} ${pairname} ${res_fine} ${res_coarse} ${max_slope} ${search_rad} ${INPUT} true true \"$p2d_extent\""
            if [ "$INPUT" = "test" ]; then
                cmd+=" ${2}"
            fi 
             
            echo $cmd
        
            #eval $cmd | tee ${work_dir}/logs/${script_name%.*}_${hostN}_${pairname}_searchrad${search_rad}.log
            cmd_list+=\ \'$cmd\'
        fi
    done

    # If cmd_list isnt empty, do slopemasking of DEM once, here
    if [[ ! -z "$cmd_list" ]] ; then

        echo; echo "Do the slopemasking for upcoming run(s)." ; echo
        ln -sf ${main_dir}/${pairname}/out-DEM_4m.tif ${work_dir}/${pairname}/out-DEM_4m.tif 2> /dev/null
        ln -sf ${main_dir}/${pairname}/out-DEM_24m.tif ${work_dir}/${pairname}/out-DEM_24m.tif 2> /dev/null

        eval parallel -j 2 'slopemask_dem.py {} -max_slope ${max_slope}' ::: ${work_dir}/${pairname}/out-DEM_24m.tif ${work_dir}/${pairname}/out-DEM_4m.tif

        echo ; echo "Run HRSI CHM script in parallel: once for each search radius DEM set (min and max)" ; echo
        echo $cmd_list

        # Not sure how to tee this to a logfile
        # num jobs depends on VM

        eval parallel -j $num_parjobs ::: $cmd_list
    else
        echo "Yes. CHMs exist for: $pairname"; echo "$(date)" ; echo "Script: $0" ; echo "List: ${batch}_${hostN}" ; echo
 
    fi

    echo "Finished ${num_pairname} of ${length_list} total pairnames."; echo

    # Remove the slope.tif, slopemasked.tif, and masked DEMs
    rm ${work_dir}/${pairname}/*{slope,masked}.tif 2> /dev/null

done < ${batch}_${hostN}
echo "----Done with `basename ${0}`---- $(date)" ; echo