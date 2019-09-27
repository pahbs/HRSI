#!/bin/bash
# 
# Filtering of ASP Point Clouds for estimating Forest Canopy Heights from HRSI stereopairs
#  
# see "run_do_hrsi_chm_filt.sh" for 3DSI batch runs
# 
# Example call:
# pupsh "hostname ~ 'ecotone05'" "do_hrsi_chm_filt.sh WV01_20150505_102001003DB7FB00_102001003D03CA00 1 4 10 2 true true true"
# or
# pmontesa@ecotone05:~$ do_dem_filt-TEST2.sh WV01_20110912_10200100157A5A00_1020010015E8D800 1 4 5 2 true true true

pairname=$1
res_fine=$2			# the fine pixel res for DEMs
res_coarse=$3		# the coarse pixel res for DEMs
max_slope=$4		# the max slope above which pixels will be masked out

#List of filters for which a DEM will be produced
filt_list="min max"	# 80-pct nmad median count

res_list="$res_fine $res_coarse"

# Larger for denser forests, but will introduce errors on slopes
search_rad=$5		# Search radius (# pixels) used for point2dem filtering 

#Format for output file naming
search_rad_frmt=$(echo $search_rad | awk '{printf("%02d", $1)}')

INPUT=$6		#Input data in /att/pubrepo/DEM/hrsi_dsm ? 
DO_P2D=$7		#Do the point2dem block of this script?
DO_DZ=$8		#Do the masking & differencing block?

p2d_extent=${9:-''}

# If INPUT is not either v1 or v2 this will be used
batch_name=${10}

# Percentage by which the resolution of the DEM is reduced to produce a slope raster
reduce_pct_slope=100

out_dz_res=2 

echo; echo "Script call:"
echo "${0} ${1} ${2} ${3} ${4} ${5} ${6} ${7} ${8} ${9} ${10}" ; echo
echo "Processing CHMs from pairname: $pairname"
echo "Processing parameters for point-cloud filtering"
echo "Pixel res. for local max: $res_fine"
echo "Pixel res. for local min: $res_coarse"
echo "Search radius (in pixels): $search_rad"
echo "Max slope above which pixels will be masked out: $max_slope"
echo "Output res. for CHM files: $out_dz_res"

if [ "${INPUT}" = "v1" ] || [ "${INPUT}" = "true" ] ; then
    main_dir=/att/pubrepo/DEM/hrsi_dsm
    work_dir=/att/gpfsfs/briskfs01/ppl/pmontesa/outASP_TEST/test2_do_dem_filt
elif [ "${INPUT}" = "v2" ] ; then
    main_dir=/att/pubrepo/DEM/hrsi_dsm/v2
    work_dir=/att/gpfsfs/briskfs01/ppl/pmontesa/chm_work/hrsi_chm_sgm_filt
elif [ "${INPUT}" = "test" ] ; then
    main_dir=/att/gpfsfs/briskfs01/ppl/pmontesa/outASP/veg_mode/SGM_5_999_0
    work_dir=/att/gpfsfs/briskfs01/ppl/pmontesa/chm_work/hrsi_chm_sgm_filt
else
    main_dir=/att/gpfsfs/briskfs01/ppl/pmontesa/outASP/${batch_name}
    work_dir=/att/gpfsfs/briskfs01/ppl/pmontesa/chm_work/${batch_name}
fi

pairname_dir=$work_dir/$pairname

echo "Main dir: ${main_dir}"
echo "Work dir: ${work_dir}"

mkdir -p $work_dir
mkdir -p ${work_dir}/chm

# This is important so you dont overwrite the original PC.tif when you are writing the CHMs to the same dir as the input...
if [ "${INPUT}" = "v1" ] || [ "${INPUT}" = "v2" ] || [ "${INPUT}" = "test" ]; then

    if [ -e ${main_dir}/${pairname}/out-strip-PC.tif ] ; then
        ln -sf ${main_dir}/${pairname}/out-strip-PC.tif $pairname_dir/out-PC.tif 2> /dev/null
    else
        ln -sf ${main_dir}/${pairname}/out-PC.tif $pairname_dir/out-PC.tif 2> /dev/null
    fi

    # Make symlinks to original data needed
    ln -sf ${main_dir}/${pairname}/out-DEM_4m.tif $pairname_dir/out-DEM_4m.tif 2> /dev/null
    ln -sf ${main_dir}/${pairname}/out-DEM_24m.tif $pairname_dirr/out-DEM_24m.tif 2> /dev/null
    ln -sf ${main_dir}/${pairname}/${pairname}_ortho.tif $pairname_dir/${pairname}_ortho.tif 2> /dev/null
fi

proj=$(utm_proj_select.py ${pairname_dir}/out-DEM_24m.tif)
echo; echo "Projection: $proj" ; echo

if [ "${proj}" = "" ] || [ -e "${proj}" ] ; then
    echo; echo "Proj string empty: ${pairname_dir}/out-DEM_24m.tif . Exiting." ; echo
    exit 1
fi

p2d_opts=''
p2d_opts="--t_srs \"$proj\""
if [ -z "$p2d_extent" ] ; then
    echo "No specified subset window for output CHM." ; echo
else
    p2d_opts+=" --t_projwin $p2d_extent"
fi

p2d_opts+=" --remove-outliers --remove-outliers-params 75.0 3.0"
p2d_opts+=" --threads 4 --nodata-value -99"
p2d_opts+=" --search-radius-factor $search_rad"

if [ "${DO_P2D}" = true ]; then

    cmd_list=''
    
    for filt in $filt_list ; do

        cmd=''
        if [ "$filt" = "min" ] ; then
            res=${res_coarse}
        else
            res=${res_fine}
        fi
        
        if [ -e $pairname_dir/out_${res}m-sr${search_rad_frmt}-${filt}-DEM.tif ] && [ -e $pairname_dir/out_${res}m-sr${search_rad_frmt}-DEM_complete ]  ; then
            echo "DEM exists: out_${res}m-sr${search_rad_frmt}-${filt}-DEM.tif"
        else
            echo "Get the $filt DEM at ${res}m using search radius ${search_rad}" 
            cmd+="point2dem --filter $filt $p2d_opts --tr $res -o $pairname_dir/out_${res}m-sr${search_rad_frmt} $pairname_dir/out-PC.tif && touch $work_dir/out_${res}m-sr${search_rad_frmt}-DEM_complete ; "
            echo; echo $cmd ; echo
            cmd_list+=\ \'$cmd\'
        fi
    done
    if [ -z "$cmd_list" ] ; then
        echo "DEMs already exists."
    else
        eval parallel -j 4 ::: $cmd_list
    fi
fi


# Set intermediate tail; ground (min) and canopy (max) DSMs; and out_dz file name
tail="-DEM_masked_masked"
min_dsm=$pairname_dir/out_${res_coarse}m-sr${search_rad_frmt}-min${tail}.tif
max_dsm=$pairname_dir/out_${res_fine}m-sr${search_rad_frmt}-max${tail}.tif
out_dz_tmp=${min_dsm%.*}_$(basename ${max_dsm%.*})_dz_eul.tif
out_dz=${pairname_dir}/${pairname}_sr${search_rad_frmt}_$(echo $(basename ${out_dz_tmp}) | sed -e 's/out_//g' | sed -e "s/${tail}//g" )

if [ -e "$out_dz" ] ; then
    echo; echo "CHM already exists:" ; echo ${out_dz} ; echo
elif [[ "${DO_DZ}" = false ]] && [[ ! -e "$out_dz" ]] ; then
    echo; echo "CHM DOESNT exists, and you chose not to compute it." ; echo
elif [[ "${DO_DZ}" = true ]] && [[ ! -e "$out_dz" ]] ; then
    echo; echo "Computing CHM:" ; echo ${out_dz}
    echo "Get a slope masks from 2 resolutions..."; echo

    rp_multi=100
    cmd_list=""
    if [ ! -e "$pairname_dir/out-DEM_24m_slopemasked.tif"  ] ; then 
        cmd="slopemask_dem.py $pairname_dir/out-DEM_24m.tif -max_slope ${max_slope} ; "
        cmd_list+=\ \'$cmd\'
    fi
    if [ ! -e "$pairname_dir/out-DEM_4m_slopemasked.tif"  ] ; then
        cmd="slopemask_dem.py $pairname_dir/out-DEM_4m.tif -max_slope ${max_slope} ; "
        cmd_list+=\ \'$cmd\'
    fi
    if [ -z "$cmd_list" ] ; then
        echo "Slopemasked DEMs already exist."
    else
        eval parallel -j 2 ::: $cmd_list
    fi
    
    echo; echo $(basename ${out_dz}) ; echo


    # Apply slope masks
    cmd_list=''

    for res in $res_list ; do  
        for filt in $filt_list ; do

            cmd=''
            dsm=$pairname_dir/out_${res}m-sr${search_rad_frmt}-${filt}-DEM.tif
   
            if [ -e "$dsm" ] ; then
                echo "Apply slope masks from 2 resolutions..."
                echo "DSM: $dsm"
                # These two must be in serial
                if [[ ( ! -e "${dsm%.*}_masked_complete" ) || ( ! -e  "${dsm%.*}_masked.tif" ) ]] ; then
                    echo "Mask: out-DEM_24m_slopemasked.tif"
                    cmd+="apply_mask_pm.py -extent intersection ${dsm} $pairname_dir/out-DEM_24m_slopemasked.tif && touch ${dsm%.*}_masked_complete ; "
                fi
                if [[ ( ! -e "${dsm%.*}_masked_masked_complete" ) || ( ! -e  "${dsm%.*}_masked_masked.tif" ) ]] ; then
                    echo "Mask: out-DEM_4m_slopemasked.tif"
                    cmd+="apply_mask_pm.py -extent intersection ${dsm%.*}_masked.tif $pairname_dir/out-DEM_4m_slopemasked.tif && touch ${dsm%.*}_masked_masked_complete ; "
                fi
                echo; echo $cmd ; echo
                cmd_list+=\ \'$cmd\'  
            fi
                
        done   
    done
    # KEEP JOBS at 2!!
    eval parallel -j 2 ::: $cmd_list
    
    echo; echo "Min dsm, Max dsm: $(basename $min_dsm) , $(basename $max_dsm)"
    echo $(basename $out_dz)
             
    compute_dz.py -tr $out_dz_res $min_dsm $max_dsm
    mv ${out_dz_tmp} ${out_dz}
    gdaladdo -ro -r average ${out_dz} 2 4 8 16 32 64

    if [ -e "$out_dz" ] ; then
        echo; echo "CHM finished:" ; echo ${out_dz} ; echo
    else
        echo; echo "Fail: CHM not made for some reason." ; echo
        echo $pairname >> ${work_dir}/list_fails
    fi

fi
 
# Make links to completed dZ tifs in a top level 'chm' dir for easy access
for dz in `ls ${pairname_dir}/${pairname}*dz_eul.tif` ; do
	ln -sf $dz ${work_dir}/chm/$(basename ${dz})
done

rm ${pairname_dir}/out-*sr${search_rad_frmt}*masked.tif 2> /dev/null
rm -rf /tmp/magick-* 2> /dev/null

echo "----Done---- $(date)" ; echo
