#!/bin/bash
# 
# Multi-res Filtering of ASP Point Clouds for estimating Forest Canopy Heights
#  
# formerly "test_point2dem.sh"
# 
# Example call:
# pupsh "hostname ~ 'ecotone05'" "do_dem_filt-TEST2.sh WV01_20110912_10200100157A5A00_1020010015E8D800 1 4 5 2 true true true true"
# or
# pmontesa@ecotone05:~$ do_dem_filt-TEST2.sh WV01_20110912_10200100157A5A00_1020010015E8D800 1 4 5 2 true true true true

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

PUBREPO=$6		#Input data in /att/pubrepo/DEM/hrsi_dsm ? 
DO_P2D=$7		#Do the point2dem block of this script?
DO_DZ=$8		#Do the masking & differencing block?

p2d_extent="${9}"

# If PUBREPO is false
batch_name=${10}

# Percentage by which the resolution of the DEM is reduced to produce a slope raster
reduce_pct_slope=100

# Output res for dz files
out_dz_res=2

#-------------------------------------------------------------------------------------

if [ "${PUBREPO}" = true ] ; then
    main_dir=/att/pubrepo/DEM/hrsi_dsm
else
    main_dir=/att/gpfsfs/briskfs01/ppl/pmontesa/outASP/${batch_name}
fi

echo; echo "Main dir: ${main_dir}"; echo

workdir=/att/gpfsfs/briskfs01/ppl/pmontesa/outASP_TEST/test2_do_dem_filt/${pairname}
workdir_old=/att/gpfsfs/briskfs01/ppl/pmontesa/outASP_TEST/test_do_dem_filt/${pairname}
mkdir -p $workdir

if [ -e ${main_dir}/${pairname}/out-strip-PC.tif ] ; then
    ln -sf ${main_dir}/${pairname}/out-strip-PC.tif $workdir/out-PC.tif
else
    ln -sf ${main_dir}/${pairname}/out-PC.tif $workdir/out-PC.tif
fi

# Make symlinks to original data needed
ln -sf ${main_dir}/${pairname}/out-DEM_4m.tif $workdir/out-DEM_4m.tif
ln -sf ${main_dir}/${pairname}/out-DEM_24m.tif $workdir/out-DEM_24m.tif
ln -sf ${main_dir}/${pairname}/${pairname}_ortho.tif $workdir/${pairname}_ortho.tif

proj=$(utm_proj_select.py ${workdir}/out-DEM_24m.tif)
echo $proj

if [ "${proj}" = "" ] || [ -e "${proj}" ] ; then
    echo "Proj string empty: ${workdir}/out-DEM_24m.tif . Exiting."
    exit 1
fi

p2d_opts=''
p2d_opts="--t_srs \"$proj\""
if [ -z "$p2d_extent" ] ; then
    echo "No projwin."
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
        
        if [ -e $workdir/out_${res}m-sr${search_rad_frmt}-${filt}-DEM.tif ] ; then
            echo "DEM exists: out_${res}m-sr${search_rad_frmt}-${filt}-DEM.tif"
        else
            echo "Get the $filt DEM at ${res}m using search radius ${search_rad}" 
            cmd+="point2dem --filter $filt $p2d_opts --tr $res -o $workdir/out_${res}m-sr${search_rad_frmt} $workdir/out-PC.tif ; "
            echo $cmd
            cmd_list+=\ \'$cmd\'
        fi
    done
    if [ -z "$cmd_list" ] ; then
        echo "DEMs already exist."
    else
        eval parallel -j 4 ::: $cmd_list
    fi
fi

if [ "${DO_DZ}" = true ] ; then

    echo; echo "Get a slope masks from 2 resolutions..."; echo
    rp_multi=100
    cmd_list=""
    if [ ! -e "$workdir/out-DEM_24m_slopemasked.tif" ] ; then
        cmd="slopemask_dem.py $workdir/out-DEM_24m.tif -max_slope ${max_slope} ; "
        cmd_list+=\ \'$cmd\'
    fi
    if [ ! -e "$workdir/out-DEM_4m_slopemasked.tif" ] ; then
        cmd="slopemask_dem.py $workdir/out-DEM_4m.tif -max_slope ${max_slope} ; "
        cmd_list+=\ \'$cmd\'
    fi
    if [ -z "$cmd_list" ] ; then
        echo "Slopemasked DEMs already exist."
    else
        eval parallel -j 2 ::: $cmd_list
    fi

    # Set intermediate tail; ground (min) and canopy (max) DSMs; and out_dz file name
    tail="-DEM_masked_masked"
    min_dsm=$workdir/out_${res_coarse}m-sr${search_rad_frmt}-min${tail}.tif
    max_dsm=$workdir/out_${res_fine}m-sr${search_rad_frmt}-max${tail}.tif
    out_dz_tmp=${min_dsm%.*}_$(basename ${max_dsm%.*})_dz_eul.tif
    out_dz=${workdir}/${pairname}_sr${search_rad_frmt}_$(echo $(basename ${out_dz_tmp}) | sed -e 's/out_//g' | sed -e "s/${tail}//g" )
    
    echo; echo $(basename ${out_dz}) ; echo
    if [ ! -e "$out_dz" ] ; then

        # Apply slope masks
        cmd_list=''

        for filt in $filt_list ; do 
            for res in $res_list ; do
                cmd=''
                dsm=$workdir/out_${res}m-sr${search_rad_frmt}-${filt}-DEM.tif   
                if [ -e "$dsm" ] && [ ! -e "${dsm%.*}_masked_masked.tif" ] ; then
                    echo "Apply slope masks from 2 resolutions..."
                    # These two must be in serial
                    cmd+="apply_mask_pm.py -extent intersection ${dsm} $workdir/out-DEM_24m_slopemasked.tif ; "
                    cmd+="apply_mask_pm.py -extent intersection ${dsm%.*}_masked.tif $workdir/out-DEM_4m_slopemasked.tif ; "
                    echo; echo $cmd ; echo
                    cmd_list+=\ \'$cmd\'  
                fi
                
            done   
        done

        eval parallel -j 2 ::: $cmd_list
        
        cmd_list=''   

        #for min_dsm in $workdir/out_${res_coarse}m-sr${search_rad_frmt}-min${tail}.tif; do
        #for max_dsm in $workdir/out_${res_fine}m-sr${search_rad_frmt}-max${tail}.tif ; do                
                
                echo; echo "Min dsm, Max dsm: $(basename $min_dsm) , $(basename $max_dsm)"
                echo $(basename $out_dz)
                
                compute_dz.py -tr $out_dz_res $min_dsm $max_dsm
                mv ${out_dz_tmp} ${out_dz}
                gdaladdo -ro -r average ${out_dz} 2 4 8 16 32 64

        #done
        #done
    fi
fi

echo; echo "Done with ${pairname}" ; echo

# Make links to completed dZ tifs in a top level 'chm' dir for easy access
for dz in `ls ${workdir}/${pairname}*dz_eul.tif` ; do
    #rm ${workdir}/chm/${pairname}*dz_eul.tif
	ln -sf $dz $(dirname ${workdir})/chm
done

#rm -v $workdir/out_*dz_eul*
rm ${workdir}/*slope.tif
#rm ${workdir}/*_masked.tif
#rm -v $workdir/*.vrt

rm -rf /tmp/magick-*
