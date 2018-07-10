#!/bin/bash
# 
# Multi-res Filtering of ASP Point Clouds for estimating Forest Canopy Heights
# 
# formerly "test_point2dem.sh"
# 
# Example call:
# pupsh "hostname ~ 'ecotone05'" "do_dem_filt-TEST.sh WV01_20110912_10200100157A5A00_1020010015E8D800 1 4 5 2 true true true true"
# or
# pmontesa@ecotone05:~$ do_dem_filt-TEST.sh WV01_20110912_10200100157A5A00_1020010015E8D800 1 4 5 2 true true true true

pairname=$1
res_fine=$2			# the fine pixel res for DEMs
res_coarse=$3		# the coarse pixel res for DEMs
max_slope=$4		# the max slope above which pixels will be masked out

#List of filters for which a DEM will be produced
filt_list="min max 80-pct"	# nmad median count

res_list="$res_fine $res_coarse"

# Larger for denser forests, but will introduce errors on slopes
search_rad=$5		# Search radius (# pixels) used for point2dem filtering 

#Format for output file naming
search_rad_frmt=$(echo $search_rad | awk '{printf("%02d", $1)}')

PUBREPO=$6		#Input data in /att/pubrepo/DEM/hrsi_dsm ? 
DO_P2D=$7		#Do the point2dem block of this script?
DO_DZ=$8		#Do the masking & differencing block?
DO_SHADE=$9		#Do the shaded relief block?

p2d_extent="${10}"

# If PUBREPO is false
batch_name=${10}

# Percentage by which the resolution of the DEM is reduced to produce a slope raster
reduce_pct_slope=50

#-------------------------------------------------------------------------------------

if [ "${PUBREPO}" = true ] ; then
    main_dir=/att/pubrepo/DEM/hrsi_dsm
else
    main_dir=/att/gpfsfs/briskfs01/ppl/pmontesa/outASP/${batch_name}
fi
echo; echo "Main dir: ${main_dir}"; echo

workdir=/att/gpfsfs/briskfs01/ppl/pmontesa/outASP_TEST/test_do_dem_filt/${pairname}
mkdir -p $workdir

if [ -e ${main_dir}/${pairname}/out-strip-PC.tif ] ; then
    ln -sf ${main_dir}/${pairname}/out-strip-PC.tif $workdir/out-PC.tif
else
    ln -sf ${main_dir}/${pairname}/out-PC.tif $workdir/out-PC.tif
fi

# Make symlinks to original data
ln -sf ${main_dir}/${pairname}/out-DEM_1m.tif $workdir/out-DEM_1m.tif
ln -sf ${main_dir}/${pairname}/out-DEM_24m.tif $workdir/out-DEM_24m.tif
ln -sf ${main_dir}/${pairname}/${pairname}_ortho.tif $workdir/${pairname}_ortho.tif

proj=$(utm_proj_select.py ${workdir}/out-DEM_24m.tif)
echo $proj

if [ "${proj}" = "" ] || [ -e "${proj}" ] ; then
    echo "Proj string empty: ${workdir}/out-DEM_24m.tif . Exiting."
    exit 1
fi

p2d_opts="--t_srs \"$proj\""
p2d_opts+=" --t_projwin $p2d_extent"
p2d_opts+=" --remove-outliers --remove-outliers-params 75.0 3.0"
p2d_opts+=" --threads 4 --nodata-value -99"
##p2d_opts+=" --rounding-error 0.125"
p2d_opts+=" --search-radius-factor $search_rad"

if [ "${DO_P2D}" = true ]; then

	# Do the various point2dem filtering in parallel
    cmd_list=''
    
    for filt in $filt_list ; do
        #for res in $res_list ; do
            cmd=''
            if [ "$filt" = "min" ] ; then
                res=4
            else
                res=1
            fi
            cmd+="point2dem --filter $filt $p2d_opts --tr $res -o $workdir/out_${res}m $workdir/out-PC.tif ; "
            echo $cmd
            cmd_list+=\ \'$cmd\'
        #done
    done

    eval parallel -verbose -j 4 ::: $cmd_list

    # Handle nmad
    for filt in $filt_list ; do
    if [ "$filt" = "nmad" ] ; then
        
        # Rename the nmad and do overviews in parallel
        cmd_list=''

        #for res in $res_list ; do
            cmd=''
            out_nmad=${workdir}/${pairname}_sr${search_rad_frmt}_${res_fine}m-nmad-DEM.tif
            mv  $workdir/out_${res_fine}m-nmad-DEM.tif $out_nmad
            cmd="gdaladdo -ro -r average ${out_nmad} 2 4 8 16 32 64 ; "
            cmd_list+=\ \'$cmd\'
        #done

        eval parallel -verbose -j 2 ::: $cmd_list
    fi
    done
fi

if [ "${DO_DZ}" = true ] ; then

    cmd_list=''
    #for res in $res_list ; do
        for dsm in $workdir/out_${res_coarse}m-min-DEM.tif $workdir/out_${res_fine}m-max-DEM.tif $workdir/out_${res_fine}m-80-pct-DEM.tif ; do
            cmd=''
            #if [ ! -e "${dsm%.*}_${reduce_pct_slope}pct_slope_mask.tif" ] ; then
                echo "Compute masked slopes to DSM: $(basename ${dsm})"
                cmd+="compute_masked_slope.py ${dsm} -max_slope ${max_slope} -reduce_pct ${reduce_pct_slope} ; "
            #fi
            cmd_list+=\ \'$cmd\'
        done
    #done

    eval parallel -verbose -j 6 ::: $cmd_list

    cmd_list=''
    #for res in $res_list ; do
        for dsm in $workdir/out_${res_coarse}m-min-DEM.tif $workdir/out_${res_fine}m-max-DEM.tif $workdir/out_${res_fine}m-80-pct-DEM.tif ; do
            cmd=''
            #if [ ! -e "${dsm%.*}_masked.tif" ] ; then
                echo "Apply masked slopes to DSM: $(basename ${dsm})"
                cmd+="apply_mask.py ${dsm} ${dsm%.*}_${reduce_pct_slope}pct_slope_mask.tif ; "
            #fi
            cmd_list+=\ \'$cmd\'
        done
    #done

    eval parallel -verbose -j 3 ::: $cmd_list

    cmd_list=''
    #for res in $res_list ; do

        tail="-DEM_masked"

        for min_dsm in $workdir/out_${res_fine}m-min${tail}.tif $workdir/out_${res_coarse}m-min${tail}.tif; do
            for max_dsm in $workdir/out_${res_fine}m-max${tail}.tif $workdir/out_${res_fine}m-80-pct${tail}.tif ; do
                
                out_dz_tmp=${min_dsm%.*}_$(basename ${max_dsm%.*})_dz_eul.tif
                out_dz=${workdir}/${pairname}_sr${search_rad_frmt}_$(echo $(basename ${out_dz_tmp}) | sed -e 's/out_//g' | sed -e "s/${tail}//g")
                
                echo "Min dsm, Max dsm: $(basename $min_dsm) , $(basename $max_dsm)"
                echo $(basename $out_dz)
                
                cmd=''
                #if [ ! -e "${out_dz}" ] ; then
                    cmd+="compute_dz.py -tr 2 $min_dsm $max_dsm ;"
                    echo; echo $cmd; echo
                    cmd+="mv ${out_dz_tmp} ${out_dz} ; "
                    cmd+="gdaladdo -ro -r average ${out_dz} 2 4 8 16 32 64 ; "
                #fi

                cmd_list+=\ \'$cmd\'
            done
        done
    #done

    eval parallel -verbose -j 2 ::: $cmd_list
fi












if [ "${DO_SHADE}" = true ]; then

    # Color Shaded Relief Generation
    mean=$(gdalinfo -stats $workdir/out_${res_coarse}m-min-DEM.tif | grep MEAN | awk -F '=' '{print $2}')
    stddev=$(gdalinfo -stats $workdir/out_${res_coarse}m-min-DEM.tif | grep STDDEV | awk -F '=' '{print $2}')

    min=$(echo $mean $stddev | awk '{print $1 - $2}')
    max=$(echo $mean $stddev | awk '{print $1 + $2}')

    cmd_list=''
    echo; echo "Check for color-shaded reliefs..."; echo
    
    for stat in $filt_list ; do
        for res in $res_list ; do
            dem_fn=$workdir/out_${res}m-${stat}-DEM.tif
        	cmd=''
            #if [ ! -e ${dem_fn%.*}_color_hs.tif.ovr ]; then
                rm -f ${dem_fn%.*}_color_hs*

        	    cmd+="color_hs.py $dem_fn -clim $min $max -hs_overlay -alpha .8 ; "
                cmd+="gdaladdo -ro -r average ${dem_fn%.*}_color_hs.tif 2 4 8 16 32 64 ; "
                cmd+="rm ${dem_fn%.tif}_color.tif ; "
        	    cmd_list+=\ \'$cmd\'
            #else
                echo "Finished: ${dem_fn%.*}_color_hs.tif"
            #fi
        done
    done

    if [[ ! -z $cmd_list ]]; then
        echo; echo "Do all colorshades and hillshades in parallel"; echo
        eval parallel -verbose -j 8 ::: $cmd_list
    fi

    # Do overviews for non-color shade files
    cmd_list=''
    for stat in nmad ; do
        for res in $res_list ; do
            cmd=''
            cmd+="gdaladdo -ro -r average $workdir/out_${res}m-${stat}-DEM.tif 2 4 8 16 32 64 ; "
        	cmd_list+=\ \'$cmd\'
        done
    done

    eval parallel -verbose -j 6 ::: $cmd_list
fi

# Make links to completed dZ tifs in a top level 'chm' dir for easy access
for dz in `ls ${workdir}/${pairname}*dz_eul.tif` ; do
    rm ${workdir}/chm/${pairname}*dz_eul.tif
	ln -sf $dz $(dirname ${workdir})/chm
done

rm -v $workdir/out_*dz_eul*
rm -v $workdir/*slope*.tif
rm -v $workdir/*masked.tif
rm -v $workdir/*.vrt
rm $workdir/*color.tif
rm $workdir/*ramp.txt
rm -rf /tmp/magick-*
