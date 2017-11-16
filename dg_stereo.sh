#!/bin/bash
#
# Stereo, point2dem, hillshades, & orthoimages for individual stereopairs on ADAPT
# example of call:
# pairname=WV01_20140603_102001002F42A400_1020010031EBEF00
# dg_stereo.sh $pairname
# or
# dg_stereo.sh $pairname $nodeslist /stt/pubrepo/DEM/hrsi_dsm "$$crop" true 2 3
# or
# pupsh "hostname ~ 'ecotone16'" "dg_stereo_par.sh /att/pubrepo/DEM/hrsi_dsm/list_pairname"
#
# Dependencies for this script that wants python scripts to be sitting in your path dir:
#	utm_proj_select.py		my edit from pygeotools; run this: pip install --user pygeotools
#   color_hs.py   		    run this: pip install --user imview
#   query_db_catid.py		my script that returns the ADAPT dir of images of given catid
#
# Hardcoded:
# The 'nodelist' txt file specifies your available VMs (nodes). It is only needed if you want to run parallel_stereo
# eg:
# ecotone01
# ecotone02
# ecotone03

t_start=$(date +%s)

function gettag() {
    xml=$1
    tag=$2
    echo $(grep "$tag" $xml | awk -F'[<>]' '{print $3}')
}

# Args: feed in pairname, split to get catids, return ADAPT dir from query of each catid & copy symlinks to out_root/pairname, feed out_root/pairname into ntfmos.sh

# Required Args
pairname=$1

# Hardcoded vars
out_root=/att/pubrepo/DEM/hrsi_dsm
nodes=~/code/nodes_ecotone_all
ADAPT=true
run_stereo=false
rmfiles=true
compress=false

left_catid="$(echo $pairname | awk -F '_' '{print $3}')"
right_catid="$(echo $pairname | awk -F '_' '{print $4}')"

# Optional Args are for stereogrammetry. Likely not needed.
crop="$3"
sgm=$4      #true or false
sa=$5		#if sgm is true, then use 1 for sgm or 2 for mgm
cm=$6       #cost mode for stereo

if [ -z "$sgm" ]; then
	sgm=false
fi

ncpu=$(cat /proc/cpuinfo | egrep "core id|physical id" | tr -d "\n" | sed s/physical/\\nphysical/g | grep -v ^$ | sort | uniq | wc -l)

parallel_point2dem=false
if [ "$ncpu" -gt "12" ] ; then
    parallel_point2dem=true
fi

# Stereogrammetry
out=${out_root}/${pairname}/out
stereo_opts=''
stereo_args=''
sgm_opts=''
tile_size=256

if [ -e ${out}-strip-PC.tif ]; then
	mv ${out}-strip-PC.tif ${out}-PC.tif
fi
if [ -e ${out}-strip-DEM.tif ]; then
	mv ${out}-strip-DEM.tif ${out}-DEM_native.tif
fi

#Set entry point based on contents of outdir
if [ -e ${out}-PC.tif ]; then
    e=5
elif [ -e ${out}-F.tif ]; then
    e=4
elif [ -e ${out}-RD.tif ]; then
    e=3
elif [ -e ${out}-D.tif ]; then
    e=2
elif [ -e ${out}-R_sub.tif ]; then
    e=1
else
    e=0
fi

#Set in_left and in_right consistent with expected output of dg_mosaic
in_left=${out_root}/${pairname}/${left_catid}.r100.tif
in_right=${out_root}/${pairname}/${right_catid}.r100.tif

#Set the name of the output orthoimage
ortho_ext=_ortho.tif
out_ortho=${out_root}/${pairname}/${pairname}${ortho_ext}

if [ ! -e $in_left ] || [ ! -e $in_right  ] ; then
    mkdir -p ${out_root}/${pairname}
    if [ ! -e $out_ortho ] ; then
        for catid in $left_catid $right_catid ; do
            cmd=''
            echo; echo "Querying ngadb, putting the symlinks catid ${catid} in ${out_root}/${pairname}"; echo
            cmd+="time query_db_catid.py $catid -out_dir ${out_root}/${pairname} ; "
            cmd_list+=\ \'$cmd\'
        done

        # Do the ADAPT db querying in parallel
        if $ADAPT ; then
            eval parallel --delay 2 -verbose -j 2 ::: $cmd_list
        fi
    fi
fi

if [ ! -e "${out}-PC.tif" ] ; then
    echo; echo "Running wv_correct and dg_mosaic to create:"; echo "${in_left}"; echo "${in_right}"
    ntfmos.sh ${out_root}/${pairname}
fi

if [ ! -e $in_left ]; then
    in_proj_xml=$(echo $(ls ${out_root}/${pairname}/*P1BS*.xml | head -1))
else
    in_proj_xml=${in_left%.*}.xml
fi

echo; echo "Get proj and native_res from ${in_proj_xml} ..."
# Get UTM proj from XML
# DEPENDENCY! run this to install library --> pip install --user pygeotools
proj=$(utm_proj_select.py ${in_proj_xml})

if grep -q MEANPRODUCTGSD $in_proj_xml ; then
    native_res=$(printf '%.2f' $(gettag $in_proj_xml 'MEANPRODUCTGSD'))
else
    native_res=$(printf '%.2f' $(gettag $in_proj_xml 'MEANCOLLECTEDGSD'))
fi

echo "Projection: ${proj}"
echo "Native res: ${native_res}"

if [ "$e" -lt "5" ] && [ -e $in_left ] && [ -e $in_right ] ; then
    stereo_opts+="-t dg"
    stereo_opts+=" --alignment-method AffineEpipolar"
    stereo_opts+=" --corr-timeout 1660"
    stereo_opts+=" --subpixel-kernel 9 9"
    stereo_opts+=" --fill-holes-max-size 15"
    stereo_opts+=" --erode-max-size 15"
    stereo_opts+=" --individually-normalize"
    stereo_opts+=" --tif-compress LZW"
    #stereo_opts+=" --job-size-w $tile_size --job-size-h $tile_size"

    if [ ! -z "$crop" ]; then
        stereo_opts+=" $crop"
    fi

    stereo_args+=" $in_left $in_right ${in_left%.*}.xml ${in_right%.*}.xml"
    stereo_args+=" ${out}"

    if [ ! -z $sgm ] && [ $sgm = true ] ; then
        # SGM stereo runs. Not applicable for our DISCOVER processing
        if [ ! -z "$sa" ]; then
            sgm_opts+=" --stereo-algorithm $sa"
        else
            sgm_opts+=" --stereo-algorithm 2"
        fi
        if [ ! -z "$cm" ]; then
            sgm_opts+=" --cost-mode $cm"
        else
            sgm_opts+=" --cost-mode 3"
        fi
        sgm_opts+=" --corr-kernel 5 5"
        sgm_opts+=" --corr-tile-size $tile_size"
        sgm_opts+=" --xcorr-threshold -1"
        sgm_opts+=" --subpixel-mode 0"
        sgm_opts+=" --median-filter-size 3"
        sgm_opts+=" --texture-smooth-size 13"
        sgm_opts+=" --texture-smooth-scale 0.13"
        sgm_opts+=" --threads 6"
        sgm_opts+="$stereo_opts"

        echo; date; echo;
        eval time stereo -e $e $sgm_opts $stereo_args
    else
        # ADAPT processing needs these
        par_opts="--threads-singleprocess $ncpu"
        par_opts+=" --processes $ncpu"
        pc_merge_opts="--threads $ncpu --tile-size=$tile_size,$tile_size -o ${out}-PC.tif"
        if [ ! -z $nodes ]; then
            par_opts+=" --nodes-list $nodes"
        fi

        # DISCOVER processing needs these.
        stereo_opts+=" --corr-kernel 21 21"
        stereo_opts+=" --subpixel-mode 2"
        stereo_opts+=" --filter-mode 1"
        stereo_opts+=" --cost-mode 2"

        if $run_stereo ; then
            if $ADAPT ; then
                echo; echo "nice -n5 parallel_stereo $par_opts $stereo_opts $stereo_args"; echo
                eval time nice -n5 parallel_stereo -e $e $par_opts $stereo_opts $stereo_args
                eval time pc_merge $pc_merge_opts ${out}*/out*PC.tif
            else
                echo; echo "stereo $stereo_opts $stereo_args"; echo
                eval time stereo -e $e $stereo_opts $stereo_args
            fi
        fi
    fi
fi
if [ ! -e "${out}-PC.tif" ] ; then
    echo; echo "Stereogrammetry unsuccessful. Exiting."
    exit 1
else
    echo; echo "Stereo point-cloud file exists. Check for DEMs."

    stats_res=24
    mid_res=4
    fine_res=1

    stats_dem=${out}-DEM_${stats_res}m.tif
    mid_dem=${out}-DEM_${mid_res}m.tif
    fine_dem=${out}-DEM_${fine_res}m.tif

    # DEM Generation
    cmd_list=''
    dem_ndv=-99

    base_dem_opts=" --remove-outliers --remove-outliers-params 75.0 3.0"
    base_dem_opts+=" --threads 4"
    base_dem_opts+=" --t_srs \"$proj\""

    for dem_res in $stats_res $mid_res $fine_res ; do
        dem_opts="$base_dem_opts"
        if [ ! -e ${out}-DEM_${dem_res}m.tif ]; then
            echo "Creating DEM at ${dem_res}m ..."
            dem_opts+=" --nodata-value $dem_ndv"
    	    dem_opts+=" --tr $dem_res"
            if [ "$dem_res" = "$stats_res" ] ; then
                dem_opts+=" --dem-hole-fill-len 10"
            fi
            dem_opts+=" -o ${out}_${dem_res}m"

          echo; date; echo;
          echo point2dem $dem_opts ${out}-PC.tif
          echo

          if $parallel_point2dem ; then
              cmd=''
              cmd+="time point2dem $dem_opts ${out}-PC.tif; "
              cmd+="mv ${out}_${dem_res}m-DEM.tif ${out}-DEM_${dem_res}m.tif; "
              cmd_list+=\ \'$cmd\'
          else
              eval time point2dem $dem_opts ${out}-PC.tif
              mv ${out}_${dem_res}m-DEM.tif ${out}-DEM_${dem_res}m.tif
          fi
        fi
    done

    if [[ ! -z $cmd_list ]] ; then
       	if (( $ncpu > 15 )) ; then
            njobs=3
        else
            njobs=2
        fi
        eval parallel -verbose -j $njobs ::: $cmd_list
    fi

    # Color Shaded Relief Generation
    mean=$(gdalinfo -stats $stats_dem | grep MEAN | awk -F '=' '{print $2}')
    stddev=$(gdalinfo -stats $stats_dem | grep STDDEV | awk -F '=' '{print $2}')

    min=$(echo $mean $stddev | awk '{print $1 - $2}')
    max=$(echo $mean $stddev | awk '{print $1 + $2}')

    cmd_list=''
    for dem in $stats_dem $mid_dem ; do
    	
    	cmd=''
        if [ ! -e ${dem%.*}_color_hs.tif.ovr ]; then
            rm -f ${dem%.*}_color_hs.tif
		    # DEPENDENCY ---> pip install --user imview
    	    cmd+="time color_hs.py $dem -clim $min $max -hs_overlay -alpha .8; "
    	    cmd_list+=\ \'$cmd\'
        fi
    done
    if [[ ! -z $cmd_list ]]; then
        echo; echo "Do all colorshades and hillshades in parallel"; echo
        eval parallel -verbose -j 10 ::: $cmd_list
        rm ${out}*color.tif
    fi

    ortho_opts="--nodata-value 0"

    map_opts="$ortho_opts"
    map_opts+=" -t rpc"
    map_opts+=" --num-processes $ncpu"

    #If both in_left and in_right exist, then catid mosaics are complete, and in_left can be ortho'd
    # else no mosiacs done, in_left is an xml used for proj and native_res; need indiv scenes indiv ortho'd then dem_mosaic
    if [ ! -e ${out_ortho} ] ; then
        if [ -e ${in_left} ] && [ -e ${in_right} ] ]; then

            echo; echo "Mapproject at ${res}m ${in_left} onto ${stats_dem}"; echo
            map_opts=" --tr $native_res"
            map_args="$stats_dem $in_left ${in_left%.*}.xml ${out_ortho}"
            time mapproject $map_opts $map_args

        else
            echo; echo "Mapproject each indiv NTF onto ${stats_dem}"; echo
            echo; echo "Get ADAPT dir with imagery to mapproject"; echo

            left_catid_dir="$(query_db_catid.py ${left_catid} -out_dir ${out_root}/${pairname})"
            ntf_list=$(ls $left_catid_dir | grep -e "${left_catid}" | grep -i P1BS | egrep 'ntf|tif' | grep -v 'corr')

            cmd_list=''
            for ntf in $ntf_list ; do
                ntf_fn=${left_catid_dir}/${ntf}
                indiv_ortho=${out_root}/${pairname}/${ntf%.*}${ortho_ext}
                map_args="$stats_dem ${ntf_fn} ${ntf_fn%.*}.xml $indiv_ortho"
    	        echo $ntf_fn
    	        cmd=''
    	        cmd+="time mapproject $map_opts $map_args; "
                cmd_list+=\ \'$cmd\'
            done
            echo; echo "Do orthos for each P1BS scene running mapproject in parallel"; echo
            eval parallel -verbose -j 6 ::: $cmd_list

            echo; echo "Do dem_mosaic at native res of orthos"; echo
            echo; echo "dem_mosaic --tr $native_res --threads $ncpu `ls ${out_root}/${pairname}/*${ortho_ext}` -o ${out_root}/${pairname}/${pairname}"; echo

            time dem_mosaic --tr $native_res --threads $ncpu `ls ${out_root}/${pairname}/*${ortho_ext}` -o ${out_root}/${pairname}/${pairname}
            mv ${out_root}/${pairname}/${pairname}-tile-0.tif ${out_ortho}
            echo "Building overviews in background for:"; echo; echo "$out_ortho"
            gdaladdo -ro -r average ${out_ortho} 2 4 8 16 32 64 &

        fi
    fi
    if [ ! -e ${out_ortho%.*}_${mid_res}m.tif.ovr ] ; then
        echo; echo "Simple gdal_translate to coarsen native_res ortho..."; echo
        gdal_translate -tr ${mid_res} ${mid_res} ${out_ortho} ${out_ortho%.*}_${mid_res}m.tif
        gdaladdo -ro -r average ${out_ortho%.*}_${mid_res}m.tif 2 4 8 16 32 64 &
    fi
    if $ADAPT ; then
        echo; echo "Writing symlinks"; echo
        for i in $out_ortho ${out_ortho%.*}_${mid_res}m.tif ; do
            ln -sfv ${i} ${out_root}/_ortho/$(basename ${i})
        done
        for i in $stats_dem $mid_dem $fine_dem ; do
            dembase=$(basename ${i})
            ln -sfv ${i} ${out_root}/_dem/${pairname}_${dembase:4}
            
            if [ -e ${dembase%.*}_color_hs.tif ] ; then
                colorbase=${dembase%.*}_color_hs.tif
                ln -sfv ${i%.*}_color_hs.tif ${out_root}/_color_hs/${pairname}_${colorbase:4}
            fi
        done
    fi
    echo; echo "Removing individual ortho scenes..."; echo
    for i in $(ls ${out_root}/${pairname}/*P1BS*ortho.tif); do
        rm -v ${i%.*}*
    done

    if [ -e ${out}-DEM_native.tif ]; then
	 	mv ${out}-DEM_native.tif ${out}-strip-DEM.tif
    fi
    if [ -e ${out}-PC.tif ]; then
	 	mv ${out}-PC.tif ${out}-strip-PC.tif
    fi
    if [ -e ${out}-L.tif ]; then
	 	mv ${out}-L.tif ${out}-strip-L.tif
    fi

    if $rmfiles ; then
        if $ADAPT ; then
            echo; echo "Removing intermediate dirs"
            rm -rf ${out}*/
        fi
        echo; echo "Removing intermediate files"
        rm ${out}-log-stereo_parse*.txt
        rm ${out_root}/${pairname}/*.r100.tif
        rm ${out_root}/${pairname}/out.*
        for i in sub.tif Mask.tif .match .exr center.txt ramp.txt; do
            rm -v ${out}-*${i}
        done
        for i in F L R RD D GoodPixelMap DEM-clr-shd DEM-hlshd-e25 DRG; do
            if [ -e ${out}-${i}.tif ]; then
                rm -v ${out}-${i}.tif
            fi
            if [ -e ${out}-strip-${i}.tif ]; then
                rm -v ${out}-strip-${i}.*
            fi
        done
    fi
fi

t_end=$(date +%s)
t_diff=$(expr "$t_end" - "$t_start")
t_diff_hr=$(printf "%0.4f" $(echo "$t_diff/3600" | bc -l ))

echo; date
echo "Total processing time in hrs: ${t_diff_hr}"