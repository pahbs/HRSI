#!/bin/bash
#
# Run do_dsm_filt-TEST*.sh in a loop
#
test_num=2
script_name=do_dem_filt-TEST${test_num}.sh
main_dir=/att/gpfsfs/briskfs01/ppl/pmontesa/outASP_TEST/test${test_num}_do_dem_filt

batch=${main_dir}/${1}

# in lat/lon
xmin=$2
ymin=$3
xmax=$4
ymax=$5

hostN=`/bin/hostname -s`

mkdir -p ${main_dir}/logs

while read -r pairname; do

    #from QGIS   lx      ly       rx	uy
    #          "439903 7174636 446392 7179954"
    # t_projwin wants this: ulx, uly, lrx, lry
    # Bonanza Creek test site
    #p2d_extent="439903 7184954 446392 7174636"
    # Ary Mas
    # 101.7943 72.3898 102.4882 72.5515
    #rm -r ${main_dir}/${pairname}
    mkdir -p ${main_dir}/${pairname}

    if [ ! -z "$xmin" -a "$xmin" != " " ]; then
        prj_utm=$(utm_proj_select.py /att/pubrepo/DEM/hrsi_dsm/${pairname}/out-DEM_1m.tif)
    
        # Create bbox shp for a specific run
        cmd="ogr2ogr -clipdst ${xmin} ${ymin} ${xmax} ${ymax} -t_srs EPSG:4326 ${main_dir}/${pairname}/bbox_tmp.shp /att/briskfs/pmontesa/userfs02/arc/world_adm0.shp"
        echo $cmd
        eval $cmd

        # Reproject bbox to utm of input DEM
        cmd="ogr2ogr -t_srs \"${prj_utm}\" ${main_dir}/${pairname}/bbox_utm.shp ${main_dir}/${pairname}/bbox_tmp.shp"
        echo $cmd        
        eval $cmd

        # Run a function in my bash_profile to convert utm bbox to the format needed for -t_prjwin
        source ~/.bash_profile
        p2d_extent=$(ogr_extent ${main_dir}/${pairname}/bbox_utm.shp)
        
        echo $p2d_extent
        rm ${main_dir}/${pairname}/bbox*
    else
        echo "No bbox specified."
        p2d_extent=''
        #p2d_extent="439903 7184954 446392 7174636"
    fi

    for search_rad in 2 5 10 ; do
        cmd="${script_name} $pairname 1 4 10 ${search_rad} true true true false \"$p2d_extent\"" 
        echo $cmd
        eval $cmd | tee ${main_dir}/logs/${script_name%.*}_${hostN}_${pairname}_searchrad${search_rad}.log
    done

done < ${batch}_${hostN}