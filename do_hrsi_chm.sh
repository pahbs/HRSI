#!/bin/bash
#
# Spaceborne CHM from DSM Differencing
#
# paul.m.montesano@nasa.gov
# 
# call: <shell script> "ground_DSM_pairname canopy_DSM_pairname"
# pmontesa@ecotone04:~$ do_hrsi_chm.sh "WV02_20130717_1030010024162C00_10300100255C7F00,WV01_20130724_10200100226FE500_1020010023851E00" $indir $out_dir_main $glas_csv
# [pmontesa@ngalogin02 ~]$ pupsh "hostname ~ 'ecotone|himat'" "do_hrsi_chm.sh dsm_diff.list"
#
# DEM masking for control surfaces, GLAS filtered to those surfaces, Warp, Co-reg with pc_align, Difference, Mask
#
# Important step:
# 		Using warptool to get the intersection of both DSMs before running pc_align
#		pc_align_wrapper.sh run on each dem at same time with GNU parallel
#
source ~/anaconda3/bin/activate py2

# Args (from a list cat'd in do_par.sh that provides a single line to this script in parallel)
out_sub_dir=${1}   # 'hrsi_chm' for good stuff, 'hrsi_chm_test' for others
diff_pairs_line=${2}
in_dir=${3:-'/att/pubrepo/DEM/hrsi_dsm'}
glas_csv=${4:-'gla01-boreal50up-fix2-data_qfilt.csv'}

# Sometimes the auto compute of the min TOA threshold used to mask water isnt good, and a lot of inland water is included as valid data.
# This lets you control how the min_toa is determined. (The default is to 'auto-compute' it; but you can specify your own on a case-by-case basis if you want..)
auto_toa=${5:-'true'}
min_toa=${6:-'0.15'}
use_hisun_mask=${7:-'true'}   							# Maybe want to apply masks from BOTH chmmask.tif files?
out_dir_main=${8:-'/att/nobackup/pmontesa/chm_work'}

# Hard coded vars
res=2
dem=out-DEM_1m.tif
dem_4control=${dem%1m*}4m.tif
glas_dir=/att/nobackup/pmontesa/userfs02/data/glas/misc/tiles_5deg_old/csv_files
glas_dir='/att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/data/glas/circ_boreal'
refdem='/att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/data/tandemx/TDM90/mos/TDM1_90m_circ_DEM.vrt'

#Hardlinks
out_chm_dir=$out_dir_main/$out_sub_dir
mkdir -p $out_chm_dir

##Symlinks
#final_chm_dir=$out_dir_main/_chm_dsm_dif   #/att/nobackup/pmontesa/hrsi_chm_20170813
#mkdir -p $final_chm_dir

log_dir=${out_dir_main}/logs_do_hrsi_chm
mkdir -p $log_dir

hostN=`/bin/hostname -s`

# Input vars
# List of pairnames that you want to coreg
pairlist=$(echo $diff_pairs_line | awk -F"," '{print $1 , $2}')
pairname_canopy=$(echo $diff_pairs_line | awk -F"," '{print $2}')
pairname_ground=$(echo $diff_pairs_line | awk -F"," '{print $1}')
diffpairs=$(echo $diff_pairs_line | awk -F"," '{print $1"_"$2}')

# Put the diffpairs dirs in a subdir, for your sanity
out_dir_diffpair=$out_dir_main/_diffpairs/$diffpairs
mkdir -p $out_dir_diffpair

logFile=$log_dir/${hostN}_${diffpairs}

echo "START>:$(date)" | tee $logFile
echo | tee -a $logFile
echo "SCRIPT CALL:" | tee -a $logFile
echo ${0} ${1} ${2} ${3} ${4} ${5} ${6} ${7} | tee -a $logFile
echo "INPUTS:" | tee -a $logFile
echo "Pairnames: $diff_pairs_line" | tee -a $logFile
echo "Reference DEM: $refdem" | tee -a $logFile
echo | tee -a $logFile
echo "DEM Alignment to reference GLAS lidar" | tee -a $logFile
echo "Steps: [1] dem_control.py, [2] filter_glas_control.py, [3] pc_align_wrapper_3dsi.sh, [4] compute_dz_chm.py, [5] apply_mask.py, [6] plot_hist.py, [7] chm_shift.py" | tee -a $logFile

# This could eventually be appended to the DEM Workflow - but GLAS reference csv files only available on ADAPT in boreal

inref=$glas_dir/$glas_csv    #gla14_N70E095.csv #gla14_N60-70.csv
echo "lidar reference: ${inref}" | tee -a $logFile

hdr_subr=`echo; echo "Step [1] Get static control DEM: masks input using TOA (dark & smooth) & DEM (roughness & slope) masked out of the 4m version of each DEM" ; echo`
echo $hdr_subr| tee -a $logFile

if [ "$auto_toa" = "false" ] ; then
    echo ; echo "Min TOA threshold for water masking is: ${min_toa}. Auto-compute min TOA is OFF!" ; echo
    parallel -verbose 'dem_control.py {1} -out_dir {3} --no-auto_min_toa -min_toa {4} -filt_param {2} -15 15' ::: ${in_dir}/${pairname_canopy}/${dem_4control} ${in_dir}/${pairname_ground}/${dem_4control} ::: $refdem ::: $out_dir_diffpair ::: ${min_toa} 2>&1 | tee -a $logFile
else
    parallel -verbose -j 1 'dem_control.py {1} -out_dir {3} -filt_param {2} -15 15' ::: ${in_dir}/${pairname_canopy}/${dem_4control} ${in_dir}/${pairname_ground}/${dem_4control} ::: $refdem ::: $out_dir_diffpair 2>&1 | tee -a $logFile
fi

dem_control_ground=${out_dir_diffpair}/${pairname_ground}_${dem_4control%.*}_control.tif
dem_control_canopy=${out_dir_diffpair}/${pairname_canopy}_${dem_4control%.*}_control.tif

#Check for valid DEM control
# THIS NOT WORKING; always evals to false and exits..
#if [ -e ${dem_control_canopy} ] && [ $(gdalinfo ${dem_control_canopy} | awk '/MEAN/ {f=1;exit}END{print f?"true":"false"}') = "false" ] ; then
#    echo; echo "Failed to build a valid canopy DEM control tif. Check if DEM masking is appropriate. Exiting."; echo
#    exit
#fi

hdr_subr=`echo; echo "Step [2] Filter ICESat-GLAS (GLA14) using the *control.tif: this provides a set of GLAS over control surfaces only." ; echo`
echo $hdr_subr| tee -a $logFile

parallel 'filter_glas_control.py {}' ::: $inref ::: ${out_dir_diffpair}/${pairname_canopy}_${dem_4control} ${out_dir_diffpair}/${pairname_ground}_${dem_4control} 2>&1 | tee -a $logFile

#
#
#

#This 'inref' name is now the output *_ref.csv: out-DEM_4m_gla14_N70E100_ref.csv
inref=${out_dir_diffpair}/${pairname_canopy}_${dem%1m*}4m_${glas_csv%.csv}_ref.csv

REF_LIDAR=true

echo | tee -a $logfile

if [ ! -e "$inref" ] ; then
    echo "No reference LiDAR available for canopy pairname: ${pairname_canopy}  - Will have to align DEMs to each other." | tee -a $logFile
    REF_LIDAR=false
else
    echo "Create shp of GLAS reference points..." | tee -a $logFile
    cmd="glas_csv2shp.sh $inref" ; echo $cmd | tee -a $logfile ; eval $cmd
    cmd="pc_align_prep.sh $inref" ; echo $cmd | tee -a $logfile ; eval $cmd
fi

# Set up input and output names for the DSM pair
demtail=${dem##*-}

hdr_subr=`echo "Prep of intermediate file names..." ; echo` 
hdr_subr+=`echo "[a] Make symlinks to input DEM names and setup names (full path) of pairlist"`
full_pairlist=""

for i in $pairlist; do
	ln -sf `echo ${in_dir}/${i}/${dem}` `echo ${out_dir_diffpair}/${i}-${demtail}` | tee -a $logFile
    echo "Symlink to ortho for spot checks."
    ln -sf `echo ${in_dir}/${i}/${i}_ortho.tif` `echo ${out_dir_diffpair}/${i}_ortho.tif` | tee -a $logFile
	full_pairlist="$full_pairlist `echo ${out_dir_diffpair}/${i}-${demtail}`"
done
hdr_subr+=`echo $full_pairlist ; echo`
echo $hdr_subr| tee -a $logFile

hdr_subr=`echo "[b] Setup names (full path) of warped pairlist (output DEM from warptool; input to pc_align_wrapper)" ; echo`

full_pairlist_warp=""
for i in $pairlist; do
	full_pairlist_warp="$full_pairlist_warp `echo ${out_dir_diffpair}/${i}-${demtail%.*}_warp.tif`"
done
hdr_subr+=`echo $full_pairlist_warp ; echo`
echo $hdr_subr| tee -a $logFile

hdr_subr=`echo "[c] Setup names (full path) of the trans ref list (the aligned data: aka 'translated reference DEMs' output from pc_align_wrapper; input to compute_dz_chm.py)" ; echo`
trans_ref_list=""

for i in $full_pairlist_warp; do
	dembase=$(basename $i)
	trans_ref_list="$trans_ref_list `echo ${i%.*}_align/${dembase%.*}-trans_reference-DEM.tif`"
done
hdr_subr+=`echo $trans_ref_list ; echo`
echo $hdr_subr| tee -a $logFile

hdr_subr=`echo "[d] Setup the name of the tmp *dz_eul.tif file that is renamed to *_chm.tif" ; echo`
out_dz=${out_dir_diffpair}/$(echo $pairlist | awk -v dem_var="${demtail%.*}" -F" " '{print $1"-"dem_var"_warp-trans_reference-DEM_"$2"-"dem_var"_warp-trans_reference-DEM"}')_dz_eul.tif
echo $hdr_subr| tee -a $logFile

if [ ! -f "${out_chm_dir}/${diffpairs}_chm.tif" ]; then
	hdr_subr=`echo "Output diffpairs dir:" ; echo "$out_dir_diffpair"; echo`
	hdr_subr+=`echo "Name of dz file:" ; echo $(basename $out_dz); echo  `
    echo $hdr_subr | tee -a $logFile

    if [[ ! -f ${out_chm_dir}/${out_dz} ]] ; then

	    hdr_subr=`echo; echo "Warping, Co-registering, Computing Difference, and Masking..."; echo `

	    hdr_subr+=`echo "Get the intersection of the two DSMs before aligning..." ; echo`
	    cmd="warptool.py $full_pairlist -tr $res -te intersection -dst_ndv -99 -outdir $out_dir_diffpair"
        hdr_subr+=`echo $cmd` ; echo $hdr_subr | tee -a $logFile ; eval $cmd 2>&1 | tee -a $logFile

	    hdr_subr=`echo; echo "Create the *_align dir and the *trans_reference-DEM.tif"; echo`
	    # TODO: look at the pc_align logs and output and capture important stats for each DSM, and append to a main csv coreg stats file 

        dem_1=$(echo $full_pairlist_warp | awk -F" " '{print $1}')
        dem_2=$(echo $full_pairlist_warp | awk -F" " '{print $2}')

	    hdr_subr+=`echo "Run pc_align_wrapper in parallel on:" `
	    hdr_subr+=`echo "$dem_1"`
        hdr_subr+=`echo "$dem_2"`        
        hdr_subr+=`echo "For pc_align_wrapper to compute translations on coarse (4m) DEMs, make the expected symlinks."`
        echo $hdr_subr| tee -a $logFile

        # putting it next to the $out_dir_diffpair/<pairname>_DEM_1m_warp.tif
        ln -sf ${in_dir}/${pairname_ground}/${dem_4control} ${dem_1%1m*}4m.tif
        ln -sf ${in_dir}/${pairname_canopy}/${dem_4control} ${dem_2%1m*}4m.tif

        if [[ "$REF_LIDAR" = "true" ]] ; then
            hdr_subr=`echo ; echo "PC align with reference LiDAR" ; echo`
            echo $hdr_subr| tee -a $logFile
            # Use previous version of pc_align_wrapper
            # previous version works, new one doesnt b/c sample_raster_pts.py : renamed to /att/gpfsfs/home/pmontesa/.local/bin/pc_align_wrapper_3dsi.sh
            parallel 'pc_align_wrapper_3dsi.sh {}' ::: ${inref%.csv}_asp.csv ::: $dem_1 $dem_2 2>&1 | tee -a $logFile 
        fi
        
    fi
    log=$(ls -t $logFile | head -1)

    dem_align_canopy=$(echo $trans_ref_list | awk -F" " '{print $2}')
    dem_align_ground=$(echo $trans_ref_list | awk -F" " '{print $1}')

	if [[ ! -e $dem_align_canopy ]] || [[ ! -e $dem_align_ground ]] ; then

        hdr_subr=`echo; echo "One of the DEMs did not get aligned. Not enough GLAS? Try aligning to each other." ; echo`
        hdr_subr+=`echo ; echo "PC align with canopy DSM as reference GRID" ; echo`
        
        # Specify one DEM as source and the other as ref (use *control.tif versions)
        # Use the canopy control.tif as the reference, since that has MORE area masked out (more likely to include only ground pixels)

        cmd="pc_align_wrapper_3dsi.sh ${dem_control_canopy} ${dem_control_ground%_out-DEM_4m*}-DEM_1m.tif"
        hdr_subr+=`echo $cmd` ; echo $hdr_subr | tee -a $logFile ; eval $cmd 2>&1 | tee -a $logFile

        hdr_subr=`echo; echo "Compute Spaceborne CHM as the difference of the 2 co-aligned DEMs"`e
        hdr_subr+=`echo "Differencing the ground *trans_reference-DEM.tif from the original canopy DEM"`

        dem_align_ground=${out_dir_diffpair}/${pairname_ground}-DEM_1m_grid_align/*1m-trans_reference-DEM.tif
        dem_orig_canopy=${out_dir_diffpair}/${pairname_canopy}-DEM_1m.tif

        cmd="compute_dz_chm.py -tr ${res} ${dem_align_ground} ${dem_orig_canopy} -outdir $out_dir_diffpair"
        hdr_subr+=`echo $cmd` ; echo $hdr_subr | tee -a $logFile ; eval $cmd 2>&1 | tee -a $logFile

        hdr_subr=`echo ; echo "Reset out_dz"`
        out_dz=$(ls ${out_dir_diffpair}/*-trans_reference-DEM_${pairname_canopy}-DEM_1m_dz_eul.tif)
        hdr_subr+=`echo $out_dz`
        echo $hdr_subr | tee -a $logFile

    else

        hdr_subr=`echo; echo "Compute Spaceborne CHM as the difference of the 2 GLAS-aligned DEMs" ; echo`
        hdr_subr+=`echo "Differencing the *warp_trans_reference-DEM.tif files"`

        cmd="compute_dz_chm.py `echo $trans_ref_list` -outdir $out_dir_diffpair"
        hdr_subr+=`echo $cmd` ; echo $hdr_subr | tee -a $logFile ; eval $cmd 2>&1 | tee -a $logFile
    fi
    
    applymask='true'

    if [[ "$applymask" = 'true' ]] ; then

        #hdr_subr=`echo; echo "Apply chmmask using TOA dark, TOA smooth, & DEM (slope) to the dz" ; echo`
        echo; echo "Apply chmmask using TOA dark, TOA smooth, & DEM (slope) to the dz" ; echo
        # Need chmmask from dem_control on the ground DEM
        # masking surfaces with slopes, TOA dark & TOA smooth (all encompassed by *chmmask.tif)
        # Note: DEM roughness not used as mask here
        # Note: if *chmmask.tif is all nan (type None) then the output chm will be nan
        # ToDo: catch the above issue to re-run chm without applying a mask to at least get a valid chm

        pairname_mask=$pairname_ground

        if [[ $use_hisun_mask = 'false' ]] ; then
            pairname_mask=$pairname_canopy
        fi
        cmd="apply_mask.py ${out_dz} ${out_dir_diffpair}/${pairname_mask}_${dem_4control%.*}_chmmask.tif -extent raster -out_fn $out_chm_dir/${diffpairs}_chm.tif"
        #hdr_subr+=`echo $cmd` ; echo $hdr_subr | tee -a $logFile ; eval $cmd 2>&1 | tee -a $logFile
        echo $cmd ; eval $cmd 2>&1 | tee -a $logFile

    else
        mv ${out_dz} $out_chm_dir/${diffpairs}_chm.tif
    fi
 
    hdr_subr=`echo "Plot histogram of chm" ; echo`
    cmd="plot_hist.py ${out_chm_dir}/${diffpairs}_chm.tif -min_val -1 -max_val 20"
    hdr_subr+=`echo $cmd` ; echo $hdr_subr | tee -a $logFile ; eval $cmd 2>&1 | tee -a $logFile

fi

hdr_subr=`echo "Compute Spaceborne CHM Correction" ; echo`
cmd="chm_shift.py $out_chm_dir/${diffpairs}_chm.tif -n_gaus 4"
hdr_subr+=`echo $cmd` ; echo $hdr_subr | tee -a $logFile ; eval $cmd 2>&1 | tee -a $logFile
hdr_subr=`echo ; echo "Cleaning up tmp dz and mask files..." ; echo` ; echo $hdr_subr | tee -a $logFile
rm -f $out_dir_diffpair/$out_dz

#for i in out-DEM_*control.tif out-DEM_*mask.tif out-DEM_*filt.tif *.png *ortho_4m_*.tif *toa* ; do
#    echo $i | tee -a $logFile
#    rm -f $in_dir/$pairname_canopy/$i
#    rm -f $in_dir/$pairname_ground/$i
#done 

if [ -f "$out_chm_dir/${diffpairs}_chm.tif" ]; then

    hdr_subr=`echo; echo "Overview Generation ..."; echo`
    cmd="do_gdaladdo.sh ${out_chm_dir}/${diffpairs}_chm.tif"
    hdr_subr+=`echo $cmd` ; echo $hdr_subr | tee -a $logFile ; eval $cmd 2>&1 | tee -a $logFile
	
    #echo " " | tee -a $logFile
	#echo "Creating symbolic link to:	${diffpairs}_chm.tif" | tee -a $logFile
	#echo "	From directory:	$out_chm_dir" | tee -a $logFile
	#echo "	To directory:	$final_chm_dir" | tee -a $logFile
	#ln -sf $out_chm_dir/${diffpairs}_chm.tif $final_chm_dir/${diffpairs}_chm.tif | tee -a $logFile
fi

echo "<END> $(date)" | tee -a $logFile