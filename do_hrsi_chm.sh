#!/bin/bash
#
# Spaceborne CHM from DSM Differencing
#
# paul.m.montesano@nasa.gov
# 
# call: <shell script> "ground_DSM_pairname canopy_DSM_pairname"
# pmontesa@ecotone04:~$ do_hrsi_chm.sh "WV02_20130717_1030010024162C00_10300100255C7F00 WV01_20130724_10200100226FE500_1020010023851E00"
# [pmontesa@ngalogin02 ~]$ pupsh "hostname ~ 'ecotone|himat'" "do_hrsi_chm.sh dsm_diff.list"
#
# This is a simple approach that doesnt involve masking reference lidar shots to exclude non-ground shots
#
# Important step:
# 		Using warptool to get the intersection of both DSMs before running pc_align
#		pc_align_wrapper.sh run on each dem at same time with GNU parallel
#

res=2

# Hard coded vars
dem=out-DEM_4m.tif
in_dir=/att/pubrepo/DEM/hrsi_dsm

out_dir_main=/att/briskfs/pmontesa/hrsi_chm_work

out_chm_dir=$out_dir_main/hrsi_chm

if [ ! -d "$out_chm_dir" ]; then
	mkdir $out_chm_dir
fi

final_chm_dir=/att/gpfsfs/briskfs01/ppl/pmontesa/hrsi_chm_20170813

if [ ! -d "$final_chm_dir" ]; then
	mkdir $final_chm_dir
fi

# GET HOSTNAME
#
hostN=`/bin/hostname -s`

# ...from a list cat'd in do_par.sh that provides a single line to this script in parallel
diff_pairs_line=${1}


# Input vars
#
# List of pairnames that you want to coreg
pairlist=$(echo $diff_pairs_line | awk -F"," '{print $1 , $2}')
diffpairs=$(echo $diff_pairs_line | awk -F"," '{print $1"_"$2}')
out_dir_diffpair=$out_dir_main/$diffpairs

logFile=${out_dir_main}/logs/${hostN}_${diffpairs}

echo "<><><><><START> $(date)" | tee -a $logFile
echo "Input line: $diff_pairs_line" | tee -a $logFile

# Reference GLAS lidar
# TODO: 
#	1.	need to filter these shots to only include ground hits
#	2.	need to convert to TOPEX/Poseiden elipsoid (diff of 0.71m)
#	3.	need to get GLAS quality flag from new files from GSun
#inref=/att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/glas/tiles_5deg/csv_files/$(echo $diff_pairs_line | awk -F"," '{print $3}')
# TMP---------TMP------
inref=/att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/glas/tiles_5deg/csv_files/gla14_N60-70.csv
#
echo "Input lidar reference: $inref" | tee -a $logFile

if [ ! -e "${inref%.csv}_asp.csv" ]; then
	pc_align_prep.sh $inref
fi

# Set up input and output names for the DSM pair
#
# [1] Input DEM names: Get a full path pairlist
full_pairlist=""
for i in $pairlist; do
	ln -sf `echo ${in_dir}/${i}/${dem}` `echo ${in_dir}/${i}/${i}-DEM.tif` | tee -a $logFile
	full_pairlist="$full_pairlist `echo ${in_dir}/${i}/${i}-DEM.tif`"
done
# [2] Output DEM names from warptool; input to pc_align_wrapper
full_pairlist_warp=""
for i in $pairlist; do
	full_pairlist_warp="$full_pairlist_warp `echo ${out_dir_diffpair}/${i}-DEM_warp.tif`"
done
# [3] Output DEM names from pc_align_wrapper; input to compute_dz
trans_ref_list=""
for i in $full_pairlist_warp; do
	dembase=$(basename $i)
	trans_ref_list="$trans_ref_list `echo ${i%.*}_align/${dembase%.*}-trans_reference-DEM.tif`"
done
# [4] Tmp difference name
out_dz=$(echo $pairlist | awk -F" " '{print $1"-DEM_warp-trans_reference-DEM_"$2"-DEM_warp-trans_reference-DEM"}')_dz_eul.tif

if [ ! -f "$out_chm_dir/${diffpairs}_chm.tif" ]; then
	echo "Output diffpairs dir: $out_dir_diffpair" | tee -a $logFile

	if [ -d "$out_dir_diffpair" ]; then
		echo "Removing dir: $out_dir_diffpair" | tee -a $logFile
		rm -rf $out_dir_diffpair
	fi

	echo "Warping, Co-registering, and Computing Difference..." | tee -a $logFile
	# ---- Get the intersection of the pair
	warptool.py $full_pairlist -tr $res -te intersection -dst_ndv -99 -outdir $out_dir_diffpair 2>&1 | tee -a $logFile

	echo " ---- Create the *_align dir and the *trans_reference-DEM.tif" | tee -a $logFile
	# TODO: look at the pc_align logs and output and capture important stats for each DSM, and append to a main csv coreg stats file 
	#for i in $full_pairlist_warp; do
	#	pc_align_wrapper.sh ${inref%.csv}_asp.csv $i 2>&1 | tee -a $logFile
	#done
	dem_1=$(echo $full_pairlist_warp | awk -F" " '{print $1}')
	dem_2=$(echo $full_pairlist_warp | awk -F" " '{print $2}')

	echo "Run pc_align_wrapper in parallel on:" | tee -a $logFile
	echo "$dem_1" | tee -a $logFile
	echo "$dem_2" | tee -a $logFile
	parallel 'pc_align_wrapper.sh {}' ::: ${inref%.csv}_asp.csv ::: $dem_1 $dem_2 2>&1 | tee -a $logFile 
	log=$(ls -t $logFile | head -1)

	if [ -e $log ]; then
		if ! grep -q 'Error: ' "$log" ; then
			# ---- Compute Spaceborne CHM
			# difference the *warp_trans_reference-DEM.tif files
			# TODO: Apply water mask?
			# TODO: improve the slope filter in the script below
			compute_dz_chm.py `echo $trans_ref_list` -max_slope 35 -outdir $out_chm_dir 2>&1 | tee -a $logFile
			# ---- Compute Spaceborne CHM Correction
			chm_correct.py $out_chm_dir/${out_dz} -out_name ${diffpairs} 2>&1 | tee -a $logFile
		else
			echo " " | tee -a $logFile
			echo "Cannot compute CHM" | tee -a $logFile
			echo "At least 1 DEM could not be aligned using reference file:" | tee -a $logFile
			echo ${inref%.csv}_asp.csv | tee -a $logFile
			echo "Check for overlap of reference with input DEMs" | tee -a $logFile
		fi
	fi
fi

if [ -e "$out_chm_dir/${diffpairs}_chm.tif" ]; then
	echo " " | tee -a $logFile
	if [ -d "$out_dir_diffpair" ]; then
		echo "Removing existing diffpair output dir: $(basename $out_dir_diffpair)" | tee -a $logFile
		rm -R $out_dir_diffpair
	fi
fi

if [ -f "$out_chm_dir/$out_dz" ]; then
	echo " " | tee -a $logFile
	echo "Cleaning up tmp dz_eul files:" | tee -a $logFile
	for i in `ls $out_chm_dir/${out_dz%.tif}*`; do
   	echo "	$(basename $i)" | tee -a $logFile
   	rm -f $i
	done
fi

if [ -f "$out_chm_dir/${diffpairs}_chm.tif" ]; then
	echo " " | tee -a $logFile
	echo "Creating symbolic link to:	${diffpairs}_chm.tif" | tee -a $logFile
	echo "	From directory:	$out_chm_dir" | tee -a $logFile
	echo "	To directory:	$final_chm_dir" | tee -a $logFile
	ln -sf $out_chm_dir/${diffpairs}_chm.tif $final_chm_dir/${diffpairs}_chm.tif | tee -a $logFile
fi

#gdaltindex -t_srs EPSG:3995 hrsi_chm_index.shp *.tif

echo "<><><><><END> $(date)" | tee -a $logFile