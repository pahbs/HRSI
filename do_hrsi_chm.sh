#!/bin/bash
#
# TEST Spaceborne CHM from DSM Differencing
#
# call: <shell script> "ground_DSM_pairname canopy_DSM_pairname"
# pmontesa@ecotone04:~/code$ ./do_hrsi_chm.sh "WV02_20130717_1030010024162C00_10300100255C7F00 WV01_20130724_10200100226FE500_1020010023851E00 gla14_N70E095.csv"
# [pmontesa@ngalogin02 ~]$ pupsh "hostname ~ 'ecotone|ngaproc|himat'" "/att/gpfsfs/home/pmontesa/code/do_hrsi_chm.sh dsm_diff.list"
#
# This is a simple approach that doesnt involve masking reference lidar shots to exclude non-ground shots
# Questions:
#		Do DSMs have to be warped to WGS84 before pc_align_wrapper?
#		Is coarsening to 2m res necessary for areas of large DSM intersection?
#
# Important step:
# 		Using warptool to get the intersection of both DSMs before running pc_align
#
# Improvement needed:
#		The final dz image values still need to be shifted -- this should be addressed with better alignment using a dem mask for ground only areas
#

# Hard coded vars
res=2
in_dir=/att/pubrepo/DEM/hrsi_dsm
out_dir_main=/att/briskfs/pmontesa/hrsi_chm_work
out_chm_dir=$out_dir_main/hrsi_chm

dem=out-strip-DEM.tif

# GET HOSTNAME
#
hostN=`/bin/hostname -s`

####  ...from list of files
###while read -r diff_pairs_line; do

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
inref=/att/gpfsfs/briskfs01/ppl/pmontesa/userfs02/glas/tiles_5deg/csv_files/$(echo $diff_pairs_line | awk -F"," '{print $3}')
echo "Input lidar reference: $inref" | tee -a $logFile
/att/gpfsfs/home/pmontesa/code/pc_align_prep.sh $inref

# Set up input and output names for the DSM pair
#
# [1] Input DEM names: Get a full path pairlist
full_pairlist=""
for i in $pairlist; do
	ln -s `echo ${in_dir}/${i}/${dem}` `echo ${in_dir}/${i}/${i}-DEM.tif`
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
# [4] Output CHM name
out_dz=$(echo $pairlist | awk -F" " '{print $1"-DEM_warp-trans_reference-DEM_"$2"-DEM_warp-trans_reference-DEM"}')_dz_eul.tif

if [ ! -f "$out_chm_dir/$out_dz" ]; then
	echo "Warping, Co-registering, and Computing Difference..." | tee -a $logFile
	# ---- Get the intersection of the pair
	warptool.py $full_pairlist -tr $res -te intersection -dst_ndv -99 -outdir $out_dir_diffpair 2>&1 | tee -a $logFile

	for i in `ls -d $out_dir_diffpair/*align`;do
		if [ -d $i ]; then
			echo "Removing existing pc_align output dir: $(basename $i)" | tee -a $logFile
			rm -R $i
		fi
	done

	# ---- Create the *_align dir and the *trans_reference-DEM.tif
	for i in $full_pairlist_warp; do
		pc_align_wrapper.sh ${inref%.csv}_asp.csv $i 2>&1 | tee -a $logFile
	done
	
	# TODO:
	# ***look at the pc_align logs and output and capture important stats for each DSM, and append to a main csv coreg stats file 
	
	# ---- Compute Spaceborne CHM
	# difference the *warp_trans_reference-DEM.tif files
   # TODO: fix the slope filter in the py below
	compute_dz_chm.py `echo $trans_ref_list` -outdir $out_chm_dir 2>&1 | tee -a $logFile

   # TODO: Apply water mask

fi

if [ ! -f "$out_chm_dir/${diffpairs}_chm_*.tif" ]; then
	chm_correct.py $out_chm_dir/${out_dz} -out_name ${diffpairs} 2>&1 | tee -a $logFile
fi

for i in `ls "$out_chm_dir/${diffpairs}_chm_*.tif"`; do
   file=$(echo $(basename $i)
	ln -s $i /att/gpfsfs/briskfs01/ppl/pmontesa/hrsi_chm/${file})
done

gdaltindex -t_srs EPSG:3995 hrsi_chm_index.shp *.tif

#rm $out_chm_dir/$out_dz

#####done < $out_dir_main/${input_list_stem}_$hostN
echo "<><><><><END> $(date)" | tee -a $logFile