#!/bin/bash
#
# Store this script in $HOME and launch like this:
# pupsh "hostname ~ 'ecotone0[1-2]'" "run_compute_glcm_ij.sh"
#

metric_list="contrast dissimilarity correlation ASM"
win_list="7 11 15"
dist_list="1 3 5"

# GET HOSTNAME
hostN=`/bin/hostname -s`

glcm_tif_list=$NOBACKUP/userfs02/projects/hrsi_forest/test_glcm/glcm_tif_list

#  ...from a VM-specific list of files
while read -r image; do
		  
	echo $image
	python /att/gpfsfs/home/pmontesa/code/compute_glcm_ij.py $image "$metric_list" "$win_list" "$dist_list"

done < ${glcm_tif_list}_$hostN