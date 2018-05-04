#!/bin/bash
#
# Run do_dsm_filt.sh in a loop
#
main_dir=/att/gpfsfs/briskfs01/ppl/pmontesa/outASP_TEST/test_do_dem_filt
batch=${main_dir}/list_pairname
hostN=`/bin/hostname -s`

mkdir -p ${main_dir}/logs

while read -r pairname; do

    do_dem_filt-TEST.sh $pairname 1 4 5 2 true false false false | tee ${main_dir}/logs/do_dem_filt-TEST_${hostN}_${pairname}.log

done < ${batch}_${hostN}