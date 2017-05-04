#! /bin/bash
#
#
# This will run ASTER L1A do_* scripts in parallel
# launch like this:
# pupsh "hostname ~ 'ngaproc'" "/att/pubrepo/hma_data/ASTER/do_par.sh scenes.outASP-DEM"

njobs=15

topDir=/att/gpfsfs/atrepo01/data/hma_data/ASTER
hostN=`/bin/hostname -s`

cd $topDir

# The scenes list stem
stem=$1

# The list name for a given VM that will run its list of files in parallel
list_name=${stem}_${hostN}

list=$(cat ${topDir}/${list_name})

#parallel_log=logs_par_gdaladdo/${list_name}_log
parallel_log=logs_par_L1/${list_name}_log

# Run do_L1.sh
parallel --progress --results $parallel_log -j $njobs --delay 3 '/att/pubrepo/hma_data/ASTER/do_L1.sh {}' ::: $list

# Run do_gdaladdo
#parallel --progress --results $parallel_log -j $njobs --delay 3 '/att/pubrepo/hma_data/ASTER/do_gdaladdo.sh {}' ::: $list



