#! /bin/bash
#
#
# This will run a script in parallel
# launch like this:
# (put in .local/bin dir)
# pupsh "hostname ~ 'ngaproc'" "do_par.sh /path/to/topDir files.list script_name"

njobs=15

topDir=$1
hostN=`/bin/hostname -s`

cd $topDir

# The scenes list stem
stem=$2

# The list name for a given VM that will run its list of files in parallel
list_name=${stem}_${hostN}

list=$(cat ${topDir}/${list_name})

parallel_log=logs_par_${3}/${list_name}_log

# Run script
parallel --progress --results $parallel_log -j $njobs --delay 3 '"$3" {}' ::: $list



