#!/bin/bash

DIRREQ=${1:-''} # /att/gpfsfs/briskfs01/ppl/pmontesa/outASP/Req986
echo $DIRREQ

# Get rid of the first 16 chars of the NTF files to match their XMLS:
#for f in $(ls *.NTF); do
#    new=$(echo $f | cut -c17-)
#    mv $f $new
#done


# Find all XML files within a request dir and feed in parallel to script
parallel 'rename_dg.sh {1} {2}' ::: `find ${DIRREQ} -type f -name '*1BS-*.XML'` ::: ${DIRREQ}
