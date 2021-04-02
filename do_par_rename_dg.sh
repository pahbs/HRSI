#!/bin/bash

DIRREQ=${1:-''} # /att/gpfsfs/briskfs01/ppl/pmontesa/outASP/Req986

if [ -z "${DIRREQ}" ] ; then
    echo "Enter a main request dir!" ; echo
    exit
fi

# Find all XML files within a request dir and feed in parallel to script
parallel 'source rename_dg.sh {1} {2}' ::: `find ${DIRREQ} -type f -name '*1BS-*.XML'` ::: ${DIRREQ}
