#!/bin/bash

INPUTXML=${1:-''}
TOPDIR=${2:-''} # top dir under which new catid dirs will sit 

if [ -z "${INPUTXML}" ] || [[ -z "${TOPDIR}" ]] ; then
    echo "Enter an XML and a top dir!" ; echo
    exit
fi


# Get tag values from XML
satid=$(awk -F '[<>]' '/SATID/{print $3}' $INPUTXML)
satid=$(echo ${satid} | awk -F ' ' '{print $1}' )

catid=$(awk -F '[<>]' '/CATID/{print $3}' $INPUTXML)

tlctime=$(awk -F '[<>]' '/TLCTIME/{print $3}' $INPUTXML)
tlctime=$(echo ${tlctime} | awk -F '[T]' '{print $1}' )
acqdate=$(echo "$(date -d "$tlctime" +%Y%m%d)")

#REFORMULATE NAME of XML, converting to lowercase extensions
RENAMEDXML=$(echo "${satid}_${acqdate}_${catid}_$(basename $INPUTXML)" | sed -r "s/([^.]*)\$/\L\1/")

# Renaming done, still in initial dir
echo $INPUTXML
echo $RENAMEDXML

# Get correspondig NTF, rename, and convert to lowercase extension
INPUTNTF="${INPUTXML%.XML}.NTF"
RENAMEDNTF=$(echo "${satid}_${acqdate}_${catid}_$(basename $INPUTNTF)" | sed -r "s/([^.]*)\$/\L\1/")

# Make CATID dir under DIRREQ
mkdir -p ${TOPDIR}/${satid}_${acqdate}_${catid}
cp $INPUTXML ${TOPDIR}/${catid}/${RENAMEDXML}
ln -sf $INPUTNTF ${TOPDIR}/${catid}/${RENAMEDNTF}