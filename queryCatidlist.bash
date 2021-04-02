#!/bin/bash
#  queryCatidList.bash
#
#  Given an input file with a list of catalog ids to query, one id per line,
#  this script will query ngadb / nga_inventory_canon table and return to the 
#  output file a list of file paths matching those catids.
#
#  v2: also optionally outputs a file listing catids NOT found in ngadb
#
#  USAGE:
#    ./queryCatidList.bash <cattIdListFile.txt> <outputPathFile.txt> [<outputNotFound.txt>]
#   eg:
#    ./queryCatidList.bash danscatids.txt dansfiles.txt notfoundlist.txt
# 
#    where:
#       <inputCatIDFile.txt> : file listing one catalog id per line
#       <outputPathFile.txt> : output file where matching filepaths will be written
#       <outputNotFound.txt> : OPTIONAL List of input IDs not found in ngadb
# 
# History:
#   v2: 20190411 : Update to add outputNotFound.txt 
#   v3: 20200623 : Update to do individual queries per catid, as previous version appeared to not be working
#

# Set command-line arguments to input parameters
catidlist=$1
outfile=$2
notfound=${3:none}
echo "queryCatidList.bash v2/20190411"

# Check input parameters
if [ $# -lt 2 ]; then
    echo "Incorrect number of arguments supplied!"
    echo "Please input: queryCatidList.bash <catidListFile.txt> <outputPathFile.txt> [<outputNotFound.txt>]"
    exit
fi
if [ -f $outfile ]; then
    echo "Output file exists! Try again.."
    exit
fi

# Print input parameters
echo "Checking CatIDs in file $catidlist against nga_inventory_canon in ngadb01."
echo "Paths of found CatIDs will be output to $outfile"
if [ -z "$notfound" ]
  then
    echo "Optional input <outputNotFound.txt> not present; will not output list of CatIDs not found"
  else
    echo "List of CatIDs NOT found will be output to $notfound"
fi
echo "----Running----"

cidlist=$(sed '/^\s*$/d' $catidlist | awk 'ORS=","' RS="\r\n" | sed '$s/.$//')
# Query one at a time
for cid in $cidlist; do
  querycmd="select s_filepath from nga_inventory_canon where catalog_id = '"$cid"'"
  psql -d ngadb01 -h ngadb01 -U anon -c "COPY ($querycmd) TO STDOUT (format csv)" >> $2
done

outn=$(wc -l < "$2")
echo "Found $outn filepaths from list of input CatIDs. Output list written to $2"

# identify catids not found, if outputNotFound.txt input parameter set
if [ -n "$notfound" ]; then
  cidlists=$(echo $cidlist | sed 's/,/ /g')
  for catid in $cidlists; do nfound=$(grep $catid $outfile | wc -l) ; if [ $nfound -eq 0 ]; then echo "$catid" >> $notfound; fi; done
  echo "CatIDs not found written to $notfound"
fi
