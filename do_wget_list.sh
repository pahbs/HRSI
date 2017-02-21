#!/bin/bash
#
# WGET order zips from a list
# Make a mirrored copy of the remote dir
# grabbing indiv scene zips
# do a find . -name *.zip | wc -l
# to see if count of scene zips matches what you expected

orderList=$1
wget --user=montesano --ask-password -l 1 -r -nc -i $orderList

# Make a scenelist
find $PWD -name '*.zip' > scenes.list

# Divide scenelist into chunks

python ~/code/gen_csv_chunks.py scenes.list ~/code/nodes_ecotone
