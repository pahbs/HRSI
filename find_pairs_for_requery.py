# loop through batch summary csvs and make big list of pairnames that did not get sent to DISCOVER for processing
# then check to see that the pair is not in outASP on ADAPT
# if it's not there, add pair to the requery list, then run create_requery_lists.py

import os
import glob
import sys
import datetime
import create_requery_lists

name= sys.argv[1] # uniuque identifier for a particular set of pairs we want to recreated input lists for -- will be for output reQuery_pairs__name.txt file


def check_pair_output(pairname):
    # will check to see if output for pair exists in outASP...will only run this function if missingData result is there

    checkOut1 = "/att/pubrepo/DEM/hrsi_dsm/{}/out-strip-DEM.txt".format(pairname)
    checkOut2 = "/att/pubrepo/DEM/hrsi_dsm/{}/out-strip-DEM.tif".format(pairname)
    if os.path.isfile(checkOut1) and os.path.isfile(checkOut2): # already ran successfully and was rsynced back to ADAPT
        requery = False # don't need to requery
    else: requery = True # if it does not exist, requery is True

    return requery


start = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")


indir = "/att/gpfsfs/briskfs01/ppl/mwooten3/Paul_TTE/batch_summary_csvs/" # where batch summaries live - loop through all
outpairslist = "/att/gpfsfs/briskfs01/ppl/mwooten3/Paul_TTE/reQuery_pairs/reQuery_pairs__{}.txt".format(name) # output file written to
if os.path.isfile(outpairslist):
    desc = raw_input("Output file for name {} already exists. Press 1 if you want to overwrite. Press 2 if you want to run script with a new name: ".format(name))
    if int(desc) == 1:
        print "  Removing {}\n".format(outpairslist)
        os.remove(outpairslist)
    if int(desc) == 2:
        print "  Rerun script with different name"
        sys.exit()

glob_summaries = glob.glob(os.path.join(indir, 'batch*output_summary.csv'))
print glob_summaries


for sumcsv in glob_summaries:
    with open(sumcsv, 'r') as sc:
        sumTxt = [s.strip() for s in sc.readlines()]

    for line in sumTxt: # loop through lines in summary csv
        row = line.split(',')
        pairname = row[1]
        result= row[9]
        if result == 'missingData': # candidate for requery, only if output does not exist in outASP (in case it was requeired already)
            requery = check_pair_output(pairname) # requery will be True if we should add to list (i.e. if missingData and output DNE); will be False if not missingData, or missingData but output exists

        else:
            continue # skip to next pair if data was not missing during query

        if not requery:
            continue # if requery is False, skip to next pair

        # now we are at this point we know result is Missing Data and requery is True. Write pair to list
        with open(outpairslist, 'a') as op:
            op.write('{}\n'.format(pairname))

# now send to this script to make the requery csv:
create_requery_lists.main(name)


