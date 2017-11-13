# from a list of pairs, loop through an dread hrsi_list csvs to find the row matching pair

import os
import glob
import sys
import datetime

name= sys.argv[1] # uniuque identifier for a particular set of pairs we want to recreated input lists for

start = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")


listdir = "/att/gpfsfs/briskfs01/ppl/mwooten3/Paul_TTE/hrsi_lists/"
inpairslist = "/att/gpfsfs/briskfs01/ppl/mwooten3/Paul_TTE/reQuery_pairs/reQuery_pairs__{}.txt".format(name)
outcsv = os.path.join(listdir, "reQuery_pairs_inputList_{}.csv".format(name))

##with open(outcsv, 'w') as o:
##    o.write('batchID,pairname,catID_1,catID_1_found,catID_2,catID_2_found,result\n')
##
##
##
##with open(incsv, 'r') as c:
##    csvlist = [f.strip() for f in c.readlines()]

with open(inpairslist, 'r') as ip:
    inpairs = [i.strip() for i in ip.readlines()]

print len(inpairs)

with open(outcsv, 'w') as oc: # write header
    oc.write('pairname,avoffnadir,avsunazim,avsunelev,avtargetaz\n')

for pair in inpairs:
    for hlist in glob.glob(os.path.join(listdir, '*csv')):
        #print hlist
        with open(hlist, 'r') as hl:
            inlines = [i.strip() for i in hl.readlines()]

        for l in inlines:
            match_pair = l.split(',')[0]

            if match_pair == pair:
                print "{} match {}".format(match_pair, pair)

                with open(outcsv, 'a') as oc:
                    oc.write('{}\n'.format(l))

                continue


