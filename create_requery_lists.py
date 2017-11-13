# from a list of pairs, loop through an dread hrsi_list csvs to find the row matching pair
# then rewrite to an output csv which can be used in query_db.py

# this will likely be called by find_pairs_for_requery

import os
import glob
import sys
import datetime

def main(name):
    start = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")


    listdir = "/att/gpfsfs/briskfs01/ppl/mwooten3/Paul_TTE/hrsi_lists/"
    inpairslist = "/att/gpfsfs/briskfs01/ppl/mwooten3/Paul_TTE/reQuery_pairs/reQuery_pairs__{}.txt".format(name)
    outcsv = os.path.join(listdir, "reQuery_pairs_inputList_{}.csv".format(name))


    with open(inpairslist, 'r') as ip:
        inpairs = [i.strip() for i in ip.readlines()]

    print len(inpairs)

    with open(outcsv, 'w') as oc: # write header
        oc.write('pairname,avoffnadir,avsunazim,avsunelev,avtargetaz\n')


    written_line = [] # list of lines that were written to csv. if line is in this list, it's been written so don't write it again
    for pair in inpairs:
        for hlist in glob.glob(os.path.join(listdir, '*csv')): # for each inpout list in dir
            #print hlist
            with open(hlist, 'r') as hl:
                inlines = [i.strip() for i in hl.readlines()]

            for l in inlines:
                if l in written_line:
                    continue # don't write it again
                match_pair = l.split(',')[0]

                if match_pair == pair:
##                    print "{} match {}".format(match_pair, pair)

                    with open(outcsv, 'a') as oc:
                        oc.write('{}\n'.format(l))
                    written_line.append(l)
                    continue


if __name__ == "__main__":
    name= sys.argv[1] # uniuque identifier for a particular set of pairs we want to recreated input lists for
    main()