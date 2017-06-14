# *TD = to-do

# original: script that takes a batch as inputs, reads the query output summary and:
# 1. writes an output checklist with a 1:1 input row to output row, where:
#    if query result was not 'processing' (ie sent to DISCOVER for processing) the outResult is the same as the inResult
#    if 'TIME LIMIT' is in slurm.out file, outResult is 'timedOut' and status is 'rerun'...*TD will eventually also write an input csv to rerun the timedOut pairs from query (with extended time)
#    if neither of these two are the case, outResult is DEM_DNE. *TD may narrow this down to other causes but for now DEM_DNE
# 2. if DEM_exists (no time outs, no missing Data errors, etc.): write to a ADAPT_completedPairs_batch$name.txt so list can be used to delete unneeded inputs and outputs from DISCOVER
#

import os
import glob
import sys
import datetime
import subprocess as sp

batch = sys.argv[1] # get batch id from command line

start = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")


aspdir = "/att/pubrepo/DEM/hrsi_dsm/"
incsv = "/att/gpfsfs/briskfs01/ppl/mwooten3/Paul_TTE/batch_summary_csvs/batch{}_output_summary.csv".format(batch)
outcsv = "/att/gpfsfs/briskfs01/ppl/mwooten3/Paul_TTE/checklists/batch{}_checklist__{}.csv".format(batch, start)
slurmdir = os.path.join(aspdir, 'outSlurm', 'batch{}'.format(batch))
logdir = os.path.join(aspdir, 'logs') # might change later

compPairsFile = "/att/gpfsfs/briskfs01/ppl/mwooten3/Paul_TTE/ADAPT_completedPairs/ADAPT_completedPairs_batch{}.txt".format(batch)

with open(outcsv, 'w') as o:
    o.write('batchID,pairname,catID_1,catID_1_found,catID_2,catID_2_found,result,reason (optional)\n')

with open(incsv, 'r') as c:
    csvlist = [f.strip() for f in c.readlines()]


requery_pairnames = [] # *TD what are we doing with this?


cnt=0
for cline in csvlist:

    cnt+=1
    if cnt==1:
        continue # skip hdr

    clist = cline.split(',')

    # get pairname from csvlist
    pairname = clist[1].strip()
    qresult = clist[9]
    cat1 = clist[2]
    cat1_found = clist[3]
    cat2 = clist[4]
    cat2_found = clist[5]

    if qresult != 'processing': # if the pair was not sent to processing, put the reason (aka qresult) in the output csv
        outline = '{},{},{},{},{},{},{}'.format(batch, pairname, cat1, cat1_found, cat2, cat2_found, qresult)
        with open(outcsv, 'a') as oc:
            oc.write('{}\n'.format(outline))
        continue

    # now check pair sent for processing to be sure output exist but only if query result = processing
    checkOut1 = os.path.join(aspdir, "{}/out-strip-DEM.txt".format(pairname))
    checkOut2 = os.path.join(aspdir, "{}/out-strip-DEM.tif".format(pairname))
    if os.path.isfile(checkOut1) and os.path.isfile(checkOut2):
        outline = '{},{},{},{},{},{},{}'.format(batch, pairname, cat1, cat1_found, cat2, cat2_found, 'DEM_exists')
        with open(outcsv, 'a') as oc:
            oc.write('{}\n'.format(outline))

        with open(compPairsFile, 'a') as cp:
            cp.write('{}\n'.format(pairname))

        continue


    # at this point we know there were no query issues and we know the DEM does not exist for the pair...but why?
    ## job time out
    ## IOerror / disk quota error
    ## can not find (xml error) ?

    # get the slurm and log files
    slurmFile = glob.glob(os.path.join(slurmdir, 'batch{}__{}__slurm*out'.format(batch, pairname)))[0]
    logFile = glob.glob(os.path.join(logdir, 'run_asp_LOG_{}__batch{}*txt'.format(pairname, batch)))[0]

    # first check for time outs
    with open(slurmFile, 'r') as sf:
        slurm = sf.read() # dump contents of slurm file into var

    if 'TIME LIMIT' in slurm:
      #  print '{} timed out'.format(pairname)
        requery_pairnames.append(pairname)
        outline = '{},{},{},{},{},{},{},{}'.format(batch, pairname, cat1, cat1_found, cat2, cat2_found, 'timedOut', 'rerun')
        with open(outcsv, 'a') as oc:
            oc.write('{}\n'.format(outline))

        # *TD run function to put pairname inputs into a new query where the csv file is called like reQuery_batch{}-try2.csv -- or rerunTime, something. might do timed out pairs separately
        # # prob create separate function that will access the batches original input list and extract the pair's line. function could take the output hrsi list name as input (so function can be reused when dealing with pairs that failed but didnt time out)

        continue

    # at this point we know no query errors, no time outs, but DEM DoesNotExist


    # *TD for now just do DEM_DNE, we may narrow this down later to further errors

    # after going through known errors, we still dont know why it wont work ??do we wanna rerun figure out later
    outline = '{},{},{},{},{},{},{}'.format(batch, pairname, cat1, cat1_found, cat2, cat2_found, 'DEM_DNE') # *TD what about status column?

    # write output line to csv:
    with open(outcsv, 'a') as oc:
        oc.write('{}\n'.format(outline))



