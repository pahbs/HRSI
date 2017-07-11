# 6/22/2017  THIS SCRIPT ASSUMES WE ARE RUNNING BECAUSE WE HAD TO STOP A BATCH MIDWAY THROUGH
# Process:
# move the old submission shell script to name_original.sh and rewrite name.sh
# for a batch input list, read the input list pairs and...
# for each pairname:
#  add call to pair slurm.j file to new submission shell script
#  erase everything in outASP
#  delete outASP directory


import os
import glob
import sys
import datetime
import subprocess as sp

batch = sys.argv[1] # get batch id from command line
inputList = sys.argv[2] # get input text list with pairnames from command line

start = datetime.datetime.now().strftime("%m%d%Y-%H:%M")

aspdir = "/discover/nobackup/projects/boreal_nga"
inASPdir = os.path.join(aspdir, 'inASP', 'batch{0}'.format(batch))
outASPdir = os.path.join(aspdir, 'outASP', 'batch{0}'.format(batch))

##compPairsFile = os.path.join(aspdir, "ADAPT_completedPairs", "ADAPT_completedPairs_batch{0}.txt".format(batch))

logdir = os.path.join(aspdir, "cleanupLogs", "cleanUp_outASP_Logs")
logfile = os.path.join(logdir, "cleanUp_outASP_batch{0}_Log_{1}.txt".format(batch, start))
print "Cleaning up batch %s. See %s for output" % (batch, logfile)
so = se = open(logfile, 'w', 0) # open our log file
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0) # re-open stdout without buffering
os.dup2(so.fileno(), sys.stdout.fileno()) # redirect stdout and stderr to the log file opened above
os.dup2(se.fileno(), sys.stderr.fileno())

with open(inputList, 'r') as pf:
    Pairs = [f.strip() for f in pf.readlines()]

# now get unique list
Pairs = list(set(list(Pairs)))

npairs = len(Pairs)


print '\nBEGIN cleaning up outASP for batch %s: %s\n' % (batch, datetime.datetime.now().strftime("%m%d%Y-%H:%M"))
print ' outASP: {0}'.format(outASPdir)
print ' Pairs text file: {0}'.format(inputList)
print ' Number of pairs being cleaned up for batch: {0}\n---------------------------------------\n'.format(npairs)

# before looping through pairs, rename original batch submission script and start to write new one
submission_file = os.path.join(inASPdir, 'submit_jobs_batch{0}.sh'.format(batch)) # original file needs to be renamed
origSubFile = submission_file.replace('.sh', '__original.sh')
os.system('mv {0} {1}'.format(submission_file, origSubFile))

with open(submission_file, 'w') as ff:
    ff.write('#!/bin/bash\n\n')

cnt=0
succ_cnt = 0
for Pair in Pairs:

    # vars for job script, in/outASP pair dirs
    inASPpair = os.path.join(inASPdir, Pair)
    outASPpair = os.path.join(outASPdir, Pair)

    job_script = os.path.join(inASPpair, 'slurm_batch%s_%s.j' % (batch, Pair)) # this already exists. just write the commands surrounding it to the newly made submission file

    with open(submission_file, 'a') as ff:
        ff.write("\ncd {0}\nchmod 755 {1}\nsed -i '$a\\' {1}\nsbatch {1}".format(inASPpair, os.path.basename(job_script)))

    cnt+=1

    print "\nPair {0}/{1}: {2}".format(cnt, npairs, Pair)

    # delete everything from outASP
    outDel = os.path.join(outASPpair, '*')
    print " Deleting all data from {0} then removing entire directory".format(outDel)
    comm2 = 'rm {0}'.format(outDel)
    print ' ', comm2
    try:
        os.system(comm2)
    except Exception as e:
        print " -Error deleting data with {0}: {1}".format(outDel, e)


    comm2a = 'rmdir {0}'.format(outASPpair)
    print ' ', comm2a
    try:
        os.system(comm2a)
    except Exception as e:
        print " -Error deleting directory {0}: {1}".format(outASPpair, e)
    print ''
    succ_cnt += 1

print "\nDONE cleaning up for batch {0}: {1}".format(batch, datetime.datetime.now().strftime("%m%d%Y-%H:%M"))
print ' Cleaned up for {0} pairs out of {1} attempted'.format(succ_cnt, npairs)

