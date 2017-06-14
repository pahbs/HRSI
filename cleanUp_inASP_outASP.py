# 5/31/2017: for a batch, this script will:
# read the ADAPT_completedPairs_batch$name.txt file to read the pairnames whose DEM exists on ADAPT
# for each pairname:
#  erase everything in inASP
#  delete directory
#  erase everything in outASP
#  delete directory

import os
import glob
import sys
import datetime
import subprocess as sp

batch = sys.argv[1] # get batch id from command line

start = datetime.datetime.now().strftime("%m%d%Y-%H:%M")

aspdir = "/discover/nobackup/projects/boreal_nga"
inASPdir = os.path.join(aspdir, 'inASP', 'batch{0}'.format(batch))
outASPdir = os.path.join(aspdir, 'outASP', 'batch{0}'.format(batch))

compPairsFile = os.path.join(aspdir, "ADAPT_completedPairs", "ADAPT_completedPairs_batch{0}.txt".format(batch))

logdir = os.path.join(aspdir, "ADAPT_completedPairs", "cleanupLogs")
logfile = os.path.join(logdir, "cleanUp_batch{0}_Log_{1}.txt".format(batch, start))
print "Cleaning up batch %s. See %s for output" % (batch, logfile)
so = se = open(logfile, 'w', 0) # open our log file
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0) # re-open stdout without buffering
os.dup2(so.fileno(), sys.stdout.fileno()) # redirect stdout and stderr to the log file opened above
os.dup2(se.fileno(), sys.stderr.fileno())

with open(compPairsFile, 'r') as cpf:
    compPairs = [f.strip() for f in cpf.readlines()]

npairs = len(compPairs)


print '\nBEGIN cleaning up batch %s: %s\n' % (batch, datetime.datetime.now().strftime("%m%d%Y-%H:%M"))
print ' inASP: {0}'.format(inASPdir)
print ' outASP: {0}'.format(outASPdir)
print ' completedPairs text file: {0}'.format(compPairsFile)
print ' Number of completed pairs for batch: {0}\n---------------------------------------\n'.format(npairs)



cnt=0
succ_cnt = 0
for cPair in compPairs:

    inASPpair = os.path.join(inASPdir, cPair)
    outASPpair = os.path.join(outASPdir, cPair)

    cnt+=1

    print "\nPair {0}/{1}: {2}".format(cnt, npairs, cPair)

    # delete everything from inASP
    inDel = os.path.join(inASPpair, '*')
    print " Deleting all data from {0} then removing entire directory".format(inDel)
    comm1 = 'rm {0}'.format(inDel)
    print ' ', comm1
    try:
        os.system(comm1)
    except Exception as e:
        print " -Error deleting data with {0}: {1}".format(inDel, e)

    comm1a = 'rmdir {0}'.format(inASPpair)
    print ' ', comm1a
    try:
        os.system(comm1a)
    except Exception as e:
        print " -Error deleting directory {0}: {1}".format(inASPpair, e)

    print ''

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

