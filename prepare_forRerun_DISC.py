# 7/11/2017: This script will prepare a list of pairs from a batch to rerun from DISCOVER:
# for each row in the output checklist csv:
## if result is missingData, do nothing
## if result is DEM_exists, clean up inASP and outASP
## at this point DEM_DNE. regardless of reason, erase outputs from outASP/pair and rewrite to submission file
## if reason is timedOut, also add one day to time in job script


import os
import glob
import sys
import datetime
import subprocess as sp

batch = sys.argv[1] # get batch id from command line
date = sys.argv[2] # the date/time string that indicates which list to use

old_limit = 6 # change this if limit was something else. will use this to search and replace with n days below:
new_limit = 7 # change this depending on how much time we wanna make the new limits for pairs that timed out. for example, batch 20170505 used 6 day limits, so we wanna try 7

start = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")

def cleanUp_inASP(inASPpair):

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

def cleanUp_outASP(outASPpair):

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



aspdir = "/discover/nobackup/projects/boreal_nga"
inASPdir = os.path.join(aspdir, 'inASP', 'batch{0}'.format(batch))
outASPdir = os.path.join(aspdir, 'outASP', 'batch{0}'.format(batch))

inChecklist = os.path.join(aspdir, 'ADAPT_checklists', "batch{0}_checklist__{1}.csv".format(batch, date))

logdir = os.path.join(aspdir, "cleanupLogs", "prepare_forRerun_Logs")
os.system('mkdir -p {0}'.format(logdir))
logfile = os.path.join(logdir, "prepare_forRerun_batch{0}_Log_{1}.txt".format(batch, start))
print "Cleaning up batch %s. See %s for output" % (batch, logfile)
so = se = open(logfile, 'w', 0) # open our log file
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0) # re-open stdout without buffering
os.dup2(so.fileno(), sys.stdout.fileno()) # redirect stdout and stderr to the log file opened above
os.dup2(se.fileno(), sys.stderr.fileno())

with open(inChecklist, 'r') as icl:
    rows = [f.strip() for f in icl.readlines()]

nrows = len(rows)

# before looping through pairs, rename original batch submission script and start to write new one
submission_file = os.path.join(inASPdir, 'submit_jobs_batch{0}.sh'.format(batch)) # original file needs to be renamed
origSubFile = submission_file.replace('.sh', '__original.sh')
os.system('mv {0} {1}'.format(submission_file, origSubFile))

print '\nBEGIN preparing batch {0} for rerun: {1}\n'.format(batch, start)
print ' inASP: {0}'.format(inASPdir)
print ' outASP: {0}'.format(outASPdir)
print ' input checklist file: {0}'.format(inChecklist)
print ' Number of pairs for batch: {0}\n---------------------------------------\n'.format(nrows)


cnt=0
for row in rows: # loop thru rows of output checklist

    cnt+=1
    if cnt==1:
        continue # skip hdr

    line = row.split(',')

    pairname = line[1]
    cat1 = line[2]
    cat1_found = line[3]
    cat2 = line[4]
    cat2_found = line[5]
    cresult = line[6]
    try:
        creason = line[7]
    except IndexError:
        pass

    # pair directories
    inASPpair = os.path.join(inASPdir, pairname)
    outASPpair = os.path.join(outASPdir, pairname)

    # first see if pair missingData. if so, do nothing
    if cresult == 'missingData' or cresult == 'missingData-ADAPT':
        print '\n Pair {0} was missing data. Skipping'.format(pairname)
        continue

    if cresult == 'DEM_exists': # if DEM_exists, erase inASP and outASP and move on
        print '\n Pair {0} ran successfully and is on ADAPT. Cleaning up inASP/outASP and moving on'.format(pairname)
        cleanUp_inASP(inASPpair)
        cleanUp_outASP(outASPpair)
        continue

    # now we know result has to be 'DEM_DNE'. for any reason, we will want to a) clean up outASP and b) rewrite the pair call to the new shell submission script

    print '\n Pair {0} DEM does not exist on ADAPT. Erase outASP and write call to new submission script'.format(pairname)


    # a) clean up outASP
    cleanUp_outASP(outASPpair)

    # b)
    job_script = os.path.join(inASPpair, 'slurm_batch%s_%s.j' % (batch, pairname)) # this already exists. just write the commands surrounding it to the newly made submission file
    with open(submission_file, 'a') as ff:
        ff.write("\ncd {0}\nchmod 755 {1}\nsed -i '$a\\' {1}\nsbatch {1}".format(inASPpair, os.path.basename(job_script)))

    # however, if reason is 'timedOut' we also want to add one day to the time limit
    if creason == 'timedOut':
        print '  pair failed due to timeOut. Change number of days from {0} to {1}'.format(old_limit, new_limit)
        addTimeComm = "sed -i -- 's/time={0}-00:00:00/time={1}-00:00:00/g' {2}".format(old_limit, new_limit, job_script)
        os.system(addTimeComm)
