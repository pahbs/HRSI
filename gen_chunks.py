#! /usr/bin/env python

import csv, math
import argparse

"""Generate chunks (subsets) of an input list of files to process based on a second list

    AVOID ERROR: create vmList.txt in linux

    This script breaks up a CSV into smaller sub-CSVs
    1. according to number of VMs that will be used to process sub files
        a. Number of VMs are sorted according to number of CPUs:
        [pmontesa@ngalogin01 code]$ ppdsh "cneigh|bsspy|ecotone" "grep processor /proc/cpuinfo|wc -l" | sort -nr > pmontesa_nodes
        b. the VMs with the most CPUs are listed first and thus are the first candidates to receive sub-CSV files in cases where # VMs > # CSV lines
    2. keeping the original header in each of the sub-CSVs
    3. naming the sub-CSVs consisently, based on input hostname
"""

def process_chunk(hdr,chunk,input_list,vm):
    out_list = input_list.rstrip(".csv") + "_" + vm
    #print " VM %s get %s items" %(vm, chunk)
    # Writes chunks to output
    with open(out_list,'wb') as writer:

        if not hdr == '':
            writer.write(hdr.replace("\r\n","\n"))

        for row in chunk:
            # Write out each row
            writer.write(row + '\n')

def getparser():
    parser = argparse.ArgumentParser(description="Generate chunks (subsets) of an input list of files to process based on a second list")
    parser.add_argument('in_list', default=None, help='Input list of data to process')
    parser.add_argument('in_list_subs', default=None, help='Second input list specifying the subsets')
    parser.add_argument('header', default=False, type=bool, help='Does list have a header?')
    return parser

def main():

    parser = getparser()
    args = parser.parse_args()

    in_list = args.in_list
    in_list_subs = args.in_list_subs
    header = args.header

    # Get list_arr without header and VMlist
    with open(in_list, "rb") as file1, open(in_list_subs, "rb") as file2:

        list_arr = []
        if header:
            hdr = file1.readline()
        else:
            hdr=''
        lines = file1.readlines()
        for line in lines:
            line = line.strip('\n')
            list_arr.append(line)

        node_arr = []
        for line in file2:
            line = line.strip('\n')
            print line
            node_arr.append(line.split(':')[0])

        # Get lengths of array
        numLines = len(list_arr)
        numVM = len(node_arr)
        print "# lines: %s" %(numLines) + ", # VMs: %s" %(numVM)

        # Get the size of each VMs list (chunksize)
        chunk = []
        if numLines >= numVM:
            chunksize = int(math.trunc(numLines/float(numVM)))
        else:
            chunksize = int(math.ceil(numLines/float(numVM)))

        print "Chunksize = %s" %(chunksize)

        for j, vm in enumerate(node_arr):
            chunk = list_arr[(j*chunksize):chunksize+(j*chunksize)]
            if len(chunk) > 0:
                #In case of last VM in list, append the remainder to its chunk
                if vm == node_arr[-1]:
                    remainder = list_arr[(numVM * chunksize):len(list_arr)]
                    for item in remainder:
                        chunk.append(item)
                process_chunk(hdr,chunk,in_list,vm)


if __name__ == "__main__":
    main()
