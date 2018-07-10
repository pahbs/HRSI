#!/usr/bin/env python
# Import and function definitions
import psycopg2
import csv
import argparse
import os, errno
import shutil

def force_symlink(file1, file2):
    try:
        os.symlink(file1, file2)
    except OSError, e:
        if e.errno == errno.EEXIST:
            os.remove(file2)
            os.symlink(file1, file2)

def getparser():
    parser = argparse.ArgumentParser(description="Query NGAdb with a catalog id")
    parser.add_argument('catID', default=None, type=str, help='Input catid')
    parser.add_argument('-prod_code', default='P1BS', type=str, help='Image production code: P1BS or M1BS')
    parser.add_argument('-out_dir', default='/att/pubrepo/DEM/hrsi_dsm', type=str, help='Output pairname dir for symlinks')
    return parser

def main():

    parser = getparser()
    args = parser.parse_args()

    catID = args.catID
    prod_code = args.prod_code
    out_dir = args.out_dir

    """
    Returns to stdout the ADAPT dir that has the imagery associated with the input catID
    If out_dir specified, copies all images and xmls associated with catid into out_dir as symbolic links
    """
    # Search ADAPT's NGA database for catID
    imglist=[]
    with psycopg2.connect(database="ngadb01", user="anon", host="ngadb01", port="5432") as dbConnect:

        cur = dbConnect.cursor() # setup the cursor

        selquery =  "SELECT s_filepath, sensor, acq_time, cent_lat, cent_long FROM nga_inventory WHERE catalog_id = '%s' AND prod_code = '%s'" %(catID, prod_code)
        print( "\n\t Now executing database query on catID '%s' ..."%catID)
        cur.execute(selquery)
        selected=cur.fetchall()
        print( "\n\t Found '%s' scenes for catID '%s' "%(len(selected),catID))

        # This will only get the data that match the first prod_id, preventing replicated data from being copied. This should prevent mosaics from failing
        prod_id = selected[0][0].split('-')[-1].split('_')[0]
        print( "\t Creating symlinks for data associated with prod_id '%s'" %(prod_id))

        for i in range(0,len(selected)):
            #print(selected[i][0])

            if prod_id in selected[i][0]:
                imglist.extend(selected[i][0])

                #Copy ntf and xml to out_dir as symlinks
                filename = os.path.split(selected[i][0])[1]
                print("\t '%s'"  %(filename))
                force_symlink( selected[i][0], os.path.join(out_dir, filename) )
                try:
                    # shutil.Error: ... are the same file
    			    # Just copy over the xmls, instead of creating a symlink to them
                    shutil.copy2(os.path.splitext(selected[i][0])[0]+'.xml', out_dir)
                except Exception, e:
                    force_symlink( os.path.splitext(selected[i][0])[0]+'.xml', os.path.join(out_dir, os.path.splitext(filename)[0]+'.xml') )

        #return(imglist)
        # Print to stdout the dir from first record selected
        print(os.path.split(selected[0][0])[0])

if __name__ == "__main__":
    main()
