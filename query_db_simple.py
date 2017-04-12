#-------------------------------------------------------------------------------
# Name:        query_db_simple.py
# Purpose:
#
# Author:      pmontesa
#
# Created:     08/12/2016
# Copyright:   (c) pmontesa 2016
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import psycopg2
import csv


def main(csvlist):
    """
    csvlist     a column with a header as a csv file
    """
    # [4] Search ADAPT's NGA database for catID_1 and catid_2
    #
    # Establish the database connection

    with open(csvlist, 'rb') as inCSV:
        reader = csv.reader(inCSV)
        catIDlist = list(reader)

    catIDlist = catIDlist[1:] # remove header

    with psycopg2.connect(database="ngadb01", user="anon", host="ngadb01", port="5432") as dbConnect:

        cur = dbConnect.cursor() # setup the cursor

        # Setup and execute the query on both catids of the stereopair indicated with the current line of the input CSV
        for num, catID in enumerate(catIDlist):

##            if '[' and ']' in catID:
##                catID = catID.replace('[','').replace(']','')

            catID = catID[0]

            selquery =  "SELECT s_filepath, sensor, acq_time, cent_lat, cent_long FROM nga_files_footprint_v2 WHERE catalog_id = '%s'" %(catID)
            print( "\n\t Now executing database query on catID '%s' ..."%catID)
            cur.execute(selquery)
            selected=cur.fetchall()
            print( "\n\t Found '%s' scenes for catID '%s' "%(len(selected),catID))

if __name__ == "__main__":
    import sys
    main( sys.argv[1] )
