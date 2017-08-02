#!/usr/bin/env python
# Import and function definitions
import psycopg2
import csv
import argparse

def getparser():
    parser = argparse.ArgumentParser(description="Query NGAdb with a catalog id")
    parser.add_argument('catID', default=None, type=str, help='Input catid')
    return parser

def main():

    parser = getparser()
    args = parser.parse_args()

    catID = args.catID

    """
    catID
    """
    # [4] Search ADAPT's NGA database for catID
    #
    # Establish the database connection


    with psycopg2.connect(database="ngadb01", user="anon", host="ngadb01", port="5432") as dbConnect:

        cur = dbConnect.cursor() # setup the cursor


        selquery =  "SELECT s_filepath, sensor, acq_time, cent_lat, cent_long FROM nga_inventory WHERE catalog_id = '%s'" %(catID)
        print( "\n\t Now executing database query on catID '%s' ..."%catID)
        cur.execute(selquery)
        selected=cur.fetchall()
        print( "\n\t Found '%s' scenes for catID '%s' "%(len(selected),catID))

if __name__ == "__main__":
    main()
