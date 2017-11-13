#!/usr/bin/python

# Import and function definitions
import os, sys, osgeo, subprocess as subp
from osgeo import ogr, osr, gdal
import argparse

def run_wait_os(cmdStr, print_stdOut=True):
    """
    Initialize OS command
    Wait for results (Communicate results i.e., make python wait until process is finished to proceed with next step)
    """
    import subprocess as subp

    Cmd = subp.Popen(cmdStr.rstrip('\n'), stdout=subp.PIPE, shell=True)
    stdOut, err = Cmd.communicate()

    if print_stdOut:
        print ("\tInitialized: %s" %(cmdStr))
        #print ("\t..Waiting for command to run...")
        print("\t" + str(stdOut) + str(err))
        print("\tEnd of command.")

def make_kml(fn):
    print("\tGenerating kml")
    kml_fn = os.path.splitext(fn)[0]+'.kml'

    cmdStr = "ogr2ogr -f KML {} {}".format(kml_fn, fn)
    cmd = subp.Popen(cmdStr, stdout=subp.PIPE, shell=True)
    s,e = cmd.communicate()

def getparser():
    parser = argparse.ArgumentParser(description="Create footprints of rasters files")
    parser.add_argument('ras_dir', default=None, help='Path to dir with raster to footprint')
    parser.add_argument('-ras_ext', default='.tif', help='The extension of rasters to be footprinted')
    parser.add_argument('-out_shp', default='raster_footprints', help='Output shapefile name of footprints')
    parser.add_argument('-file_fieldname', type=str, default='Name', help='String indicating the field name describing the files')
    parser.add_argument('-kml', action='store_true', help='Output kml of footprints for Google Earth')
    parser.add_argument('-c_pct', default='.25', type=str, help='The percent by which input pixel sizes will be coarsened (divided by)')
    return parser

def main():

    """Create/Update a directory's footprint shapefile.
        Produces a coarsened shapefile & KML version of the valid pixels from all geotiffs in a dir
    """
    parser = getparser()
    args = parser.parse_args()

    ras_dir = args.ras_dir
    ras_ext = args.ras_ext
    out_shp = args.out_shp
    file_fieldname = args.file_fieldname
    kml = args.kml
    c_pct = args.c_pct

    out_shp_fn = os.path.join(ras_dir,out_shp)

    if not out_shp.endswith('shp'):
        out_shp_fn += '.shp'

    print "\n\tRunning valid pixel footprints on: %s\n" %ras_dir

    # Collect raster feature names in working directory and subfolders therein
    ras_name_list = []
    pathroot = []
    ras_fn_list = []
    for root, dirs, files in os.walk(ras_dir):
        for f in files:
            if f.endswith(ras_ext) or f.endswith(ras_ext.upper()):
                ras_fn_list.append(os.path.join(root, f))
                ras_name_list.append(f)
                pathroot.append(root)

    for num, ras_fn in enumerate(ras_fn_list):
        dir_name,file_name = os.path.split(ras_fn)
        tmp1 = os.path.join(dir_name, "tmp1.tif")
        tmp1_sieve = os.path.join(dir_name, "tmp1_sieve.tif")
        tmp2 = os.path.join(dir_name, "tmp2.shp")
        tmp3 = os.path.join(dir_name, "tmp3.shp")
        tmp4 = os.path.join(dir_name, "tmp4.shp")
        tmp5 = os.path.join(dir_name, "tmp5.shp")
        tmp6 = os.path.join(dir_name, "tmp6.shp")

        if not os.path.isfile(ras_fn):
            print "\tWill not footprint: %s does not exist." %os.path.split(ras_fn)[1]
        else:
            if os.path.isfile("/att/gpfsfs/home/pmontesa/code/sqlite_fix.env"):
                cmdStr = "sqlite_fix.env"
                run_wait_os(cmdStr,print_stdOut=False)
            print "\tFootprinting: %s" %os.path.split(ras_fn)[1]
            print "\t %s of %s rasters " %(num+1,len(ras_fn_list))
            try:
                print "\tCOARSEN..."
                #nodata becomes 0, data becomes 255
                cmdStr = "gdal_translate -outsize {}% {}% -co compress=lzw -b 1 -ot byte -scale 1 1 {} {}".format(c_pct, c_pct, ras_fn, tmp1)
                run_wait_os(cmdStr,print_stdOut=False)
                print "\tSIEVE..."
                cmdStr = "gdal_sieve.py -8 -st 10 {} {}".format(tmp1,tmp1_sieve)
                run_wait_os(cmdStr,print_stdOut=False)
                print "\tPOLYGONIZE..."
                cmdStr = "gdal_polygonize.py -f 'ESRI Shapefile' {} {}".format(tmp1_sieve, tmp2)
                run_wait_os(cmdStr,print_stdOut=False)
                print "\tREMOVE NODATA HOLES..."
                cmdStr = "ogr2ogr {} {} -where 'DN>0' -overwrite".format(tmp3, tmp2)
                run_wait_os(cmdStr,print_stdOut=False)
                print "\tREPROJECT & MERGE..."
                cmdStr = "ogr2ogr -f 'ESRI Shapefile' -t_srs EPSG:3995 {} {} -overwrite".format(tmp4, tmp3)
                run_wait_os(cmdStr,print_stdOut=False)
                print "\tDISSOLVE/AGGREGATE INTO 1 FEATURE..."
                input_basename = os.path.split(tmp4)[1].replace(".shp","")
                cmdStr = "ogr2ogr {} {} -dialect sqlite -sql 'SELECT GUnion(geometry), DN FROM {} GROUP BY DN'".format(tmp5, tmp4, input_basename)
                run_wait_os(cmdStr,print_stdOut=False)
                print "\tSIMPLIFY..."
                cmdStr = "ogr2ogr {} {} -simplify .001".format(tmp6, tmp5)
                run_wait_os(cmdStr,print_stdOut=False)

                # Check to see if the file name exists in the 'Name' field of the output shp
                update = True
                if os.path.isfile(out_shp_fn):
                    # Open a Shapefile, and get field names
                    shp = ogr.Open(out_shp_fn, 1)
                    layer = shp.GetLayer()
                    layer_defn = layer.GetLayerDefn()
                    field_names = [layer_defn.GetFieldDefn(i).GetName() for i in range(layer_defn.GetFieldCount())]

                    for feature in layer:
                        if feature.GetField(file_fieldname) == file_name:
                            update = False
                            print "\tFootprint of %s already exists" %file_name
                            break
                        pass
                    shp, layer, layer_defn, field_names, feature = (None for i in range(5))

                if update and os.path.isfile(tmp6):
                    # Add fields to the pairname's shp (tmp6)
                    ##https://gis.stackexchange.com/questions/3623/how-to-add-custom-feature-attributes-to-shapefile-using-python
                    # Open a Shapefile, and get field names
                    shp = ogr.Open(tmp6, 1)
                    layer = shp.GetLayer()

                    # Set the field types
                    for new_field_str in [file_fieldname]:
                        if 'Name' in new_field_str or 'chm' in new_field_str:
                            fieldType = ogr.OFTString
                        elif 'year' or 'month' or 'day' in new_field_str:
                            fieldType = ogr.OFTInteger
                        else:
                            fieldType = ogr.OFTReal

                        # Add a new field
                        new_field = ogr.FieldDefn(new_field_str, fieldType)
                        layer.CreateField(new_field)

                    # Update fields based on attributes
                    ## http://www.digital-geography.com/create-and-edit-shapefiles-with-python-only/#.V8hlLfkrLRY
                    layer_defn = layer.GetLayerDefn()
                    field_names = [layer_defn.GetFieldDefn(i).GetName() for i in range(layer_defn.GetFieldCount())]
                    feature = layer.GetFeature(0)   # Gets first, and only, feature
                    for feature in layer:
                        for num, f in enumerate(field_names):
                            i = feature.GetFieldIndex(f)
                            if num > 0:
                                feature.SetField(i, os.path.split(ras_fn)[1]) #write the raster name to the attribute here
                                layer.SetFeature(feature)
                    shp = None

                    print "\tDISSOLVE/AGGREGATE INTO 1 FEATURE..AGAIN.."
                    input_basename = os.path.split(tmp4)[1].replace(".shp","")
                    cmdStr = "ogr2ogr {} {} -dialect sqlite -sql 'SELECT GUnion(geometry), {} FROM {} GROUP BY {}'".format(tmp5, tmp4, file_fieldname, input_basename, file_fieldname)
                    run_wait_os(cmdStr,print_stdOut=False)

                    # Append tmp6.shp to out_shp
                    if os.path.isfile(out_shp_fn):
                        print "\tUpdating footprint..."
                        cmdStr = "ogr2ogr -f 'ESRI Shapefile' -update -append {} {}".format(out_shp_fn, tmp6)  #-nln merge
                        run_wait_os(cmdStr,print_stdOut=False)
                    else:
                        print "\tCreating footprint shp: %s" %out_shp_fn
                        cmdStr = "ogr2ogr -f 'ESRI Shapefile' {} {}".format(out_shp_fn, tmp6)
                        run_wait_os(cmdStr,print_stdOut=False)

                # Clean up tmp files
                file_list = os.listdir(dir_name)
                for f in file_list:
                    if 'tmp' in str(f):
                        os.remove(os.path.join(dir_name,f))

            except Exception, e:
                print "\tFailed to footprint: %s" %ras_fn


    if kml and os.path.isfile(out_shp_fn):
        make_kml(out_shp_fn)

if __name__ == '__main__':
    main()
