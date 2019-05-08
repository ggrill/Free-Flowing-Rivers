import cPickle
import datetime
import logging
import os
import shutil
import stat
import sys
import time
from collections import defaultdict

import arcpy
import numpy as np
import pandas as pd
# Creates an global object from the config class var. This holds the
# field and path names names. For example fd.RIV_ORD returns the name of
# the field holding information about the discharge stream order
from config import config

fd = config.var


def test_64():
    print("Running against: {}".format(sys.version))
    if sys.maxsize > 2 ** 32:
        print("Running python 64 bit")
    else:
        print("Running python 32 bit")


def get_stamp():
    """
    Creates a time stamp in a specific format
    :return: timestamp
    """
    stamp1 = int(
        datetime.datetime.fromtimestamp(time.time()).strftime('%y%m%d'))
    stamp21 = int(datetime.datetime.fromtimestamp(time.time()).strftime('%H'))
    stamp22 = int(datetime.datetime.fromtimestamp(time.time()).strftime('%M'))
    stamp23 = int(datetime.datetime.fromtimestamp(time.time()).strftime('%S'))
    stamp3 = int(datetime.datetime.fromtimestamp(time.time()).strftime('%f'))

    stamp = str(stamp1) + "_" + str(stamp21).zfill(2) + str(stamp22).zfill(
        2) + str(stamp23).zfill(2) + "_" + str(
        stamp3)

    return stamp


def check_fields(table, flds):
    """

    Check if multiple fields exist

    :param table: Table to check
    :param flds: Fields to check

    """
    fld_list = arcpy.ListFields(table)
    x = [a.name for a in fld_list]

    count_wrong = 0
    for fld in flds:
        if fld not in x:
            print ("Field {} does not exist".format(fld))

            count_wrong += 1

    if count_wrong > 0:
        print ("There were {} fields missing from the table. "
               "Please fix add the fields to the table before "
               "proceeding".format(count_wrong))
        sys.exit()


def check_field(table, fld):
    """
    Checks if one field exists

    :param table: Table to check
    :param fld: Field to check

    """
    fld_list = arcpy.ListFields(table)
    x = [a.name for a in fld_list]

    if fld not in x:
        return True
    else:
        return False


def check_esri_item(path_to_item):
    """
    Checks if an ESRI item exists

    :param path_to_item:
    :return:
    """

    if arcpy.Exists(path_to_item):
        pass
    else:
        arcpy.AddMessage("%s does not exist" % path_to_item)
        sys.exit(-99)


def create_path(path):
    """
    Checks if a path exists

    :param path: path to check
    :return: nada
    """
    try:
        os.makedirs(path)
    except Exception as ErrorDesc:
        print ("Error creating path")
        print (str(ErrorDesc))


def copytree(src, dst, symlinks=False, ignore=None):
    """
    Copies an entire directory, in this case the code, into the output
    directory

    see https://stackoverflow.com/a/22331852/344647

    :param src: source directory
    :param dst: destination directory
    :param symlinks:
    :param ignore:
    :return:
    """
    if not os.path.exists(dst):
        os.makedirs(dst)
        shutil.copystat(src, dst)
    lst = os.listdir(src)
    if ignore:
        excl = ignore(src, lst)
        lst = [x for x in lst if x not in excl]
    for item in lst:
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if symlinks and os.path.islink(s):
            if os.path.lexists(d):
                os.remove(d)
            os.symlink(os.readlink(s), d)
            try:
                st = os.lstat(s)
                mode = stat.S_IMODE(st.st_mode)
                os.lchmod(d, mode)
            except Exception as ErrorDesc:
                print ("Error copying tree")
                print(str(ErrorDesc))

        elif os.path.isdir(s):
            try:
                copytree(s, d, symlinks, ignore)
            except Exception as e:
                print (str(e))
                print ("Perhaps the EXCEL file is still open?")
        else:
            try:
                shutil.copy2(s, d)
            except Exception as e:
                print (str(e))
                print ("Perhaps the EXCEL file is still open?")


def add_fields(array, desc):
    """
    Adds fields to a numpy array

    see: https://stackoverflow.com/a/1201821/344647

    :param array:
    :param desc:
    :return:
    """
    if array.dtype.fields is None:
        arcpy.AddMessage("A must be a structured numpy array")

    b = np.empty(array.shape, dtype=array.dtype.descr + desc)
    for name in array.dtype.names:
        b[name] = array[name]
    return b


def delete_field(esri_table, list_of_field_names):
    """

    Deletes a field

    :param esri_table: esri feature class or table
    :param list_of_field_names: field name to delete
    :return:
    """

    for f in list_of_field_names:
        if check_field(table=esri_table, fld=f):
            try:
                arcpy.DeleteField_management(esri_table, f)
                print ("Deleted %s" % f)

            except Exception as ErrorDesc:
                print ("Error deleting a field " + f)
                print(str(ErrorDesc))


def add_index(lyr, field_name):
    try:
        if not index_exists(lyr, field_name):
            arcpy.AddIndex_management(lyr, field_name, field_name, "UNIQUE", "ASCENDING")
    except Exception as error:
        print ("Adding index {} not successful. {}".format(field_name, str(error)))


def index_exists(tablename, indexname):
    if not arcpy.Exists(tablename):
        return False

    tabledescription = arcpy.Describe(tablename)

    for iIndex in tabledescription.indexes:
        if iIndex.Name == indexname:
            return True
    return False


def update_stream_routing_index(streams):
    """
    Function to recalculate the Network OIDs of a stream network, if the
    stream network was extracted from the global network. In this case the
    NOIDs used for the routing are out of sync with the object ids and
    with the numpy indices. The object with the network id 1 must be the
    first object in the list etc.

    args:
        streams(numpy array): numpy array representing the stream network
    return:
        Updates and returns the stream network with new network ids
    """

    for n in ["GOID", "NOID", "NDOID", "NUOID"]:
        if not n in streams.dtype.names:
            raise Exception("Field {} does not exist".format(n))

    # river network must be global or extracted from global. NOID NDOID and
    # NUOID must not be sorted
    if streams["NOID"][0] != streams["GOID"][0]:
        raise Exception("It seems like you are attempting to resort "
                        "a network that has already been sorted. "
                        "This would be confusing, please use an extract of "
                        "the original network")

    # print("Updating routing index")
    # Create Routing Dictionaries and fill

    oidDict = {}
    upsDict = defaultdict(str)
    downDict = defaultdict(long)

    i = 1
    for myrow in streams:
        oidDict[int(myrow[fd.GOID])] = i
        i += 1

    i = 1
    for myrow in streams:
        dnOldOID = myrow[fd.NDOID]
        newOid = oidDict.get(int(dnOldOID), -1)

        if newOid != -1:
            # Write OID of next downstream reach
            downDict[i] = newOid
            # Write OID of next upstream reach
            exValue = upsDict.get(int(newOid), -99)

            if exValue == -99:
                upsDict[int(newOid)] = i
            else:
                newV = str(exValue) + '_' + str(i)
                upsDict[int(newOid)] = newV
        i += 1

    # Writing index values back to numpy
    i = 1
    for myrow in streams:
        myrow[fd.NOID] = i
        myrow[fd.NDOID] = downDict[i]
        myrow[fd.NUOID] = upsDict[i]

        i = i + 1

    return streams


def delete_path(path):
    try:
        if os.path.exists(path):
            shutil.rmtree(path)

    except Exception as err:
        print ("Cannot remove folder")
        print (str(err))


def create_path(path):
    try:
        if not os.path.exists(path):
            os.makedirs(path)
    except Exception as err:
        print ("Cannot make directory")
        print (str(err))


def copy_between(to_join_fc, to_join_field, IntoJoinField, FromJoinFC,
                 FromJoinField, FromValueField, over_mode=True,
                 over_value=0):
    """
    Copies data between tables
    """

    print ("Updating tables")

    from_valueDict = defaultdict(float)

    to_flds = [to_join_field, IntoJoinField]

    # Loading the values from FROM table
    from_flds = [FromJoinField, FromValueField]
    with arcpy.da.SearchCursor(FromJoinFC, from_flds) as cursor:
        for myrow in cursor:
            from_valueDict[myrow[0]] = myrow[1]

    # Writing values to TO table
    with arcpy.da.UpdateCursor(to_join_fc, to_flds) as cursor:
        for row1 in cursor:
            ToJoinFieldValue = row1[0]
            newValue = from_valueDict.get(ToJoinFieldValue)

            if newValue is None:
                if over_mode is True:
                    # This overwrites the values in the TO table
                    # with zero values for IDs not present in FROM table
                    # Default for updating DOR DOF results

                    # If you just want to update certain values from one table
                    # to the other and leave all other rows untouched,
                    # then use the "update" over_mode

                    row1[1] = over_value
            else:
                row1[1] = newValue

            cursor.updateRow(row1)


def get_writer(base_dir, stamp):
    # Setup a Excel Writer target file
    excel_file_output = os.path.join(base_dir, "results_" + stamp + ".xls")
    return pd.ExcelWriter(excel_file_output), excel_file_output


def export_excel(df, name, writer, id=False, startrow=0,
                 header=False, first=False):
    try:
        startrow = len(writer.sheets[name].rows)
    except BaseException:
        startrow = 0

    if startrow == 0:
        header = True
    else:
        header = False

    if first:
        header = False

    df.to_excel(
        writer,
        name,
        index=False,
        encoding='utf-8',
        startrow=startrow,
        header=header)
    writer.save()


def create_results_sheet(writer):
    result_list = ["Stamp",
                   "Sce_name",
                   "I_1",
                   "I_2",
                   "I_3",
                   "I_4",
                   "I_5",
                   "I_6",
                   "W_1",
                   "W_2",
                   "W_3",
                   "W_4",
                   "W_5",
                   "W_6",
                   "Threshold",
                   "Flood_weight",
                   "Export",
                   "Filter_Threshold",
                   "Sce_name",
                   "Count_reaches",
                   "Count_reaches_affected",
                   "Perc_reaches_affected",
                   "Mean_csi",
                   "count_nff",
                   "perc_nff",
                   "Bench_match"]
    results_pd = pd.DataFrame(result_list).T
    results_pd.drop(results_pd.index[[0]])

    export_excel(results_pd, "Global_stats", writer, first=True)

    dom_list = ["Stamp",
                "Sce_name",
                "Pressure",
                "Number of reaches"]
    dom_pd = pd.DataFrame(dom_list).T
    dom_pd.drop(dom_pd.index[[0]])
    export_excel(dom_pd, "Global_dom", writer, first=True)

    dom_bench_list = ["Stamp",
                      "Sce_name",
                      "FFR_ID",
                      "Rivername",
                      "Pressure",
                      "Number of reaches",
                      "Source"
                      ]
    dom_bench_pd = pd.DataFrame(dom_bench_list).T
    dom_bench_pd.drop(dom_bench_pd.index[[0]])
    export_excel(dom_bench_pd, "Bench_dom", writer, first=True)


def load_parameters(path):
    start_dict = defaultdict()
    value_dict = defaultdict()

    # Check spreadsheet

    # Does it exist? Opening Excel sheet
    try:
        xls = pd.ExcelFile(path)
        sheet_names = xls.sheet_names
    except Exception as err:
        print ("Error opening file. Does it exist? {}").format(str(err))
        sys.exit()

    # Checking the sheets.
    # Does it have the start sheet?

    if "START" not in sheet_names:
        print ("\r\n" + "Sheet START does not exist")
        sys.exit()

    if len(sheet_names) < 3:
        print ("There must be at least four sheets in the workbook.")
        print ("1. The sheet 'Description of variables'")
        print ("2. The sheet 'START'")
        print ("3. The sheet 'SET' (Settings)")
        print ("4. The sheet 'SCE' (Scenarios)")
        sys.exit()

    try:
        settings_sheet = xls.parse("START")
    except Exception as e:
        print ("Does sheet 'START' exist? {}").format(str(e))
        sys.exit()

    var1 = settings_sheet['Key']
    var2 = settings_sheet['Value']

    ind = 0
    for fname in var1:
        start_dict[fname] = var2[ind]
        ind += 1

    for sname in ["settings_sheet", "scenarios_sheet", "run_dof", "run_dor", "run_sed", "run_csi"]:
        if sname not in start_dict.keys():
            print "\r\n" + "Start variable not found: {}".format(sname)
            sys.exit()

    set_sheet = start_dict["settings_sheet"]
    sce_sheet = start_dict["scenarios_sheet"]

    sequence = {"run_dof": start_dict["run_dof"],
                "run_dor": start_dict["run_dor"],
                "run_sed": start_dict["run_sed"],
                "run_csi": start_dict["run_csi"]}

    # Load project values
    try:
        settings_sheet = xls.parse(set_sheet)
    except Exception as e:
        print (str(e))
        print "Does sheet {} exist?".format(set_sheet)
        sys.exit()

    var1 = settings_sheet['Key']
    var2 = settings_sheet['Value']

    ind = 0
    for fname in var1:
        value_dict[fname] = var2[ind]
        ind += 1

    # Load scenario list
    sce_list = []

    try:
        sce_sheet = xls.parse(sce_sheet)
    except Exception as e:
        print (str(e))
        print "Does sheet {} exist?".format(sce_sheet)
        sys.exit()

    # Check if fields are there

    header = ["scenario_name",
              "indicator_1",
              "indicator_2",
              "indicator_3",
              "indicator_4",
              "indicator_5",
              "indicator_6",
              "weight_1",
              "weight_2",
              "weight_3",
              "weight_4",
              "weight_5",
              "weight_6",
              "csi_thresh",
              "fld_damp",
              "filter_thresh",
              "to_process",
              "to_export"]

    for h in header:
        if h not in sce_sheet.columns:
            print "{} not found".format(h)

    field_set = set()

    for index, row in sce_sheet.iterrows():
        scename = row[0]
        list_of_fields = [row[1], row[2], row[3], row[4], row[5], row[6]]
        list_of_weights = [row[7], row[8], row[9], row[10], row[11], row[12]]
        csi_thresh = row[13]
        flood_damp = row[14]
        filter_thresh = row[15]
        process = row[16]
        export = row[17]

        field_set |= set(list_of_fields)

        sce_list.append([scename, list_of_fields, list_of_weights,
                         csi_thresh, flood_damp, filter_thresh, process,
                         export])

    return sequence, value_dict, sce_list, list(field_set)


def load_stream_array(stream_feature_class, stream_fields, use_npy=0,
                      fname=""):
    """
    Loading input stream feature class (river network) to numpy array.
    Optionally, the stream feature class can bes saved as a numpy memory
    object and loaded instead, which can be much more efficient (depending
    on extent)

    :param stream_feature_class:
    :param stream_fields:
    :param use_npy:
    :param fname:
    :return:
    """

    def save_arr(arr, loc):
        try:
            np.save(loc, arr)
        except BaseException:
            print ("Unexpected error:", sys.exc_info()[0])
            raise

    def load_arr(loc):
        try:
            return np.load(loc)
        except BaseException:
            print ("Unexpected error:", sys.exc_info()[0])
            raise

    if use_npy == 1:
        if os.path.isfile(fname):
            print ("Loading stream array from npy object")
            arr = load_arr(fname)
        else:
            print ("Loading stream array from feature class, then save as "
                   "array")
            arr = arcpy.da.TableToNumPyArray(stream_feature_class,
                                             stream_fields)
            print ("Saving stream array to npy object")
            save_arr(arr, fname)
    else:
        print ("Loading stream array from feature class")
        arr = arcpy.da.TableToNumPyArray(stream_feature_class, stream_fields)

    # Test if Global network or extracted network
    # If extracted, the routing index must be updated
    update_stream_routing_index(arr)
    return arr


def create_gdb(gdb_folder, gdb_name):
    """
    Creates a path and a geodatabase

    :param gdb_folder:
    :param gdb_name:
    :return:
    """

    if not os.path.exists(gdb_folder):
        os.makedirs(gdb_folder)

    gdb_file_name = gdb_name + ".gdb"
    gdb_full_path = gdb_folder + "\\" + gdb_file_name

    arcpy.CreateFileGDB_management(gdb_folder, gdb_file_name)

    return gdb_full_path, gdb_file_name


def export_joined(output_geodatabase_path, output_table_name, table_to_join,
                  join_table):
    """
    Joins the temporary output to the join_fc_lyr feature class and writes it to geodatabase
    """

    arcpy.env.qualifiedFieldNames = False  # to maintain proper field names

    print("Adding indices")

    check_esri_item(join_table)
    add_index(lyr=join_table, field_name="GOID")
    add_index(lyr=table_to_join, field_name="GOID")

    csi_fc = output_geodatabase_path + "\\" + output_table_name

    # Copy the join feature class to the output destination
    print("Joining features")
    try:
        join_name = "join_fc_lyr" + str(output_table_name)
        join_fc_lyr = arcpy.MakeFeatureLayer_management(join_table, join_name)
        joined = arcpy.AddJoin_management(
            join_fc_lyr, "GOID", table_to_join, "GOID", "KEEP_ALL")

        print("Exporting features")

        arcpy.CopyFeatures_management(joined, csi_fc)

        delete_field(esri_table=csi_fc,
                     list_of_field_names=["OBJECTID_1", "GOID_1"])

    except Exception as err:
        print ("Error joining or exporting feature class. {}".format(str(err)))
        sys.exit(0)

    return csi_fc


def get_ffr_field_names(name):
    ffr_stat1 = str(name) + "_FF1"
    ffr_stat2 = str(name) + "_FF2"
    ffr_dis = str(name) + "_FFID"

    new_fields = [(ffr_stat1, 'i4'), (ffr_stat2, 'i4'), (ffr_dis, 'u4')]

    return ffr_stat1, ffr_stat2, ffr_dis, new_fields


def get_csi_field_names(name):
    csi_name = str(name)
    dom_name = str(name) + "_D"
    ff_thresh = str(name) + "_FF"

    new_fields = [(csi_name, 'f8'), (dom_name, '|S3'), (ff_thresh, 'i4')]

    return csi_name, dom_name, ff_thresh, new_fields


def setup_logging(output_folder):
    logfile_path = os.path.join(output_folder, "log.txt")

    logging.basicConfig(
        level=logging.DEBUG,
        filename=logfile_path,
        filemode="a+",
        format="%(asctime)-15s %(message)s")
    return logging


def pd_to_np(nparay, list_of_fields="all"):
    """

    # Turn panda to numpy array
    # https://my.usgs.gov/confluence/display/cdi/pandas.DataFrame+to+ArcGIS+Table

    :param nparay:
    :param list_of_fields:
    :return:
    """
    panda_df = pd.DataFrame(nparay)

    if list_of_fields != "all":
        panda_df = panda_df[list_of_fields]

    # Turn panda to numpy
    # https://my.usgs.gov/confluence/display/cdi/pandas.DataFrame+to+ArcGIS+Table
    x = np.array(np.rec.fromrecords(panda_df.values))
    names = panda_df.dtypes.index.tolist()
    x.dtype.names = tuple(names)

    return x


def remove_csi_traces(fc, name):
    """
    Removes all traces of MIN_, MAX_ etc from dissolve operation output
    For example, the fields generated for each scenario will have the name
    "CSI001", "CSI001_D", "CSI001_FF" etc, if the scenario name (as defined in
    the Excel is "CSI001". To make mapping easier in the GIS, we remove the
    scenario names with "CSI". The results afterwards will be
    "CSI", "CSI_D", "CSI_FF"

    :param fc: feature class
    :param name:
    :return:
    """

    rename_list = [name]

    fieldList = arcpy.ListFields(
        fc)  # get a list of fields for each feature class
    for field in fieldList:  # loop through each field
        for ren in rename_list:

            if field.name.startswith(ren):
                fn = field.name.replace(ren, 'CSI')
                arcpy.AlterField_management(fc, field.name, fn, fn)


def save_as_cpickle(pickle_object, folder, name, file_extension):
    outfile = os.path.join(folder, str(name) + file_extension)
    with open(outfile, 'wb') as fp:
        cPickle.dump(pickle_object, fp)


def update_dam_routing_index(dams, arr):
    """
    Function to recalculate the Global Network OIDs to match with the
    reduced network data set

    args:
        dams(numpy array): numpy array representing a list of dams
        arr(numpy array): numpy array representing the stream network
    return:
        Updates and returns the dam numpy array with new network ids
    """
    for dam in dams:
        oldGOID = dam[fd.GOID]
        find = arr[(arr[fd.GOID]) == oldGOID]
        new_goid = find[fd.NOID]
        dam[fd.GOID] = new_goid
