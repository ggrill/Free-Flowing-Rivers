import cPickle
import multiprocessing
import os
import sys
import time

import arcpy
import numpy as np
import pandas as pd

import indices.dor
import tools.helper as tool
from config import config

fd = config.var


def run_dor(stamp, para, paths):
    """
    Set up for multiprocessing. Creates a isolated processing environment,
    where each hydrological river basin is processed separately

    :param stamp: timestamp
    :param para: parameters
    :param paths: pathnames
    :return:
    """

    dams_fc = para["dams_fc"]
    streams_fc = para["streams_fc"]

    update_mode = para["update_mode"]

    barrier_inc_field = para["barrier_inc_field"]
    dor_field = para["dor_field"]

    gdb_full_path = paths["gdb_full_path"]

    output_folder = para["output_folder"]
    output_folder = os.path.join(output_folder, "Results_" + stamp)

    scratch_ws = output_folder + r"\Scratch"

    tool.create_path(scratch_ws)

    in_basins = list(get_unique(dams_fc, barrier_inc_field))

    print dams_fc
    print ("Loading {}".format(str(streams_fc)))
    streams = load_streams(streams_fc, dor_field)
    dams_temp = load_dams(dams_fc, barrier_inc_field)

    pooled = True

    if pooled:
        pool = multiprocessing.Pool(8)

        jobs = []
        i = 1

        print ("Starting analysis pooled")
        for basin in in_basins:
            # https://stackoverflow.com/a/8533626/344647
            # Much faster than querying the global dataset on disk is
            # to load the global dataset into memory first and then query it
            # using numpy methods
            streams_sel = np.copy(streams[streams[fd.BAS_ID] == basin])

            # Also faster than querying the feature dataset on disk,
            # it is better to to load it once and then
            # use numpy indexing to get the dams we want
            dams_sel = np.copy(dams_temp[dams_temp[fd.BAS_ID] == basin])

            jobs.append(pool.apply_async(run_basin, (streams_sel, dams_sel, basin,
                                                     stamp + str(i), scratch_ws, dor_field)))
            i += 1

        pool.close()
        pool.join()

        out_basin = [job.get() for job in jobs]

    else:

        jobs = []
        i = 1

        print ("Starting analysis unpooled")
        for basin in in_basins:
            # https://stackoverflow.com/a/8533626/344647
            # Much faster than querying the global dataset on disk is
            # to load the global dataset into memory first and then query it
            # using numpy methods
            streams_sel = np.copy(streams[streams[fd.BAS_ID] == basin])

            # Also faster than querying the feature dataset on disk,
            # it is better to to load it once and then
            # use numpy indexing to get the dams we want
            dams_sel = np.copy(dams_temp[dams_temp[fd.BAS_ID] == basin])

            jobs.append(
                run_basin(streams_sel, dams_sel, basin, stamp + str(i), scratch_ws, dor_field))
            i += 1

        out_basin = [job for job in jobs]

    # Merge the temporary outputs
    print("Merging temporary outputs into output table %s ..." % gdb_full_path)

    i = 0
    tbl = {}
    for bas in out_basin:
        i += 1

        with open(bas, 'rb') as fp:
            tbl[i] = cPickle.load(fp)

    merged = np.concatenate(tbl.values(), 1)

    df = pd.DataFrame(merged)

    # Turn panda to numpy
    # https://my.usgs.gov/confluence/display/cdi/pandas.DataFrame+to+ArcGIS+Table
    x = np.array(np.rec.fromrecords(df.values))
    names = df.dtypes.index.tolist()
    x.dtype.names = tuple(names)

    output_table_location = gdb_full_path + "\\" + "dor"

    arcpy.da.NumPyArrayToTable(x, output_table_location)
    tool.add_index(lyr=merged, field_name="GOID")

    # Update automatically
    if update_mode == "YES":
        print("Updating dor values in database {} ".format(streams_fc))

        tool.copy_between(to_join_fc=streams_fc,
                          to_join_field="GOID",
                          IntoJoinField=dor_field,
                          FromJoinFC=output_table_location,
                          FromJoinField="GOID",
                          FromValueField=dor_field,
                          over_mode=True,
                          over_value=0)

    tool.delete_path(scratch_ws)


def run_basin(streams, dams, basin, stamp, scratchws, dor_field):
    """
    Calculate DOR for all barriers in a specified river basin

    :param streams:
    :param dams:
    :param basin:
    :param stamp:
    :param scratchws:
    :param dof_field:
    :param drf_upstream:
    :param drf_downstream:
    :param mode:
    :param use_dam_level_df:
    :return:
    """
    scratch_gdb = set_environment(scratchws, basin, stamp)

    tool.update_stream_routing_index(streams)
    tool.update_dam_routing_index(dams, streams)

    print ("Calculating DOR for basin {}".format(str(basin)))

    indices.dor.calculate_dor(dams, streams, dor_field)

    final_table = export(streams, basin, scratch_gdb)

    # Return only reference (path) to table
    return final_table


def get_unique(dam_table, inc_field):
    """
    Calculates a list of unique river basins that need to be processed based
    on the barriers to be considered.

    :param dam_table: umpy array with dams to process
    :param inc_field: field to determine dams to include
    :return: List of river basins
    """

    flds = [fd.BAS_ID, inc_field]
    whereBClause = inc_field + ' = 1'
    whereBClause = whereBClause.replace("'", "")

    dams = arcpy.da.TableToNumPyArray(
        dam_table, flds, whereBClause, null_value=-1)

    in_basins = np.unique(dams[fd.BAS_ID])

    if 0 in in_basins:
        sys.exit(
            "One of the dams has a basin ID of 0. BAS_ID cannot be zero. "
            "Please provide a BAS_ID other than 0 for "
            "all dams ")

    return in_basins


def set_environment(scratch_ws, basin, stamp):
    """
    Creates isolated output paths for specific basin using timestamp in scratch workspace.

    :param scratch_ws: scratch workspace
    :param basin:  river basin to process
    :param stamp: timestamp
    :return: fully specified pathname
    """

    out_folder = "basin_" + str(basin) + "_" + str(stamp)
    fullpath = os.path.join(scratch_ws, out_folder)

    tool.create_path(fullpath)
    return fullpath


def load_dams(dam_table, inc_field):
    """
    This function loads from the database

    :param dam_table:
    :param inc_field:
    :param use_dam_level_df:
    :return: numpy array with dams
    """
    flds = [fd.BAS_ID, fd.GOID, fd.STOR_MCM, fd.INC, inc_field]

    tool.check_fields(dam_table, flds + [fd.INC])

    whereBClause2 = inc_field + ' > 0'
    whereBClause3 = ' AND ' + fd.INC + ' > 0'
    whereBClause4 = whereBClause2 + whereBClause3
    whereBClause5 = whereBClause4.replace("'", "")

    dams = arcpy.da.TableToNumPyArray(dam_table, flds, whereBClause5, null_value=0)

    return dams


def load_streams(stream_table, dor_field):
    """
    Loads the streams and adds a field for holding the DOF values

    :param stream_table: numpy array representing the river reaches
    :param dor_field: field name to store DOR results
    :return:
    """

    flds = [fd.BAS_ID, fd.GOID, fd.NOID, fd.NDOID, fd.NUOID, fd.RIV_ORD,
            fd.DIS_AV_CMS, fd.HYFALL]

    tool.check_fields(stream_table, flds)

    arr = arcpy.da.TableToNumPyArray(stream_table, flds, null_value=0)
    arr = tool.add_fields(arr, [(str(dor_field), 'f4')])
    arr[dor_field] = 0
    return arr


def export(streams, basin, folder):
    suffix = ".bas"
    name = "out_" + str(basin)
    fullname = name + suffix

    fullpath = os.path.join(folder, fullname)

    tool.save_as_cpickle(pickle_object=streams,
                         folder=folder,
                         name=name,
                         file_extension=suffix)
    return fullpath


def create_gdb_workspace(gdb_folder, gdb_name):
    """
    Creates a path and a geodatabase with timestamp

    :param stamp:
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
