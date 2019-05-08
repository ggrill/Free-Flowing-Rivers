import cPickle
import multiprocessing
import os
import sys
import time

import arcpy
import numpy as np
import pandas as pd

import indices.dof
import tools.helper as tool
from config import config

fd = config.var


def run_dof(stamp, para, paths):
    """
    Set up for multiprocessing. Creates a isolated processing environment,
    where each hydrological river basin is processed separately

    :param stamp: timestamp
    :param para: parameters
    :param paths: pathnames
    :return:
    """

    dam_fc = para["dams_fc"]
    streams_fc = para["streams_fc"]

    update_stream_mode = para["update_mode"]

    drf_upstream = para["drf_upstream"]
    drf_downstream = para["drf_downstream"]

    barrier_inc_field = para["barrier_inc_field"]

    dof_field = para["dof_field"]
    dof_mode = para["dof_mode"]

    use_dam_level_df = para["use_dam_level_df"]

    gdb_full_path = paths["gdb_full_path"]

    output_folder = para["output_folder"]
    output_folder = os.path.join(output_folder, "Results_" + stamp)

    scratch_ws = output_folder + r"\Scratch"

    tool.delete_path(scratch_ws)
    tool.create_path(scratch_ws)

    print ("Discharge range factor used (upstream): %s" % drf_upstream)
    print ("Discharge range factor used (downstream): %s" % drf_downstream)

    in_basins = list(get_unique(dam_fc, barrier_inc_field))

    print ("Loading {}".format(str(streams_fc)))
    streams = load_streams(streams_fc, dof_field)
    dams_temp = load_dams(dam_fc, barrier_inc_field, use_dam_level_df)

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
                                                     stamp + str(i),
                                                     scratch_ws,
                                                     dof_field,
                                                     drf_upstream,
                                                     drf_downstream,
                                                     dof_mode,
                                                     use_dam_level_df)))

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

            jobs.append(run_basin(streams_sel,
                                  dams_sel,
                                  basin,
                                  stamp + str(i),
                                  scratch_ws,
                                  dof_field,
                                  drf_upstream,
                                  drf_downstream,
                                  dof_mode,
                                  use_dam_level_df))
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

    output_table_location = gdb_full_path + "\\" + "dof"

    arcpy.da.NumPyArrayToTable(x, output_table_location)
    tool.add_index(lyr=merged, field_name="GOID")

    # Update automatically
    if update_stream_mode.lower() == "yes":
        print "Updating dof values in database {} ".format(streams_fc)

        tool.copy_between(to_join_fc=streams_fc,
                          to_join_field="GOID",
                          IntoJoinField=dof_field,
                          FromJoinFC=output_table_location,
                          FromJoinField="GOID",
                          FromValueField=dof_field,
                          over_mode=True,
                          over_value=0)

    tool.delete_path(scratch_ws)


def run_basin(streams, dams, basin, stamp, scratchws, dof_field, drf_upstream,
              drf_downstream, mode, use_dam_level_df):
    """
    Calculate DOF for all barriers in a specified river basin

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

    # Setup isolated path and environment
    temp_out_folder = set_environment(scratchws, basin, stamp)

    # Update network ids for rivers and dams
    tool.update_stream_routing_index(streams)
    tool.update_dam_routing_index(dams, streams)

    # Calculate and write DOF into designated field
    indices.dof.calculate_DOF(dams, streams, mode, dof_field,
                              drf_upstream, drf_downstream, use_dam_level_df)

    # Export table to temporay geodatabase
    final_table = export(streams, basin, temp_out_folder)

    # returns the path to table for later merging
    return final_table


def get_unique(dam_table, inc_field):
    """
    Calculates a list of unique river basins that need to be processed based
    on the barriers to be considered.

    :param dam_table: numpy array with dams to process
    :param inc_field: field to determine dams to include
    :return: List of river basins
    """

    flds = [fd.BAS_ID, inc_field]
    where_clause = inc_field + ' = 1'
    where_clause = where_clause.replace("'", "")

    dams = arcpy.da.TableToNumPyArray(
        dam_table, flds, where_clause, null_value=-1)

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


def load_dams(dam_table, inc_field, use_dam_level_df):
    """
    This function loads from the database

    :param dam_table:
    :param inc_field:
    :param use_dam_level_df:
    :return: numpy array with dams
    """
    if use_dam_level_df.lower() == "yes":

        flds = [fd.BAS_ID, fd.GOID, fd.STOR_MCM, fd.DFU, fd.DFD,
                inc_field]

    else:
        flds = [fd.BAS_ID, fd.GOID, fd.STOR_MCM, inc_field]

    tool.check_fields(dam_table, flds)

    whereBClause2 = inc_field + ' > 0'
    whereBClause3 = ' AND ' + fd.INC + ' > 0'
    whereBClause4 = whereBClause2 + whereBClause3
    whereBClause5 = whereBClause4.replace("'", "")

    dams = arcpy.da.TableToNumPyArray(
        dam_table, flds, whereBClause5, null_value=0)

    return dams


def load_streams(stream_table, dof_field):
    """
    Loads the streams and adds a field for holding the DOF values

    :param stream_table: numpy array representing the river reaches
    :param dof_field: field name to store DOF results
    :return:
    """
    flds = [fd.BAS_ID, fd.GOID, fd.NOID, fd.NDOID, fd.NUOID, fd.RIV_ORD,
            fd.DIS_AV_CMS, fd.HYFALL]

    tool.check_fields(stream_table, flds)

    arr = arcpy.da.TableToNumPyArray(stream_table, flds, null_value=0)
    arr = tool.add_fields(arr, [(str(dof_field), 'f4')])
    arr[dof_field] = 0
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
    Creates a path and a geodatabase with timestamp as name

    :param stamp:
    :param gdb_folder:
    :param gdb_name:
    :return: full path and geodatabase name
    """

    if not os.path.exists(gdb_folder):
        os.makedirs(gdb_folder)

    gdb_file_name = gdb_name + ".gdb"
    gdb_full_path = gdb_folder + "\\" + gdb_file_name

    arcpy.CreateFileGDB_management(gdb_folder, gdb_file_name)

    return gdb_full_path, gdb_file_name
