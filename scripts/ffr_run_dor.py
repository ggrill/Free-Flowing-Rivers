import pickle as cPickle
import multiprocessing
import os
import sys
import time

import geopandas as gpd

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

    dams_fc:gpd.GeoDataFrame = para["dams_fc"]
    streams_fc:gpd.GeoDataFrame = para["streams_fc"]

    update_mode:str = para["update_mode"]

    barrier_inc_field = para["barrier_inc_field"]
    dor_field = para["dor_field"]

    gpkg_full_path = paths["gpkg_full_path"]

    output_folder = para["output_folder"]
    output_folder = os.path.join(output_folder, "Results_" + stamp)

    scratch_ws = output_folder + r"\Scratch"

    tool.create_path(scratch_ws)

    in_basins = list(get_unique(dams_fc, barrier_inc_field))

    print(dams_fc.head(1))
    print (f"Loading streams")
    streams = load_streams(streams_fc, dor_field)
    dams_temp = load_dams(dams_fc, barrier_inc_field)

    num_cores = os.cpu_count()
    pooled = num_cores > 1

    if pooled:
        pool = multiprocessing.Pool(num_cores)

        jobs = []
        i = 1

        print ("Starting analysis pooled")
        for basin in in_basins:
            # https://stackoverflow.com/a/8533626/344647
            # Much faster than querying the global dataset on disk is
            # to load the global dataset into memory first and then query it
            # using numpy methods
            streams_sel = streams[streams[fd.BAS_ID] == basin]

            # Also faster than querying the feature dataset on disk,
            # it is better to to load it once and then
            # use numpy indexing to get the dams we want
            dams_sel = dams_temp[dams_temp[fd.BAS_ID] == basin]

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
            streams_sel = streams[streams[fd.BAS_ID] == basin]

            # Also faster than querying the feature dataset on disk,
            # it is better to to load it once and then
            # use numpy indexing to get the dams we want
            dams_sel = dams_temp[dams_temp[fd.BAS_ID] == basin]

            jobs.append(
                run_basin(streams_sel, dams_sel, basin, stamp + str(i), scratch_ws, dor_field))
            i += 1

        out_basin = [job for job in jobs]

    # Merge the temporary outputs
    print(f"Merging temporary outputs into output table {gpkg_full_path} ...")

    i = 0
    tbl = {}
    for bas in out_basin:
        i += 1

        with open(bas, 'rb') as fp:
            tbl[i] = cPickle.load(fp)

    gdf:gpd.GeoDataFrame = pd.concat(tbl.values(), axis = 1)
    gdf.to_file(gpkg_full_path, driver="GPKG", layer = "DOR")

    # Update automatically
    if update_mode.lower() == "YES":
        print(f"Updating dor values in database {streams_fc.head(1)}")

        gdf = tool.copy_between(
            to_join_fc=streams_fc,
            to_join_field="GOID",
            IntoJoinField=dor_field,
            FromJoinFC=gdf,
            FromJoinField="GOID",
            FromValueField=dor_field,
            over_mode=True,
            over_value=0
        )
        gdf.to_file(gpkg_full_path, driver="GPKG", layer = "DOR_COPIED")

    tool.delete_path(scratch_ws)

    return gdf[dor_field]


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
    scratch_gpkg = set_environment(scratchws, basin, stamp)

    streams = tool.update_stream_routing_index(streams)
    dams = tool.update_dam_routing_index(dams, streams)

    print (f"Calculating DOR for basin {basin}")

    dams, streams = indices.dor.calculate_dor(dams, streams, dor_field)

    final_table = export(streams, basin, scratch_gpkg)

    # Return only reference (path) to table
    return final_table


def get_unique(dam_table:gpd.GeoDataFrame, inc_field)->gpd.GeoDataFrame:
    """
    Calculates a list of unique river basins that need to be processed based
    on the barriers to be considered.

    :param dam_table: numpy array with dams to process
    :param inc_field: field to determine dams to include
    :return: List of river basins
    """
    
    in_basins:gpd.GeoDataFrame = dam_table[dam_table[inc_field] == 1]
    in_basins = in_basins[fd.BAS_ID].unique()


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


def load_dams(dam_table:gpd.GeoDataFrame, inc_field):
    """
    This function loads from the database

    :param dam_table:
    :param inc_field:
    :param use_dam_level_df:
    :return: numpy array with dams
    """
    flds = [fd.BAS_ID, fd.GOID, fd.STOR_MCM, fd.INC, inc_field]

    tool.check_fields(dam_table, flds + [fd.INC])

    dam_table[flds].fillna(0)
    dam_table = dam_table[(dam_table[inc_field] > 0) & (dam_table[fd.INC] > 0)]
    return dam_table


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

    stream_table[flds].fillna(0)
    stream_table = tool.add_fields(stream_table, [str(dor_field), 'f4'])
    
    return stream_table


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


def create_gpkg_workspace(gpkg_folder, gpkg_name):
    """
    Creates a path and a geodatabase with timestamp

    :param stamp:
    :param gpkg_folder:
    :param gpkg_name:
    :return:
    """
    raise Exception("ERRRROOOOOOR")

    if not os.path.exists(gpkg_folder):
        os.makedirs(gpkg_folder)

    gpkg_file_name = gpkg_name + ".gpkg"
    gpkg_full_path = gpkg_folder + "\\" + gpkg_file_name

    arcpy.CreateFilegpkg_management(gpkg_folder, gpkg_file_name)

    return gpkg_full_path, gpkg_file_name
