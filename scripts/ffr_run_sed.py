import logging
import sys
import os
from collections import defaultdict

import geopandas as gpd

import indices.sed
from config import config
import tools.helper as tool

fd = config.var


def run_sed(para, paths):
    """
    Script to calculate the Sediment Trapping Index (SED)

    :param para: input parameters and path names for executing the script
    :param paths: output pathnames
    :return:
    """
    streams_fc:gpd.GeoDataFrame = para["streams_fc"]
    dams_fc:gpd.GeoDataFrame = para["dams_fc"]
    lakes_fc:gpd.GeoDataFrame = para["lakes_fc"]

    svol_field = para["svol_field"]
    barrier_inc_field = para["barrier_inc_field"]
    sed_field = para["sed_field"]
    out_gpkg = paths["gpkg_full_path"]

    streams = load_streams(streams_fc)
    streams, convert_dict = update_stream_routing_index(streams)

    barriers = load_barriers(dams_fc, convert_dict, svol_field, barrier_inc_field)

    dam_volu_dict = barriers_calculate(barriers, svol_field)

    lakes = load_lakes(lakes_fc, convert_dict)

    small_lake_loss_dict, lake_volu_dict = indices.sed.lakes_calculate(lakes)

    streams = indices.sed.calculate_sed(streams, dam_volu_dict, lake_volu_dict, small_lake_loss_dict)

    prt("Exporting results sediment table")

    # Adding indices helps with joining tables to geometry
    streams.sort_values(by=[fd.GOID, fd.GOID], inplace=True, ascending=True)
    streams.to_file(out_gpkg, driver="GPKG", layer = "SED")

    # Update original database
    if para["update_mode"] == "YES":
        print(f"Updating SED values in database {streams_fc.head(1)}")
        try:
            streams = tool.copy_between(
                streams_fc,
                fd.GOID,
                sed_field,
                streams,
                fd.GOID,
                fd.SED,
                "overwrite",
                0,
            )
            streams.to_file(out_gpkg, driver="GPKG", layer = "SED_COPIED")
        
        except Exception as e:
            print (str(e))
            sys.exit(0)
    
    return streams[sed_field]


def load_streams(stream_table:gpd.GeoDataFrame):
    """
    Loading stream network and adding fields

    :param stream_table:
    :return: stream array with necessary fields
    """

    # Existing fields to load
    flds = [fd.GOID, fd.NOID, fd.NDOID, fd.NUOID, fd.INC,
            fd.DIS_AV_CMS, fd.BAS_ID, fd.UPLAND_SKM,
            fd.ERO_YLD_TON]

    tool.check_fields(stream_table, flds)
    stream_table[flds].fillna(0)

    stream_table = tool.add_fields(stream_table, [
        'f8',
        fd.SED_LSS_LKS_OT_NAT,
        fd.SED_LSS_LKS_IN_NAT,
        fd.SED_NAT_UP,
        fd.SED_NAT,
        fd.SED_LSS_LKS_OT_ANT,
        fd.SED_LSS_LKS_IN_ANT,
        fd.SED_LSS_DMS_ANT,
        fd.SED_ANT_UP,
        fd.SED_ANT,
        fd.SED_LSS_TOT,
        fd.SED,
        fd.SED_NAT,
    ])

    return stream_table


def update_stream_routing_index(streams:gpd.GeoDataFrame):
    """
    Function to sort the stream network using the upstream area. This allows
    the network to be processed in order from headwaters to the ocean. Afterwards
    the Network IDS are recalculated

    :param streams: numpy array of stream network
    :return:
    """

    print("Updating stream index")

    # Maintain the old GOID values in a new field
    streams:gpd.GeoDataFrame = tool.add_fields(streams, ["OGOID", 'i4'])
    streams["OGOID"] = streams["GOID"]

    # Sort the array by upland area and basin
    # This is key to being able to process river network from top to bottom
    streams.sort_values(by=['BAS_ID', 'UPLAND_SKM'], inplace=True)

    # Create Routing Dictionaries and fill
    oid_dict = {}

    i = 1
    for index, myrow in streams.iterrows():
        oid_dict[myrow['GOID']] = i
        i += 1

    convert_dict = {}
    ups_dict = defaultdict(str)
    down_dict = defaultdict(float)

    i = 1
    for index, myrow in streams.iterrows():
        dn_old_oid = myrow["NDOID"]
        new_oid = oid_dict.get(int(dn_old_oid), -1)

        if new_oid != -1:
            # Write OID of next downstream reach
            down_dict[i] = new_oid
            # Write OID of next upstream reach
            exi_value = ups_dict.get(int(new_oid), -99)

            if exi_value == -99:
                ups_dict[int(new_oid)] = i
            else:
                new_value = str(exi_value) + '_' + str(i)
                ups_dict[int(new_oid)] = new_value

        i += 1

    # Writing index values back to numpy
    i = 1
    for index, myrow in streams.iterrows():
        streams.at[index, "NOID"] = i
        streams.at[index, "NDOID"] = down_dict[i]
        streams.at[index, "NUOID"] = ups_dict[i]

        i = i + 1

    # Create Dictionary to convert old (KEY) to new (VALUE)
    i = 1
    for _, myrow in streams.iterrows():
        old = myrow["OGOID"]
        new = myrow["NOID"]
        convert_dict[int(old)] = new
        i += 1

    return streams, convert_dict


def load_lakes(lakes_table:gpd.GeoDataFrame, convert_dict:dict)->gpd.GeoDataFrame:
    """
    Load lakes and add field for sediment trapping calculations

    :param lakes_table:
    :param convert_dict:
    :return: numpy array of lakes
    """

    print("Loading lakes")

    flds = ["GOID", "GOOD", "Lake_type", "SED_ACC", "IN_STREAM", "IN_CATCH",
            "Vol_total", "Dis_avg", "Res_time"]

    lakes_table[flds].fillna(0)

    lakes_table = tool.add_fields(lakes_table, ["TE_brune", "LOSS_LKES_OUT_NET"])
    lakes_table["TE_brune"] = 0
    lakes_table["LOSS_LKES_OUT_NET"] = 0

    for index, a in lakes_table.iterrows():
        lakes_table.at[index, "GOID"] = convert_dict.get(a["GOID"], 0)

    return lakes_table


def load_barriers(barriers_table:gpd.GeoDataFrame, convert_dict:dict, svol_field, barrier_inc_field)->gpd.GeoDataFrame:
    """
    Loading dams and converting old to new

    :param barriers_table:
    :param convert_dict:
    :param svol_field:
    :param barrier_inc_field:
    :return:
    """
    print("Loading dams")

    # Existing fields to load
    flds = [fd.GOID, fd.NOID, svol_field, fd.INC, barrier_inc_field]

    barriers_table[flds].fillna(0)

    for index, a in barriers_table.iterrows():
        barriers_table.at[index, fd.GOID] = convert_dict.get(a[fd.GOID], 0)

    # Select only dams that are included in analysis through field INC or INC1
    barriers_table = barriers_table[(barriers_table[barrier_inc_field] == 1) & (barriers_table[fd.INC] == 1)]

    return barriers_table


def barriers_calculate(barriers:gpd.GeoDataFrame, svol_field):
    """
    In some cases, there are multiple reservoirs located on a river reach.
    This function calculates the sum of reservoir volume for each river reach.

    :param barriers:
    :param svol_field:
    :return:
    """
    dam_volu_dict = {}

    for _, b in barriers.iterrows():
        goid = b[fd.GOID]
        dam_volu_dict[goid] = dam_volu_dict.get(goid, 0) + b[svol_field]

    return dam_volu_dict


def prt(txt):
    logging.info(txt)
    print(txt)
