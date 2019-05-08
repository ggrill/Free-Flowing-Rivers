import logging
import sys
from collections import defaultdict

import arcpy

import indices.sed
from config import config
from tools import helper

fd = config.var


def run_sed(para, paths):
    """
    Script to calculate the Sediment Trapping Index (SED)

    :param para: input parameters and path names for executing the script
    :param paths: output pathnames
    :return:
    """
    streams_fc = para["streams_fc"]
    dams_fc = para["dams_fc"]
    svol_field = para["svol_field"]
    barrier_inc_field = para["barrier_inc_field"]
    lakes_fc = para["lakes_fc"]
    sed_field = para["sed_field"]
    out_gdb = paths["gdb_full_path"]

    streams = load_streams(streams_fc)
    streams, convert_dict = update_stream_routing_index(streams)

    barriers = load_barriers(dams_fc, convert_dict,
                             svol_field, barrier_inc_field)
    dam_volu_dict = barriers_calculate(barriers, svol_field)

    lakes = load_lakes(lakes_fc, convert_dict)

    small_lake_loss_dict, lake_volu_dict = indices.sed.lakes_calculate(lakes)

    streams = indices.sed.calculate_sed(streams, dam_volu_dict, lake_volu_dict,
                                        small_lake_loss_dict)

    prt("Exporting results sediment table")

    outtbl = export_results_table(streams, out_gdb)

    # Adding indices helps with joining tables to geometry
    arcpy.AddIndex_management(outtbl, fd.GOID, fd.GOID, "UNIQUE", "ASCENDING")

    # Update original database
    if para["update_mode"] == "YES":
        print("Updating SED values in database {} ".format(streams_fc))
        try:
            helper.copy_between(streams_fc, fd.GOID,
                                sed_field, outtbl,
                                fd.GOID, fd.SED,
                                "overwrite", 0)
        except Exception as e:
            print (str(e))
            sys.exit(0)


def load_streams(stream_table):
    """
    Loading stream network and adding fields

    :param stream_table:
    :return: stream array with necessary fields
    """

    # Existing fields to load
    flds = [fd.GOID, fd.NOID, fd.NDOID, fd.NUOID, fd.INC,
            fd.DIS_AV_CMS, fd.BAS_ID, fd.UPLAND_SKM,
            fd.ERO_YLD_TON]

    arr = arcpy.da.TableToNumPyArray(stream_table, flds)

    arr = helper.add_fields(arr, [(fd.SED_LSS_LKS_OT_NAT, 'f8')])
    arr = helper.add_fields(arr, [(fd.SED_LSS_LKS_IN_NAT, 'f8')])
    arr = helper.add_fields(arr, [(fd.SED_NAT_UP, 'f8')])
    arr = helper.add_fields(arr, [(fd.SED_NAT, 'f8')])

    arr = helper.add_fields(arr, [(fd.SED_LSS_LKS_OT_ANT, 'f8')])
    arr = helper.add_fields(arr, [(fd.SED_LSS_LKS_IN_ANT, 'f8')])
    arr = helper.add_fields(arr, [(fd.SED_LSS_DMS_ANT, 'f8')])
    arr = helper.add_fields(arr, [(fd.SED_ANT_UP, 'f8')])
    arr = helper.add_fields(arr, [(fd.SED_ANT, 'f8')])

    arr = helper.add_fields(arr, [(fd.SED_LSS_TOT, 'f8')])

    arr = helper.add_fields(arr, [(fd.SED, 'f8')])

    arr[fd.SED_NAT] = 0
    arr[fd.SED_NAT_UP] = 0
    arr[fd.SED_LSS_LKS_OT_NAT] = 0
    arr[fd.SED_LSS_LKS_IN_NAT] = 0

    arr[fd.SED_ANT] = 0
    arr[fd.SED_ANT_UP] = 0
    arr[fd.SED_LSS_LKS_OT_ANT] = 0
    arr[fd.SED_LSS_LKS_IN_ANT] = 0
    arr[fd.SED_LSS_DMS_ANT] = 0

    arr[fd.SED_LSS_TOT] = 0

    arr[fd.SED] = 0

    return arr


def update_stream_routing_index(streams):
    """
    Function to sort the stream network using the upstream area. This allows
    the network to be processed in order from headwaters to the ocean. Afterwards
    the Network IDS are recalculated

    :param streams: numpy array of stream network
    :return:
    """

    print("Updating stream index")

    # Maintain the old GOID values in a new field
    streams = helper.add_fields(streams, [("OGOID", 'i4')])
    streams["OGOID"] = streams["GOID"]

    # Sort the array by upland area and basin
    # This is key to being able to process river network from top to bottom
    streams.sort(order=['BAS_ID', 'UPLAND_SKM'])

    # Create Routing Dictionaries and fill
    oid_dict = {}
    convert_dict = {}
    ups_dict = defaultdict(str)
    down_dict = defaultdict(long)

    i = 1
    for myrow in streams:
        oid_dict[int(myrow["GOID"])] = i
        i += 1

    i = 1
    for myrow in streams:
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
    for myrow in streams:
        myrow["NOID"] = i
        myrow["NDOID"] = down_dict[i]
        myrow["NUOID"] = ups_dict[i]

        i = i + 1

    # Create Dictionary to convert old (KEY) to new (VALUE)
    i = 1
    for myrow in streams:
        old = myrow["OGOID"]
        new = myrow["NOID"]
        convert_dict[int(old)] = new
        i += 1

    return streams, convert_dict


def load_lakes(lakes_table, convert_dict):
    """
    Load lakes and add field for sediment trapping calculations

    :param lakes_table:
    :param convert_dict:
    :return: numpy array of lakes
    """

    print("Loading lakes")

    flds = ["GOID", "GOOD", "Lake_type", "SED_ACC", "IN_STREAM", "IN_CATCH",
            "Vol_total", "Dis_avg", "Res_time"]

    arr = arcpy.da.TableToNumPyArray(lakes_table, flds, null_value=0)

    arr = helper.add_fields(arr, [("TE_brune", 'f8')])
    arr = helper.add_fields(arr, [("LOSS_LKES_OUT_NET", 'f8')])

    arr["TE_brune"] = 0
    arr["LOSS_LKES_OUT_NET"] = 0

    for a in arr:
        a["GOID"] = convert_dict.get(a["GOID"], 0)

    return arr


def load_barriers(barriers_table, convert_dict, svol_field, barrier_inc_field):
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

    # Load the array from file
    arr = arcpy.da.TableToNumPyArray(barriers_table, flds, null_value=0)

    for a in arr:
        a[fd.GOID] = convert_dict.get(a[fd.GOID], 0)

    # Select only dams that are included in analysis through field INC or INC1
    arr2 = arr[(arr[barrier_inc_field] == 1) & (arr[fd.INC] == 1)]

    return arr2


def barriers_calculate(barriers, svol_field):
    """
    In some cases, there are multiple reservoirs located on a river reach.
    This function calculates the sum of reservoir volume for each river reach.

    :param barriers:
    :param svol_field:
    :return:
    """
    dam_volu_dict = {}

    for b in barriers:
        goid = b[fd.GOID]
        dam_volu_dict[goid] = dam_volu_dict.get(goid, 0) + b[svol_field]

    return dam_volu_dict


def export_results_table(streams, out_gdb):
    out_tbl = out_gdb + "\\sed"
    arcpy.da.NumPyArrayToTable(streams[[fd.GOID, fd.SED]], out_tbl)
    return out_tbl


def prt(txt):
    logging.info(txt)
    print(txt)
