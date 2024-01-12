"""
This module provides functions to calculate the free-flowing river status.

This includes calculating the
"""

import numpy as np
import pandas as pd

from config import config as conf

fd = conf.var


def calculate_sta(stream_array:pd.DataFrame, stream_alt:pd.DataFrame, ff_field, ffr_stat1_field, ffr_stat2_field,
                  ffr_dis_id_field):
    """
    Calculate the free-flowing status (STA)

    :param stream_array:
    :param stream_alt:
    :param ff_field: field name of field indicating CSI above/below threshold
    :param ffr_stat1_field: field name for FFR status (two categories)
    :param ffr_stat2_field: field name for FFR status (three categories)
    :param ffr_dis_id_field: field name for river stretch.
    :return:
    """
    status_dict = {}

    # creating a DataFrame using the alternative (filtered) stream network
    df = stream_alt.copy()

    fun = {fd.LENGTH_KM: np.sum, fd.BB_LEN_KM: 'first'}
    df2:pd.DataFrame = df.groupby([fd.BB_ID, ffr_dis_id_field, ff_field],
                     as_index=False).agg(fun)

    # TODO: If study area cuts part of river network, the status attribute
    #  will not be correct because the some rivers are cut, and even if all
    #  reaches are FF, will not reach 100 % compared to BB_LEN_KM,
    #  which relates to entire river. Solution: recalculate the BB_LEN_KM
    #  attribute here

    # Calculating percent free-flowing of each stretch using the alternative stream array
    df2.loc[:, "PCT_FF"] = (df2[fd.LENGTH_KM] / df2[fd.BB_LEN_KM]) * 100

    print("Creating status dictionaries")
    dis_id = df2[ffr_dis_id_field].tolist()
    pct_ff = df2["PCT_FF"].tolist()
    status = df2[ff_field].tolist()

    for x, r in enumerate(dis_id):
        dis_id_val = dis_id[x]
        pct_ff_val = pct_ff[x]
        status_val = status[x]

        status_dict[dis_id_val] = return_sta(
            status_val=status_val,
            pct_ff_val=pct_ff_val
        )

    print("Updating status in original stream_array")
    stream_array = calc_status_values(stream_array, status_dict, ffr_dis_id_field, ffr_stat1_field, ffr_stat2_field)

    return stream_array


def calc_status_values(stream_array:pd.DataFrame, status_dict:dict, ffr_dis_id_field, ffr_stat1_field, ffr_stat2_field):
    """
    Assigns Free-flowing river status (STA) to the river reach.
    There are two types of STA values:

    a) CSI_FF1:

    1 = Free-flowing river; or
    3 = Not a free-flowing river

    b) CSI_FF2:

    1 = Free-flowing river;
    2 = River stretch with "Good Connectivity Status; or
    3 = Not a free-flowing river

    :param stream_array: original stream array to be updated
    :param status_dict: dictionary of FFR status values from alternative stream network
        to be transferred to original stream network
    :param ffr_dis_id_field: river stretch identifier
    :param ffr_stat1_field: field name of FFR status (two categories)
    :param ffr_stat2_field: field name of FFR status (three categories)
    :return:
    """

    for index, stream in stream_array.iterrows():
        dis_id = stream[ffr_dis_id_field]
        status = status_dict.get(dis_id, 0)

        if status == 3:
            stream_array.at[index, ffr_stat1_field] = 3
            stream_array.at[index, ffr_stat2_field] = 3
        elif status == 2:
            stream_array.at[index, ffr_stat1_field] = 3
            stream_array.at[index, ffr_stat2_field] = 2
        elif status == 1:
            stream_array.at[index, ffr_stat1_field] = 1
            stream_array.at[index, ffr_stat2_field] = 1
        else:
            # Should not occur
            print("{} with invalid STA value".format(str(stream[fd.GOID])))

            stream_array.at[index, ffr_stat1_field] = 0
            stream_array.at[index, ffr_stat2_field] = 0

    return stream_array


def return_sta(status_val, pct_ff_val): 
    """
    Calculates the STA value

    :param status_val: CSI status value (0 or 1 depending on threshold)
    :param pct_ff_val: percentage length of river that is free-flowing
    :return: STA value (1, 2, or 3)
    """

    # Making sure percentage cannot be None
    if np.isnan(pct_ff_val):
        pct_ff_val = 0

    if status_val == 1.0:
        if pct_ff_val >= 99.999:
            return 1
        elif pct_ff_val < 99.999:
            return 2
    elif status_val == 0:
        return 3


def dissolve_rivers(stream_array_temp:pd.DataFrame, ff_fields, dis_id_field):
    """
    This function is dissolving the results feature class and return
    aggregated results for each backbone river.

    :param stream_array_temp:  A Feature Class created by joining the CSI results table
            to a simple stream FC as preparation for dissolving
    :param ff_fields:  This field determines if a reach is below (0) or above (1)
            the csi threshold
    :param dis_id_field: Returns dissolved and aggregated FC with statistics to feed
            into filtering (next step)
    :return:
    """

    stream_array_temp[dis_id_field] = 0

    diss_field1 = str(fd.BB_ID)
    diss_field2 = str(ff_fields)

    diss1 = stream_array_temp[diss_field1].tolist()
    diss2 = stream_array_temp[diss_field2].tolist()

    dissid = stream_array_temp[dis_id_field].tolist()

    upsids = stream_array_temp["NUOID"].tolist()

    nodes = []
    diss_id_reach = 0

    # We need a set to check which ids are already in use. IDs should be unique
    diss_id_set = set([])

    sink_streams = stream_array_temp[stream_array_temp["NDOID"] == 0]
    sink_streams = sink_streams["NOID"].tolist()

    x = 0
    for sink in sink_streams:
        x += 0
        """ Process each sink stream in a loop """
        # Make sure the new id has not been used yet
        while True:
            new_id = diss_id_reach + 1
            if new_id not in diss_id_set:
                diss_id_set.add(new_id)
                break

        nodes.append(sink)
        dissid[sink - 1] = new_id
        diss_id_reach = new_id

        while True:
            new_nodes = []
            if len(nodes) == 0:
                break
            for node in nodes:
                if node != -1 and node != '':

                    # update segment
                    diss_value1 = diss1[node - 1]
                    diss_value2 = diss2[node - 1]

                    diss_id = dissid[node - 1]

                    ups = upsids[node - 1].split("_")

                    if len(ups) >= 1:
                        for up in ups:
                            if up != '':
                                new_nodes.append(int(up))
                                # Convert to int and adjust for index start at
                                # 0
                                up = int(up) - 1
                                if (diss1[up] == diss_value1) & (
                                        diss2[up] == diss_value2):
                                    dissid[up] = diss_id
                                else:
                                    while True:
                                        new_id = diss_id_reach + 1
                                        if new_id not in diss_id_set:
                                            diss_id_set.add(new_id)
                                            break
                                    dissid[up] = new_id
                                    diss_id_reach = new_id
            nodes = new_nodes

    # Assign list to numpy array field
    stream_array_temp[dis_id_field] = dissid

    return stream_array_temp


def apply_volume_filter(csi_fc:pd.DataFrame, ff_field, dis_id_field, pct_aff_thres):

    """
    This filter averages out some small inconsistencies, that could affect big rivers

    :param csi_fc: Path to feature class to filter
    :param ff_field: This field determines if a reach is below (0) or above (1) the csi threshold
    :param dis_id_field: Filtering on (1) or off (0)
    :param pct_aff_thres: volume_mcm threshold
    :return:
    """

    df = csi_fc.copy()
    df = df[df[ff_field] == 0]

    fun = {fd.LENGTH_KM: np.sum, fd.VOLUME_TCM: np.sum, fd.BB_VOL_TCM: 'first'}
    df1 = df.groupby([fd.BB_ID, dis_id_field], as_index=False).agg(fun)

    # Calculating a "Length Category"; grouping the backbone rivers into three
    # categories
    df1.loc[:, "PCT_AFF"] = (df1[fd.VOLUME_TCM] / df1[fd.BB_VOL_TCM]) * 100
    df1.loc[:, "FILTER"] = 0

    df1 = df1[df1["PCT_AFF"] < pct_aff_thres]
    df1.loc[:, "FILTER"] = 1

    return df1


def update_csi(streams_diss:pd.DataFrame, bb_id_to_filter:pd.DataFrame, dis_id_field, csi_field, ff_field):
    """
    This function alters the reach level results according to the spatial
    filtering after dissolving that occurred before.

    :param streams_diss: The river reach array to be modified
    :param disk_csi_layer: The river reach fc used for to select from
    during spatial selection
    :param diss_fc: the dissolved backbone layer used for spatial selection
    :param csi_field: field to be altered
    :param ff_field: field to be altered
    :return: modifies "df_mod"
    """

    # The CSI results will be temporarily overwritten, so that the
    # ffr status can be calculated using the previously applied filtering
    # techniques. Original CSI output is not affected
    print("Temporary overwriting CSI results")
    cnt = 0

    bbid_list = []
    for _, stretch in bb_id_to_filter.iterrows():
        bbid = int(stretch[fd.BB_ID])
        dis_id = int(stretch[dis_id_field])
        bbid_list.append(str(bbid) + str(dis_id))

    for index, stream in streams_diss.iterrows():
        bb_id = int(stream[fd.BB_ID])
        dis_id = int(stream[dis_id_field])

        combo = str(bb_id) + str(dis_id)

        if combo in bbid_list:
            streams_diss.at[index, csi_field] = 100
            streams_diss.at[index, ff_field] = 1
            cnt += 1

    # Number of river reaches temporarliy altered for this procedur
    print("Number of river reaches altered: " + str(cnt))

    return streams_diss


def update_streams_with_diss_id(stream_array:pd.DataFrame, streams_diss2:pd.DataFrame, ff_dis_id_field):
    """
    Function to update original stream ID

    :param stream_array:
    :param streams_diss2:
    :param ff_dis_id_field:
    :return:
    """
    dis_ids = streams_diss2[ff_dis_id_field].tolist()

    indx = 0
    for index, stream in stream_array.iterrows():
        disid = dis_ids[indx]
        stream_array.at[index, ff_dis_id_field] = disid
        indx += 1

    return stream_array
