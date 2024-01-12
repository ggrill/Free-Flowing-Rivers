import math
import sys
import geopandas as gpd

from config import config

fd = config.var


def calculate_DOF(dams:gpd.GeoDataFrame, streams:gpd.GeoDataFrame, mode:int, dof_field:str, drf_upstream:int, drf_downstream:int,
                  use_dam_level_df:str):
    """
    Calculates the degree of fragmentation (DOF) given a set of dams

    :param dams: numpy array including one or several dams
    :param streams: numpy array with a stream network
    :param mode: numeric value defining the selection of decay function
    :param dof_field: field to fill with DOF value
    :param drf_upstream: multiplier to indicate deviation from discharge
        occurring at barrier, usually 5(=0.5 orders of magnitude). Can be set
        individually for each dam, and for each direction
    :param drf_downstream: multiplier to indicate deviation from discharge
        occurring at barrier, usually 5(=0.5 orders of magnitude). Can be set
        individually for each dam, and for each direction
    :param use_dam_level_df: If true, looks for DFU and DFD fields,
        which hold discharge range factor values for upstream and
        downstream direction
    :return: stream array with updates DOF values
    """

    ndoid = streams["NDOID"].tolist()
    nuoid = streams["NUOID"].tolist()
    wfall = streams["HYFALL"].tolist()
    disch = streams["DIS_AV_CMS"].tolist()

    upstream_mode = mode
    downstream_mode = mode

    nodes = []
    downstream_reaches = set([])
    stop = set([])

    # First process everything upstream of dam
    for index, dam in dams.iterrows():

        dam_goid = dam[fd.GOID]

        if use_dam_level_df is True or (isinstance(use_dam_level_df, str) and use_dam_level_df.upper() == "YES"):
            drf_upstream = dam[fd.DFU]
            drf_downstream = dam[fd.DFD]
        else:
            drf_upstream = drf_upstream
            drf_downstream = drf_downstream

        if drf_upstream <= 1:
            # To prevent log10(1) = 0
            drf_upstream = 1.000000000000001
            if drf_upstream < 1:
                print ("discharge range factor (upstream) can not be "
                       "lower than 1. Setting drf_upstream to 1")
        if drf_downstream <= 1:
            # To prevent log10(1) = 0
            drf_downstream = 1.000000000000001
            if drf_downstream < 1:
                print ("discharge range factor (downstream) can not be "
                       "lower than 1. Setting drf_downstream to 1")

        nodes.append(dam_goid)  # Add the dam to the nodes list to process
        stop.clear()
        stop.add(dam_goid)
        downstream_reaches.clear()

        discharge_barrier_location = disch[dam_goid - 1]

        if discharge_barrier_location == 0:
            streams.at[dam_goid - 1, dof_field] = 100
            continue

        # discharge range factor, usually 10 (= one order of magnitude)
        dis_low = discharge_barrier_location / drf_upstream
        dis_hgh = discharge_barrier_location * drf_downstream

        # 1) First process everything upstream of dam
        while 1:
            new_nodes = []
            if len(nodes) == 0:
                break
            for n in nodes:

                # Add one or several constraints on how to rout
                if n != -1 and n != '':

                    # Check if node has waterfall. If the waterfall is on the
                    # reach where the dam is, stop routing upstream
                    if wfall[n - 1] == 0:
                        # Check for conditions of discharge
                        if dis_hgh >= disch[n - 1] >= dis_low:
                            # declare segment as fragmented
                            discharge_local = disch[n - 1]

                            local_impact_score = get_dof_up(
                                discharge_local=discharge_local,
                                discharge_barrier=discharge_barrier_location,
                                upstream_mode=upstream_mode,
                                dis_range_factor=drf_upstream)
                            
                            if streams.loc[n - 1][dof_field] <= local_impact_score or streams.loc[n - 1][dof_field] == 0:
                                streams.at[n - 1, dof_field] = local_impact_score

                            # if local_impact_score > 0:  # find upstream nodes
                            upstream_oid = nuoid[n - 1].split("_")
                            if len(upstream_oid) > 0:
                                for up in upstream_oid:
                                    if up != '' and int(up) > 0:
                                        new_nodes.append(int(up))

            nodes = new_nodes

        # 2) Then process downstream
        nodes.append(dam_goid)  # Add the dam to the nodes list to process

        while 1:
            new_nodes = []
            if len(nodes) == 0:
                break
            for n in nodes:
                if n != -1 and n != '':
                    # declare segment as fragmented if local discharge of
                    # reach is within upper and lower limits
                    discharge_local = disch[n - 1]
                    if dis_low <= discharge_local <= dis_hgh:
                        local_impact_score = get_dof_down(
                            discharge_local=discharge_local,
                            discharge_barrier=discharge_barrier_location,
                            downstream_mode=downstream_mode,
                            dis_range_factor=drf_downstream)
                        if streams.loc[n - 1][dof_field] <= local_impact_score or streams.loc[n - 1][dof_field] == 0:
                            streams.at[n - 1, dof_field] = local_impact_score

                        # if local_impact_score > 0:  # find downstream nodes
                        downstream_oid = ndoid[n - 1]
                        if downstream_oid > 0:
                            new_nodes.append(int(downstream_oid))
                            downstream_reaches.add(downstream_oid)

            nodes = new_nodes

    return dams, streams


def get_dof_down(discharge_local, discharge_barrier, downstream_mode,
                 dis_range_factor):
    """
    Calculates DOF for downstream reaches

    :param discharge_local: discharge (CMS) at current river reach to be processed
    :param discharge_barrier: discharge (CMS) at river reach of barrier location
    :param downstream_mode: defines the function of gradual decay of effect in the downstream
    direction
    :param dis_range_factor: defines the ratio between discharge_barrier and
    discharge_local below which DOF effects are considered
    :return: degree of fragmentation (DOF) in percent
    """
    if discharge_local < discharge_barrier:
        # prevents DOF to become larger than 100
        # in special cases when e.g, discharge decreases downstream
        # and becomes larger than local discharge
        discharge_local = discharge_barrier  # reset discharge

    if downstream_mode == 1:  # log two order
        a = abs(math.log10(discharge_local) - math.log10(discharge_barrier))
        b = a * (100 / math.log10(dis_range_factor))
        x = 100 - b
    else:
        print("discharge mode undefined")
        sys.exit()

    # I some cases x can get out of bounds
    if x < 0:
        x = 0
    if x > 100:
        x = 100

    return x


def get_dof_up(discharge_local, discharge_barrier, upstream_mode,
               dis_range_factor):
    """
    Calculates DOF for upstream reaches

    :param discharge_local: discharge (CMS) at current river reach to be processed
    :param discharge_barrier: discharge (CMS) at river reach of barrier location
    :param upstream_mode: defines the function of gradual decay of effect in the upstream direction
    :param dis_range_factor: defines the ratio between discharge_barrier and
    discharge_local below which DOF effects are considered
    :return: degree of fragmentation (DOF) in percent
    """

    if discharge_local > discharge_barrier:
        discharge_local = discharge_barrier
        # prevents DOF to become larger than 100
        # in special cases when e.g, discharge increases upstream,
        # and becomes smaller than local discharge

    if upstream_mode == 1:  # log one order
        a = abs(math.log10(discharge_local) - math.log10(discharge_barrier))
        b = a * (100 / math.log10(dis_range_factor))
        x = 100 - b
    else:
        print("discharge mode undefined")
        sys.exit()

    # I some cases x can get out of bounds
    if x < 0:
        x = 0
    if x > 100:
        x = 100

    return x
