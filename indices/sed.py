import math
import logging
import config.config
import geopandas as gpd
fd = config.config.var


def lakes_calculate(lakes:gpd.GeoDataFrame):
    """
    Calculate the trapping of sediments in natural lakes

    :param lakes: numpy array with properties of lakes
    :return: dictionaries of small and large lakes and their trapping in tons per year
    """
    prt("")
    prt("***************************************************")
    prt("PART 1: Calculating sediment trapping in lakes")
    prt("***************************************************")
    prt("")

    # Select natural lake, exclude Grand dam, or regulated lake
    arr1 = lakes[lakes["Lake_type"] == 1]
    # Exclude KCL (GOOD2) dam used in the FFR analysis
    arr2 = arr1[arr1["GOOD"] == 0]
    # Select only lakes that are within stream catchment
    # (some lakes are outside , coastal etc.)
    lakes = arr2[arr2["IN_CATCH"] == 1]

    # Dictionaries will hold values
    small_lake_loss_dict = {}
    large_lake_volu_dict = {}

    # Iterate through the lakes and extract values, fill dictionaries etc.
    for _, lake in lakes.iterrows():
        goid = lake["GOID"]

        if lake["IN_STREAM"] == 0:
            # not in stream network, calculate TE_brune and loss
            lake_te = TE(lake["Vol_total"], lake["Dis_avg"])
            lake["TE_brune"] = lake_te

            lake["LOSS_LKES_OUT_NET"] = lake["TE_brune"] * lake["SED_ACC"]

            curr_loss = small_lake_loss_dict.get(goid, 0)
            small_lake_loss_dict[goid] = curr_loss + lake["LOSS_LKES_OUT_NET"]

        else:
            # sum volume_mcm for later processing during routing
            current_lake = large_lake_volu_dict.get(goid, 0)
            large_lake_volu_dict[goid] = current_lake + lake["Vol_total"]

    return small_lake_loss_dict, large_lake_volu_dict


def calculate_sed(streams:gpd.GeoDataFrame, dam_volu_dict:dict, lake_volu_dict:dict, small_lake_loss_dict:dict):
    """
    Calculates the Sediment Trapping Index (SED)

    :param streams:
    :param dam_volu_dict:
    :param lake_volu_dict:
    :param small_lake_loss_dict:
    :return:
    """

    # 1) Accumulate sediments taking into account lakes inside and outside
    # the network
    #
    # Calculating potential sediment load

    prt("")
    prt("***************************************************")
    prt("PART 2: Calculating potential sediment load")
    prt("***************************************************")
    prt("")

    for _, stream in streams.iterrows():

        dis = stream[fd.DIS_AV_CMS]
        noid = stream[fd.NOID]

        vol_lakes_in_network = lake_volu_dict.get(noid, 0)

        loss_lakes_outside_net = small_lake_loss_dict.get(noid, 0)

        sed_sum_current = stream[fd.ERO_YLD_TON]
        sed_nat_ups = stream[fd.SED_NAT_UP]

        sed_nat = sed_sum_current + sed_nat_ups

        # 1) Account for losses from natural lakes outside network (Type 1)
        sed_nat = sed_nat - loss_lakes_outside_net

        # Write into table
        stream[fd.SED_LSS_LKS_OT_NAT] = loss_lakes_outside_net

        # 2) Account for losses from natural lakes inside network (Type 1)
        loss_lakes_in_nat = sed_nat - (sed_nat * TE(vol_lakes_in_network, dis))

        # Write into table
        stream[fd.SED_LSS_LKS_IN_NAT] = loss_lakes_in_nat

        sed_nat = sed_nat - loss_lakes_in_nat

        # 3) Write to table
        stream[fd.SED_NAT] = sed_nat

        # Add the results to next downstream reach
        if (stream[fd.NOID]-1 in streams.index) and (stream[fd.NDOID]-1 in streams.index) and (streams.loc[stream[fd.NOID]-1][fd.NDOID] != 0):
            streams.at[stream[fd.NDOID]-1, fd.SED_NAT_UP] = streams.loc[stream[fd.NDOID]-1][fd.SED_NAT_UP] + sed_nat

    # 2) Accumulate sediments taking into account lakes inside and
    # outside the network and dams

    # Calculating anthropogenic sediment load

    prt("")
    prt("***************************************************")
    prt("PART 3: Calculating anthropogenic sediment load    ")
    prt("***************************************************")
    prt("")

    for index, stream in streams.iterrows():

        dis = stream[fd.DIS_AV_CMS]
        noid = stream[fd.NOID]

        vol_lakes_in_network = lake_volu_dict.get(noid, 0)
        vol_dams = dam_volu_dict.get(noid, 0)

        loss_lakes_outside_net = small_lake_loss_dict.get(noid, 0)

        sed_sum_current = stream[fd.ERO_YLD_TON]
        sed_ant_ups = stream[fd.SED_ANT_UP]

        sed_ant = sed_sum_current + sed_ant_ups

        # 1) Account for losses from natural lakes outside network (Type 1)
        sed_ant = sed_ant - loss_lakes_outside_net

        # Write into table
        streams.at[index, fd.SED_LSS_LKS_OT_ANT] = loss_lakes_outside_net

        # 2) Account for losses from natural lakes inside network
        # (Type 1) and dams

        # First process the lakes, ....
        loss_lakes_in_ant = sed_ant - (sed_ant * TE(vol_lakes_in_network, dis))
        # ... and substract the losses from lakes, ....
        sed_ant = sed_ant - loss_lakes_in_ant
        # then process the dams.....
        loss_dams_in_ant = sed_ant - (sed_ant * TE(vol_dams, dis))
        # ... and substract the losses from dams
        sed_ant = sed_ant - loss_dams_in_ant

        # Write into table
        streams.at[index, fd.SED_LSS_LKS_IN_ANT] = loss_lakes_in_ant

        # Write into table
        streams.at[index, fd.SED_LSS_DMS_ANT] = loss_dams_in_ant

        # 3) Write to table
        streams.at[index, fd.SED_ANT] = sed_ant

        # Add the results to next downstream reach
        if (stream[fd.NOID] - 1 in streams.index) and (stream[fd.NDOID] - 1 in streams.index) and (streams.loc[stream[fd.NOID] - 1][fd.NDOID] != 0):
            streams.at[stream[fd.NDOID] - 1, fd.SED_ANT_UP] = streams.loc[stream[fd.NDOID] - 1][fd.SED_ANT_UP] + sed_ant

    # 3) Calculate the difference between 3 and 2, which is the
    # losses due to dams
    #
    # Calculate index (difference)

    prt("")
    prt("***************************************************")
    prt("PART 4: Calculating SED index")
    prt("***************************************************")
    prt("")

    for index, stream in streams.iterrows():
        sed_nat = stream[fd.SED_NAT]
        sed_ant = stream[fd.SED_ANT]

        sediment_loss = sed_nat - sed_ant

        streams.at[index, fd.SED_LSS_TOT] = sediment_loss

        # Make sure there is no division by zero error
        if sed_nat > 0.000000001:

            sti = 100 * (sediment_loss / sed_nat)

            # Clip values smaller than 0.1 just like for DOR
            if sti >= 0.1:
                streams.at[index, fd.SED] = 100 * (sediment_loss / sed_nat)
            else:
                streams.at[index, fd.SED] = 0
        else:
            streams.at[index, fd.SED] = 0

    return streams


def TE(volume, discharge):
    """
    Calculates the trapping efficiency of reservoir or lake according to the Brune equation

    :param volume: volume of reservoir or lake in million cubic meters (MCM)
    :param discharge: discharge at reservoir or lake location (outflow) in cubic meters per
    second (CMS)

    :return: trapping efficiency in percent
    """

    if discharge < 0.00000001:
        TE_RATIO = 1.0
    else:
        vol_dis = (volume * 1000000.0) / (
                discharge * 60.0 * 60.0 * 24.0 * 365.0)
        div = math.sqrt(vol_dis)

        if div < 0.00000001:
            TEF = 0
            TE_RATIO = 1.0 - TEF
        else:
            TEF = 1.0 - ((0.05) / float(div))

            if TEF < 0:
                TEF = 0

            TE_RATIO = 1.0 - TEF

    return TE_RATIO


def prt(txt):
    logging.info(txt)
    print(txt)
