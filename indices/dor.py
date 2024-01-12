from config import config
import geopandas as gpd

fd = config.var


def calculate_dor(dams:gpd.GeoDataFrame, streams:gpd.GeoDataFrame, dor_field):  # List of OIDs
    """
    Updates the degree of regulation (DOR) index, given a
    set of dams.

    :param dams: numpy array of barriers and their attributes
    :param streams: numpy array of streams and their attributes
    :param dor_field: field to store the DOR values
    :return: stream array with updated DOR values
    """

    length = streams.shape[0]
    ndoid = streams["NDOID"].tolist()
    disch = streams["DIS_AV_CMS"].tolist()
    svol = [0] * length

    for index, dam in dams.iterrows():

        routing_list = set([])
        dam_oid = dam[fd.GOID]

        while 1:
            new_node = -1
            if dam_oid == -1:
                break

            if dam_oid not in routing_list:
                routing_list.add(dam_oid)

                # Calculate DOR and update array
                svol[dam_oid - 1] += dam[fd.STOR_MCM]

                new_dor = get_dor(disch[dam_oid - 1], svol[dam_oid - 1])
                streams.at[dam_oid - 1, dor_field] = new_dor

                dw = ndoid[dam_oid - 1]
                if dw != 0:
                    new_node = dw

            dam_oid = new_node

    return dams, streams


def get_dor(discharge, storage):
    """
    Get the DOR value given the discharge and storage values

    :param discharge: value provided in cubic meters per second
    :param storage: value provided in million cubic meters
    :return: degree of regulation (in percent)
    """
    if discharge == 0:
        return 0

    storage = storage * 1000000  # convert to cubic meters
    # convert cms to annual discharge in cubic meters
    annual_disch_reach = discharge * 60 * 60 * 24 * 365
    temp = storage / annual_disch_reach
    _dor = (100 * temp)

    # Limit DOR to 100. The DOR can technically be higher, however we consider
    # a DOR of 100 % as severely impacting the river and assume this value as
    # the ceiling of the DOR value.
    if _dor > 100:
        return 100

    # A DOR smaller than 0.1 is negligible and should be considered as zero.
    if _dor < 0.1:
        return 0

    return _dor
