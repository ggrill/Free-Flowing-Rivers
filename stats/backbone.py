import numpy as np
import pandas as pd

import tools.helper as tools
from stats.benchmarking import fd


def backbone_stats(
        stream_array:pd.DataFrame,
        scenario_name,
        min_length,
        sta_pickle_folder,
        writer
    ):
    """
    Calculate backbone stats

    :param stream_array:
    :param scenario_name:
    :param min_length:
    :param sta_pickle_folder:
    :param writer:
    :return:
    """

    # River_stats_1(two status options)
    bb0 = backbone_stats_0(scenario_name, stream_array)

    # River_stats_2 (three status options)
    bb1 = backbone_stats_1(scenario_name, stream_array)

    # River_stats_good
    bb2 = backbone_stats_2(scenario_name, stream_array)

    # List_of_FFRs
    bb3 = backbone_stats_3(scenario_name, stream_array, min_length)

    # Saving raw strings to output folder for later testing.
    for i, o in enumerate([bb1, bb0, bb3, bb2]):
        # https://stackoverflow.com/a/522578/344647
        tools.save_as_cpickle(pickle_object=o,
                              folder=sta_pickle_folder,
                              name=scenario_name + "bb" + str(i),
                              file_extension=".bb")

    tools.export_excel(bb0, 'River_stats_1', writer)
    tools.export_excel(bb1, 'River_stats_2', writer)
    tools.export_excel(bb2, 'River_stats_good', writer, True)
    tools.export_excel(bb3, 'List_of_FFRs', writer)


def backbone_stats_0(sce_name, stream_array:pd.DataFrame):
    """
    Calculating backbone statistics for Table 1.
    Only two categories: free-flowing or not (no "good" status)

    :param sce_name:
    :param stream_array:
    :return:
    """

    # Converting to panda data frame
    df = stream_array.copy()
    df = df.loc[df["INC"] == 1]

    df.loc[:, "SCE"] = sce_name

    # Calculating a field for "Length Category" and thus grouping the
    # backbone rivers into five categories:
    #
    # very short 0-10 km (not analyzed)
    # short rivers: 10 - 100 km
    # medium rivers 100 - 500 km
    # long rivers 500 - 1000 km
    # very long rivers > 1000 km

    df.loc[:, sce_name + "_LCAT"] = df[fd.BB_LEN_KM].apply(get_length_cat)

    # Function to group by continent, river, length category,
    # and free-flowing status and calculate length (km), volume (million
    # cubic meters) and connectivity to ocean.
    # see Table 1

    fun1 = {'SCE': 'first',
            fd.LENGTH_KM: np.sum,
            fd.VOLUME_TCM: np.sum,
            fd.BB_OCEAN: 'max'}

    tbl_1_temp = df.groupby([fd.CON_ID,
                             fd.BB_ID,
                             sce_name + "_LCAT",
                             sce_name + "_FF1"],
                            as_index=False).agg(fun1)

    # The result is a list of rivers by continent. We're adding a "count"
    # field (NUM) so we can now count the number of rivers
    tbl_1_temp.loc[:, "NUM"] = 1

    # Then repeat the same function as above

    fun2 = {'SCE': 'first',
            fd.LENGTH_KM: np.sum,
            fd.VOLUME_TCM: np.sum,
            fd.BB_OCEAN: np.sum,
            "NUM": np.sum}

    tbl_1 = tbl_1_temp.groupby([fd.CON_ID,
                                sce_name + "_LCAT",
                                sce_name + "_FF1"],
                               as_index=False).agg(fun2)

    # Some cleanup
    tbl_1.rename(columns={sce_name + "_LCAT": 'LCAT'}, inplace=True)
    tbl_1.rename(columns={sce_name + "_FF1": 'CAT_FFR'}, inplace=True)

    return tbl_1


def backbone_stats_1(sce_name, stream_array:pd.DataFrame):
    """
    Calculating backbone statistics for Table 1.
    Three categories presented in paper: free-flowing, good status, impacted

    :param sce_name:
    :param stream_array:
    :return:
    """

    # Converting to panda data frame
    df = stream_array.copy()
    df = df.loc[df["INC"] == 1]

    df.loc[:, "SCE"] = sce_name

    # Calculating a field for "Length Category" and thus grouping the
    # backbone rivers into five categories:
    #
    # very short 0-10 km (not analyzed)
    # short rivers: 10 - 100 km
    # medium rivers 100 - 500 km
    # long rivers 500 - 1000 km
    # very long rivers > 1000 km

    df.loc[:, sce_name + "_LCAT"] = df[fd.BB_LEN_KM].apply(get_length_cat)

    # Function to group by continent, river, length category,
    # and free-flowing status and calculate length (km), volume (million
    # cubic meters) and connectivity to ocean.
    # see Table 1

    fun1 = {'SCE': 'first',
            fd.LENGTH_KM: np.sum,
            fd.VOLUME_TCM: np.sum,
            fd.BB_OCEAN: 'max'}

    tbl_1_temp = df.groupby([fd.CON_ID,
                             fd.BB_ID,
                             sce_name + "_LCAT",
                             sce_name + "_FF2"],
                            as_index=False).agg(fun1)

    # The result is a list of rivers by continent. We're adding a "count"
    # field (NUM) so we can now count the number of rivers
    tbl_1_temp.loc[:, "NUM"] = 1

    # Then repeat the same function as above

    fun2 = {'SCE': 'first',
            fd.LENGTH_KM: np.sum,
            fd.VOLUME_TCM: np.sum,
            fd.BB_OCEAN: np.sum,
            "NUM": np.sum}

    tbl_1 = tbl_1_temp.groupby([fd.CON_ID,
                                sce_name + "_LCAT",
                                sce_name + "_FF2"],
                               as_index=False).agg(fun2)

    # Some cleanup
    tbl_1.rename(columns={sce_name + "_LCAT": 'LCAT'}, inplace=True)
    tbl_1.rename(columns={sce_name + "_FF2": 'CAT_FFR'}, inplace=True)

    return tbl_1


def backbone_stats_2(sce_name, stream_array:pd.DataFrame):
    """
    Calculating backbone statistics for Table 1.
    Three categories presented in paper: free-flowing, good status, impacted

    :param sce_name:
    :param stream_array:
    :return:
    """

    # Converting to panda data frame
    df = stream_array.copy()
    df = df[df["INC"] == 1]

    df.loc[:, "SCE"] = sce_name

    # Select only rivers in "good" status
    df = df[df[sce_name + "_FF2"] == 2]

    # Function to group by continent, river, length category,
    # and free-flowing status and calculate length (km), volume (million
    # cubic meters) and connectivity to ocean.
    # see Table 1

    fun1 = {'SCE': 'first',
            fd.LENGTH_KM: np.sum,
            fd.VOLUME_TCM: np.sum
            }

    good_temp:pd.DataFrame = df.groupby([fd.CON_ID, fd.BB_ID], as_index=False).agg(fun1)

    if good_temp.empty:
        good_temp.rename(columns={sce_name + "_LCAT": 'LCAT'}, inplace=True)
        return good_temp

    good_temp.loc[:, sce_name + "_LCAT"] = good_temp[fd.LENGTH_KM].apply(get_length_cat)

    # The result is a list of rivers by continent. We're adding a "count"
    # field (NUM) so we can now count the number of rivers
    good_temp.loc[:, "NUM"] = 1

    # Then repeat the same function as above

    fun2 = {'SCE': 'first',
            fd.LENGTH_KM: np.sum,
            fd.VOLUME_TCM: np.sum,
            "NUM": np.sum}

    good = good_temp.groupby([fd.CON_ID, sce_name + "_LCAT"],
                             as_index=False).agg(fun2)

    # Some cleanup
    good.rename(columns={sce_name + "_LCAT": 'LCAT'}, inplace=True)

    return good


def backbone_stats_3(scenario_name, stream_array:pd.DataFrame, min_length):
    """
    Calculating backbone statistics for Excel appendix, i.e. list
    of free-flowing rivers larger than 500 km.

    :param scenario_name:
    :param stream_array:
    :param min_length: minimum threshold length to be analyzed. For global
    analysis, it is 500km. More regional analysis could have a lower
    threshold, since there might not be rivers larger than 500 km

    :return:
    """

    # Converting to panda data frame
    df = stream_array.copy()
    df = df.loc[df["INC"] == 1]

    df.loc[:, "SCE"] = scenario_name

    # Aggregating length of each bb river...
    fct = {"SCE": 'first', fd.CON_ID: 'first', fd.BAS_NAME: 'first',
           fd.LENGTH_KM: np.sum, fd.VOLUME_TCM: np.sum,
           fd.BB_NAME: 'first', fd.BB_OCEAN: 'first',
           fd.BB_LEN_KM: 'first', fd.RIV_ORD: np.min,
           scenario_name + "_FF1": 'max',
           scenario_name + "_FF2": 'max',
           scenario_name + "_FFID": 'first',
           }
    length = df.groupby([fd.BB_ID], as_index=False).agg(fct)
    length.loc[:, "NUM"] = 1

    # Result 1: Creating a list of FFR larger than X (min_length) km
    ff_river_list = length[length[fd.LENGTH_KM] > min_length]

    # Only select free-flowing rivers
    ff_river_list = ff_river_list[ff_river_list[scenario_name + "_FF1"] == 1]

    # Select a subset of fields
    ff_river_list = ff_river_list[["SCE", fd.CON_ID, fd.BAS_NAME, fd.BB_ID,
                                   fd.BB_NAME, fd.LENGTH_KM, fd.RIV_ORD,
                                   fd.BB_OCEAN]]

    return ff_river_list


def get_length_cat(x):
    r = 0

    if x >= -1:
        r = 0
    if x > 10:
        r = 10
    if x > 100:
        r = 100
    if x > 500:
        r = 500
    if x > 1000:
        r = 1000

    return r
